import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, select

from ....models.ledger import BatchExecutionRecord, ExecutionStatus
from ...config import settings
from ...ledger.service import execution_ledger_service
from ...projects import get_workflow_execution_automation_policy
from ...registry.service import source_registry_service
from ...shaping.policy import (
    arq_queue_depth_allows_enqueue,
    can_admit_by_in_flight,
    count_execute_in_flight_runs,
    execute_admission_budget,
    shaping_enqueue_pace,
    shaping_queue_max_depth,
)

logger = logging.getLogger(__name__)


def chunked(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        size = 1
    return [items[i : i + size] for i in range(0, len(items), size)]


def workflow_execution_policy_for_module(project_module: str) -> dict[str, Any]:
    defaults = {
        "enabled": False,
        "archive_name": "casda",
        "max_sources_per_execution": 20,
        "max_sources_per_tick": 500,
        "max_executions_per_tick": 20,
        "min_sources_to_trigger": 1,
        "max_wait_minutes": 24 * 60,
        "claim_ttl_minutes": 180,
    }
    raw_policy = get_workflow_execution_automation_policy(project_module)
    if not raw_policy:
        return defaults
    policy = {
        "enabled": bool(raw_policy.get("enabled", defaults["enabled"])),
        "archive_name": str(raw_policy.get("archive_name", defaults["archive_name"])),
        "max_sources_per_execution": int(raw_policy.get("max_sources_per_execution", defaults["max_sources_per_execution"])),
        "max_sources_per_tick": int(raw_policy.get("max_sources_per_tick", defaults["max_sources_per_tick"])),
        "max_executions_per_tick": int(raw_policy.get("max_executions_per_tick", defaults["max_executions_per_tick"])),
        "min_sources_to_trigger": int(
            raw_policy.get("min_sources_to_trigger", defaults["min_sources_to_trigger"])
        ),
        "max_wait_minutes": int(raw_policy.get("max_wait_minutes", defaults["max_wait_minutes"])),
        "claim_ttl_minutes": int(raw_policy.get("claim_ttl_minutes", defaults["claim_ttl_minutes"])),
    }
    if "deployment_profile_id" in raw_policy and raw_policy["deployment_profile_id"]:
        policy["deployment_profile_id"] = str(raw_policy["deployment_profile_id"])

    def _pos_int(key: str) -> int | None:
        if key not in raw_policy:
            return None
        try:
            val = int(raw_policy[key])
        except (TypeError, ValueError):
            return None
        return val if val > 0 else None

    def _pos_float(key: str) -> float | None:
        if key not in raw_policy:
            return None
        try:
            val = float(raw_policy[key])
        except (TypeError, ValueError):
            return None
        return val if val > 0 else None

    for key in (
        "execution_max_attempts_external",
        "execution_max_duration_minutes_external",
        "execution_max_attempts_db",
        "execution_max_duration_minutes_db",
        "execution_max_polls",
        "execution_poll_max_duration_minutes",
        "discovery_max_attempts_external",
        "discovery_max_duration_minutes_external",
        "discovery_max_attempts_db",
        "discovery_max_duration_minutes_db",
    ):
        val = _pos_int(key)
        if val is not None:
            policy[key] = val

    for key in (
        "execution_initial_retry_seconds",
        "execution_max_retry_interval_seconds",
        "discovery_initial_retry_seconds",
        "discovery_max_retry_interval_seconds",
    ):
        val = _pos_float(key)
        if val is not None:
            policy[key] = val
    return policy


async def has_active_auto_execution(db: Any, project_module: str) -> bool:
    result = await db.execute(
        select(BatchExecutionRecord.uuid)
        .where(
            and_(
                BatchExecutionRecord.project_module == project_module,
                BatchExecutionRecord.scheduler_name == settings.WORKFLOW_AUTOMATION_SCHEDULER_NAME,
                BatchExecutionRecord.status.in_(
                    [ExecutionStatus.PENDING, ExecutionStatus.RUNNING, ExecutionStatus.RETRYING]
                ),
            )
        )
        .limit(1)
    )
    return result.scalar_one_or_none() is not None


async def process_workflow_module_for_execution_schedule(
    db: Any,
    redis: Any,
    module_name: str,
    *,
    created_executions: list[str],
    enqueued_jobs: list[str],
    skipped_modules: list[str],
    reason_counts: dict[str, int] | None = None,
) -> int:
    """Plan and enqueue. Returns number of sources scheduled."""

    def _bump(reason: str) -> None:
        if reason_counts is not None:
            reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1

    policy = workflow_execution_policy_for_module(module_name)
    logger.debug("event=workflow_execution_policy project_module=%s policy=%s", module_name, policy)
    if not policy["enabled"]:
        _bump("disabled")
        skipped_modules.append(module_name)
        return 0

    if await has_active_auto_execution(db=db, project_module=module_name):
        _bump("active_execution_gate")
        logger.info(
            "event=workflow_execution_schedule_skip_active_execution project_module=%s",
            module_name,
        )
        skipped_modules.append(module_name)
        return 0

    max_sources_for_module = max(1, int(policy["max_sources_per_tick"]))
    max_executions_for_module = max(1, int(policy["max_executions_per_tick"]))
    pending_stats = await source_registry_service.get_workflow_pending_stats(
        db=db,
        project_module=module_name,
    )
    pending_count = int(pending_stats.get("count") or 0)
    if pending_count <= 0:
        return 0

    oldest_pending_at = pending_stats.get("oldest_pending_at")
    max_wait_minutes = max(1, int(policy["max_wait_minutes"]))
    max_wait_triggered = bool(
        oldest_pending_at
        and oldest_pending_at <= datetime.now(UTC) - timedelta(minutes=max_wait_minutes)
    )
    min_sources_to_trigger = max(1, int(policy["min_sources_to_trigger"]))
    if not max_wait_triggered and pending_count < min_sources_to_trigger:
        _bump("threshold_not_met")
        logger.info(
            "event=workflow_execution_batch_skip_threshold project_module=%s pending_count=%s min_sources=%s",
            module_name,
            pending_count,
            min_sources_to_trigger,
        )
        return 0

    in_flight_cap = settings.SHAPING_EXECUTE_MAX_IN_FLIGHT_RUNS
    if in_flight_cap is not None:
        in_flight_runs = await count_execute_in_flight_runs(db=db)
        if not can_admit_by_in_flight(current=in_flight_runs, max_in_flight=in_flight_cap):
            _bump("in_flight_cap")
            logger.warning(
                "event=workflow_execution_schedule_in_flight_cap project_module=%s in_flight_executions=%s max_in_flight_executions=%s",
                module_name,
                in_flight_runs,
                in_flight_cap,
            )
            return 0
        remaining_slots = max(0, int(in_flight_cap) - int(in_flight_runs))
        if remaining_slots <= 0:
            _bump("in_flight_cap")
            return 0
        max_executions_for_module = min(max_executions_for_module, remaining_slots)

    admitted_executions = await execute_admission_budget(
        redis, desired_runs=max_executions_for_module
    )
    if admitted_executions <= 0:
        _bump("rate_limited")
        logger.info(
            "event=workflow_execution_schedule_rate_limited project_module=%s requested_executions=%s admitted_executions=%s",
            module_name,
            int(policy["max_executions_per_tick"]),
            admitted_executions,
        )
        return 0
    max_executions_for_module = min(max_executions_for_module, admitted_executions)

    max_sources_for_module = min(
        max_sources_for_module,
        max_executions_for_module * max(1, int(policy["max_sources_per_execution"])),
    )

    claim_token, pending_sources = await source_registry_service.claim_pending_sources_for_workflow_run(
        db=db,
        project_module=module_name,
        limit=max_sources_for_module,
        lease_ttl_minutes=max(1, int(policy["claim_ttl_minutes"])),
        commit=False,
    )
    if not claim_token or not pending_sources:
        _bump("no_claimable_sources")
        await db.commit()
        return 0

    chunk_size = max(1, int(policy["max_sources_per_execution"]))
    created_for_module = 0
    sources_scheduled = 0
    try:
        for chunk in chunked(pending_sources, chunk_size):
            if created_for_module >= max_executions_for_module:
                break
            allowed, qdepth = await arq_queue_depth_allows_enqueue(
                redis,
                queue_name=settings.WORKER_QUEUE_NAME,
                max_depth=shaping_queue_max_depth(settings),
            )
            if not allowed:
                _bump("queue_full")
                logger.warning(
                    "event=workflow_execution_schedule_queue_full project_module=%s queue=%s queue_depth=%s max_queue_depth=%s action=stop_enqueue",
                    module_name,
                    settings.WORKER_QUEUE_NAME,
                    qdepth,
                    shaping_queue_max_depth(settings),
                )
                break
            execution = await execution_ledger_service.create_execution(
                db=db,
                project_module=module_name,
                sources=[{"source_identifier": src} for src in chunk],
                archive_name=policy["archive_name"],
                deployment_profile_id=(
                    UUID(policy["deployment_profile_id"])
                    if policy.get("deployment_profile_id")
                    else None
                ),
                created_by_id=None,
            )
            execution_uuid = str(execution["uuid"])
            await execution_ledger_service.update_execution_status(
                db=db,
                execution_id=execution["uuid"],
                scheduler_name=settings.WORKFLOW_AUTOMATION_SCHEDULER_NAME,
            )
            job = await redis.enqueue_job(
                "execute_execution_job",
                execution_uuid,
                _queue_name=settings.WORKER_QUEUE_NAME,
            )
            job_id = job.job_id if job else None
            logger.info(
                "event=workflow_execution_batch project_module=%s source_count=%s execution_uuid=%s job_id=%s",
                module_name,
                len(chunk),
                execution_uuid,
                job_id,
            )
            created_executions.append(execution_uuid)
            if job_id:
                enqueued_jobs.append(job_id)
            sources_scheduled += len(chunk)
            created_for_module += 1
            await shaping_enqueue_pace()
    finally:
        await source_registry_service.release_workflow_claim(
            db=db,
            project_module=module_name,
            source_identifiers=pending_sources,
            claim_token=claim_token,
            commit=False,
        )
        await db.commit()

    return sources_scheduled
