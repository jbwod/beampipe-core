import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, select

from ....models.ledger import BatchRunRecord, RunStatus
from ...config import settings
from ...ledger.service import run_ledger_service
from ...projects import load_project_module
from ...registry.service import source_registry_service

logger = logging.getLogger(__name__)


def chunked(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        size = 1
    return [items[i : i + size] for i in range(0, len(items), size)]


def workflow_run_policy_for_module(project_module: str) -> dict[str, Any]:
    defaults = {
        "enabled": False,
        "archive_name": "casda",
        "max_sources_per_run": 25,
        "max_sources_per_tick": 500,
        "max_runs_per_tick": 20,
        "min_sources_to_trigger": 1,
        "max_wait_minutes": 24 * 60,
        "claim_ttl_minutes": 180,
    }
    module = load_project_module(project_module)
    raw_policy = getattr(module, "WORKFLOW_RUN_AUTOMATION", None)
    if not isinstance(raw_policy, dict):
        return defaults
    policy = {
        "enabled": bool(raw_policy.get("enabled", defaults["enabled"])),
        "archive_name": str(raw_policy.get("archive_name", defaults["archive_name"])),
        "max_sources_per_run": int(raw_policy.get("max_sources_per_run", defaults["max_sources_per_run"])),
        "max_sources_per_tick": int(raw_policy.get("max_sources_per_tick", defaults["max_sources_per_tick"])),
        "max_runs_per_tick": int(raw_policy.get("max_runs_per_tick", defaults["max_runs_per_tick"])),
        "min_sources_to_trigger": int(
            raw_policy.get("min_sources_to_trigger", defaults["min_sources_to_trigger"])
        ),
        "max_wait_minutes": int(raw_policy.get("max_wait_minutes", defaults["max_wait_minutes"])),
        "claim_ttl_minutes": int(raw_policy.get("claim_ttl_minutes", defaults["claim_ttl_minutes"])),
    }
    if "execution_profile_id" in raw_policy and raw_policy["execution_profile_id"]:
        policy["execution_profile_id"] = str(raw_policy["execution_profile_id"])
    return policy

# Move to Service
async def has_active_auto_run(db: Any, project_module: str) -> bool:
    result = await db.execute(
        select(BatchRunRecord.uuid)
        .where(
            and_(
                BatchRunRecord.project_module == project_module,
                BatchRunRecord.scheduler_name == settings.WORKFLOW_AUTOMATION_SCHEDULER_NAME,
                BatchRunRecord.status.in_([RunStatus.PENDING, RunStatus.RUNNING, RunStatus.RETRYING]),
            )
        )
        .limit(1)
    )
    return result.scalar_one_or_none() is not None


async def process_workflow_module_for_schedule(
    db: Any,
    redis: Any,
    module_name: str,
    *,
    created_runs: list[str],
    enqueued_jobs: list[str],
    skipped_modules: list[str],
) -> int:
    """Plan and enqueue Returns number of sources scheduled."""
    policy = workflow_run_policy_for_module(module_name)
    logger.debug("event=workflow_run_policy project_module=%s policy=%s", module_name, policy)
    if not policy["enabled"]:
        skipped_modules.append(module_name)
        return 0

    if await has_active_auto_run(db=db, project_module=module_name):
        logger.info(
            "event=workflow_run_schedule_skip_active_run project_module=%s",
            module_name,
        )
        skipped_modules.append(module_name)
        return 0

    max_sources_for_module = max(1, int(policy["max_sources_per_tick"]))
    max_runs_for_module = max(1, int(policy["max_runs_per_tick"]))
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
        logger.info(
            "event=workflow_run_batch_skip_threshold project_module=%s pending_count=%s min_sources=%s",
            module_name,
            pending_count,
            min_sources_to_trigger,
        )
        return 0

    # claiming 
    claim_token, pending_sources = await source_registry_service.claim_pending_sources_for_workflow_run(
        db=db,
        project_module=module_name,
        limit=max_sources_for_module,
        lease_ttl_minutes=max(1, int(policy["claim_ttl_minutes"])),
        commit=False,
    )
    if not claim_token or not pending_sources:
        await db.commit()
        return 0

    chunk_size = max(1, int(policy["max_sources_per_run"]))
    created_for_module = 0
    sources_scheduled = 0
    try:
        for chunk in chunked(pending_sources, chunk_size):
            if created_for_module >= max_runs_for_module:
                break
            run = await run_ledger_service.create_run(
                db=db,
                project_module=module_name,
                sources=[{"source_identifier": src} for src in chunk],
                archive_name=policy["archive_name"],
                execution_profile_id=(
                    UUID(policy["execution_profile_id"])
                    if policy.get("execution_profile_id")
                    else None
                ),
                created_by_id=None,
            )
            run_uuid = str(run["uuid"])
            await run_ledger_service.update_run_status(
                db=db,
                run_id=run["uuid"],
                scheduler_name=settings.WORKFLOW_AUTOMATION_SCHEDULER_NAME,
            )
            job = await redis.enqueue_job(
                "execute_run_job",
                run_uuid,
                _queue_name=settings.WORKER_QUEUE_NAME,
            )
            job_id = job.job_id if job else None
            logger.info(
                "event=workflow_run_batch project_module=%s source_count=%s run_uuid=%s job_id=%s",
                module_name,
                len(chunk),
                run_uuid,
                job_id,
            )
            created_runs.append(run_uuid)
            if job_id:
                enqueued_jobs.append(job_id)
            sources_scheduled += len(chunk)
            created_for_module += 1
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
