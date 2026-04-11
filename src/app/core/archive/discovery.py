"""Archive discovery service.

Handles polling and event-driven discovery of newly deposited datasets.
"""
# - Polling-based discovery for CASDA
# using the ARQ queue system in /workers and /tasks
# using the source_registry_service to get sources for discovery and module grouping
import asyncio
import logging
import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from arq.connections import ArqRedis
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..positive_policy import positive_int_optional
from ..projects import get_workflow_discovery_automation_policy
from ..registry.service import source_registry_service
from ..shaping.policy import (
    can_admit_by_in_flight,
    discovery_admission_budget,
    estimate_discovery_in_flight_batches,
    shaping_queue_max_depth,
    shaping_enqueue_pace,
)
from .tap_health import all_taps_reachable, get_tap_health, unreachable_taps

logger = logging.getLogger(__name__)


def discovery_scheduler_policy_for_module(
    project_module: str,
) -> dict[str, Any]:
    defaults = {
        "enabled": True,
        "tick_discovery_source_limit": settings.DISCOVERY_MAX_SOURCES_PER_RUN,
        "batch_size": settings.DISCOVERY_BATCH_SIZE,
        "tick_discovery_batch_limit": settings.SHAPING_DISCOVERY_MAX_BATCHES_PER_TICK,
        "stale_after_hours": settings.DISCOVERY_STALE_HOURS,
        "claim_ttl_minutes": settings.DISCOVERY_CLAIM_TTL_MINUTES,
    }
    raw = get_workflow_discovery_automation_policy(project_module)
    if not raw:
        return defaults
    policy = dict(defaults)
    policy["enabled"] = bool(raw.get("enabled", defaults["enabled"]))

    sources_cap = positive_int_optional(raw, "tick_discovery_source_limit")
    if sources_cap is not None:
        policy["tick_discovery_source_limit"] = sources_cap

    for key in ("batch_size", "tick_discovery_batch_limit", "stale_after_hours", "claim_ttl_minutes"):
        val = positive_int_optional(raw, key)
        if val is not None:
            policy[key] = val
    max_in_flight = positive_int_optional(raw, "concurrent_discovery_batch_limit")
    if max_in_flight is not None:
        policy["concurrent_discovery_batch_limit"] = max_in_flight
    return policy


def extract_filename_from_url(url: str) -> str | None:
    """Extract filename from a URL (handles query parameters)."""
    decoded_url = unquote(url)
    parsed = urlparse(decoded_url)
    query_params = parse_qs(parsed.query)
    response_disposition = query_params.get("response-content-disposition", [])
    for value in response_disposition:
        match = re.search(r'filename="?([^";]+)"?', value)
        if match:
            return match.group(1)
    filename = parsed.path.split("/")[-1]
    return filename or None


def _group_claimed_rows_by_module(
    claimed_rows: list[dict[str, str]],
) -> list[tuple[str, list[str]]]:
    batch_by_module: dict[str, list[str]] = {}
    for row in claimed_rows:
        batch_by_module.setdefault(row["project_module"], []).append(row["source_identifier"])
    return list(batch_by_module.items())

async def discover_schedule(
    db: AsyncSession,
    redis: ArqRedis,
    project_module: str | None = None,
    batch_size: int | None = None,
    stale_after_hours: int | None = None,
) -> dict[str, Any]:
    """Enqueue discovery batch jobs for sources needing discovery."""
    scheduled_at = datetime.now(UTC).isoformat()
    target_module = project_module or "all"
    max_sources_per_run = settings.DISCOVERY_MAX_SOURCES_PER_RUN
    max_queue_depth = shaping_queue_max_depth(settings)
    max_discovery_jobs_per_tick = settings.SHAPING_DISCOVERY_MAX_BATCHES_PER_TICK
    max_in_flight_batches = settings.SHAPING_DISCOVERY_MAX_IN_FLIGHT_BATCHES
    policy: dict[str, Any] | None = None
    project_in_flight_cap: int | None = None

    if project_module:
        policy = discovery_scheduler_policy_for_module(project_module)
        if not bool(policy.get("enabled", True)):
            return {
                "ok": True,
                "scheduled_at": scheduled_at,
                "project_module": target_module,
                "total_sources": 0,
                "total_jobs": 0,
                "job_ids": [],
                "enqueue_failures": 0,
                "failed_batches": [],
                "max_sources_per_run": 0,
                "queue_depth": None,
                "skipped_due_to_queue_full": False,
                "skipped_due_to_tick_discovery_batch_limit": False,
                "admitted_by_rate": 0,
                "blocked_by_rate": False,
                "blocked_by_in_flight": False,
                "reason": "discovery_automation_disabled",
            }
        project_cap = max(1, int(policy["tick_discovery_source_limit"]))
        max_sources_per_run = min(max_sources_per_run, project_cap)
        if policy.get("tick_discovery_batch_limit") is not None:
            project_jobs_cap = int(policy["tick_discovery_batch_limit"])
            if max_discovery_jobs_per_tick is None:
                max_discovery_jobs_per_tick = project_jobs_cap
            else:
                max_discovery_jobs_per_tick = min(max_discovery_jobs_per_tick, project_jobs_cap)
        if policy.get("concurrent_discovery_batch_limit") is not None:
            project_in_flight_cap = int(policy["concurrent_discovery_batch_limit"])
            project_in_flight_batches = await estimate_discovery_in_flight_batches(
                db,
                redis,
                queue_name=settings.WORKER_QUEUE_NAME,
                project_module=project_module,
            )
            if not can_admit_by_in_flight(
                current=project_in_flight_batches,
                max_in_flight=project_in_flight_cap,
            ):
                return {
                    "ok": True,
                    "scheduled_at": scheduled_at,
                    "project_module": target_module,
                    "total_sources": 0,
                    "total_jobs": 0,
                    "job_ids": [],
                    "enqueue_failures": 0,
                    "failed_batches": [],
                    "max_sources_per_run": max_sources_per_run,
                    "queue_depth": None,
                    "skipped_due_to_queue_full": False,
                    "skipped_due_to_tick_discovery_batch_limit": False,
                    "admitted_by_rate": 0,
                    "blocked_by_rate": False,
                    "blocked_by_in_flight": True,
                }
            project_remaining_slots = max(0, project_in_flight_cap - int(project_in_flight_batches))
            if project_remaining_slots <= 0:
                return {
                    "ok": True,
                    "scheduled_at": scheduled_at,
                    "project_module": target_module,
                    "total_sources": 0,
                    "total_jobs": 0,
                    "job_ids": [],
                    "enqueue_failures": 0,
                    "failed_batches": [],
                    "max_sources_per_run": max_sources_per_run,
                    "queue_depth": None,
                    "skipped_due_to_queue_full": False,
                    "skipped_due_to_tick_discovery_batch_limit": False,
                    "admitted_by_rate": 0,
                    "blocked_by_rate": False,
                    "blocked_by_in_flight": True,
                }
            if max_discovery_jobs_per_tick is None:
                max_discovery_jobs_per_tick = project_remaining_slots
            else:
                max_discovery_jobs_per_tick = min(max_discovery_jobs_per_tick, project_remaining_slots)
    requested_sources_tick = max(0, int(max_sources_per_run))
    requested_batches_tick = (
        int(max_discovery_jobs_per_tick) if max_discovery_jobs_per_tick is not None else None
    )

    def _error_result(error: str) -> dict[str, Any]:
        return {
            "ok": False,
            "error": error,
            "scheduled_at": scheduled_at,
            "project_module": target_module,
            "total_sources": 0,
            "total_jobs": 0,
            "job_ids": [],
            "enqueue_failures": 0,
            "failed_batches": [],
            "max_sources_per_run": max_sources_per_run,
            "queue_depth": None,
            "skipped_due_to_queue_full": False,
            "skipped_due_to_tick_discovery_batch_limit": False,
            "admitted_by_rate": 0,
            "blocked_by_rate": False,
            "blocked_by_in_flight": False,
        }

    if not redis:
        error = "Redis queue is not available for discovery scheduling"
        logger.error("event=discover_schedule_error project_module=%s error=%s", target_module, error)
        return _error_result(error)

    if batch_size is None:
        if policy and policy.get("batch_size") is not None:
            batch_size = int(policy["batch_size"])
        else:
            batch_size = settings.DISCOVERY_BATCH_SIZE
    if stale_after_hours is None:
        if policy and policy.get("stale_after_hours") is not None:
            stale_after_hours = int(policy["stale_after_hours"])
        else:
            stale_after_hours = settings.DISCOVERY_STALE_HOURS
    queue_depth: int | None = None
    blocked_by_rate = False
    blocked_by_in_flight = False
    admitted_by_rate = 0

    if max_queue_depth is not None:
        try:
            queue_depth = await redis.zcard(settings.WORKER_QUEUE_NAME)
        except Exception as exc:
            logger.warning(
                "event=discover_schedule_queue_depth_unavailable project_module=%s queue=%s error=%s",
                target_module,
                settings.WORKER_QUEUE_NAME,
                exc,
                exc_info=True,
            )
        else:
            logger.debug(
                "event=discover_schedule_queue_depth project_module=%s queue=%s queue_depth=%s max_queue_depth=%s",
                target_module,
                settings.WORKER_QUEUE_NAME,
                queue_depth,
                max_queue_depth,
            )
            if queue_depth >= max_queue_depth:
                logger.warning(
                    "event=discover_schedule_queue_full "
                    "project_module=%s queue=%s queue_depth=%s "
                    "max_queue_depth=%s action=skip",
                    target_module,
                    settings.WORKER_QUEUE_NAME,
                    queue_depth,
                    max_queue_depth,
                )
                return {
                    "ok": True,
                    "scheduled_at": scheduled_at,
                    "project_module": target_module,
                    "total_sources": 0,
                    "total_jobs": 0,
                    "job_ids": [],
                    "enqueue_failures": 0,
                    "failed_batches": [],
                    "max_sources_per_run": max_sources_per_run,
                    "queue_depth": queue_depth,
                    "skipped_due_to_queue_full": True,
                    "skipped_due_to_tick_discovery_batch_limit": False,
                    "admitted_by_rate": 0,
                    "blocked_by_rate": False,
                    "blocked_by_in_flight": False,
                }

    requested_sources = requested_sources_tick
    admitted_by_rate = await discovery_admission_budget(
        redis,
        desired_sources=requested_sources,
    )
    if admitted_by_rate <= 0:
        blocked_by_rate = True
        logger.debug(
            "event=discover_schedule_rate_limited project_module=%s requested_sources=%s admitted_sources=%s",
            target_module,
            requested_sources,
            admitted_by_rate,
        )
        return {
            "ok": True,
            "scheduled_at": scheduled_at,
            "project_module": target_module,
            "total_sources": 0,
            "total_jobs": 0,
            "job_ids": [],
            "enqueue_failures": 0,
            "failed_batches": [],
            "max_sources_per_run": max_sources_per_run,
            "queue_depth": queue_depth,
            "skipped_due_to_queue_full": False,
            "skipped_due_to_tick_discovery_batch_limit": False,
            "admitted_by_rate": 0,
            "blocked_by_rate": True,
            "blocked_by_in_flight": False,
        }

    # skip scheduling when a TAP endpoint is unreachable to avoid endless retries
    if settings.DISCOVERY_TAP_HEALTH_CHECK_ENABLED:
        try:
            tap_health = await get_tap_health(
                timeout_seconds=settings.DISCOVERY_TAP_HEALTH_TIMEOUT_SECONDS
            )
            logger.debug(
                "event=discover_schedule_tap_health project_module=%s tap_health=%s",
                target_module,
                tap_health,
            )
            if not all_taps_reachable(tap_health):
                unreachable = unreachable_taps(tap_health)
                logger.warning(
                    "event=discover_schedule_tap_unreachable "
                    "project_module=%s tap_unreachable=%s action=skip",
                    target_module,
                    unreachable,
                )
                return {
                    "ok": True,
                    "scheduled_at": scheduled_at,
                    "project_module": target_module,
                    "total_sources": 0,
                    "total_jobs": 0,
                    "job_ids": [],
                    "enqueue_failures": 0,
                    "failed_batches": [],
                    "max_sources_per_run": max_sources_per_run,
                    "queue_depth": queue_depth,
                    "skipped_due_to_queue_full": False,
                    "skipped_due_to_tap_unreachable": True,
                    "tap_unreachable": unreachable,
                    "skipped_due_to_tick_discovery_batch_limit": False,
                    "admitted_by_rate": admitted_by_rate,
                    "blocked_by_rate": blocked_by_rate,
                    "blocked_by_in_flight": blocked_by_in_flight,
                }
        except Exception as exc:
            logger.warning(
                "event=discover_schedule_tap_health_error project_module=%s error=%s action=continue",
                target_module,
                exc,
            )
            # On health-check failure, continue with scheduling (fail open)

    logger.debug(
        "event=discover_schedule_claim_strategy project_module=%s strategy=global_oldest_first batch_size=%s "
        "stale_after_hours=%s max_sources_per_run=%s",
        target_module,
        batch_size,
        stale_after_hours,
        max_sources_per_run,
    )

    job_ids = []
    total_sources = 0
    enqueue_failures = 0
    failed_batches: list[dict[str, Any]] = []
    skipped_due_to_queue_full = False
    remaining_sources = admitted_by_rate
    discovery_jobs_this_tick = 0

    while remaining_sources > 0:
        if max_queue_depth is not None:
            try:
                queue_depth = await redis.zcard(settings.WORKER_QUEUE_NAME)
            except Exception as exc:
                logger.warning(
                    "event=discover_schedule_queue_depth_unavailable project_module=%s queue=%s error=%s",
                    target_module,
                    settings.WORKER_QUEUE_NAME,
                    exc,
                    exc_info=True,
                )
            else:
                logger.debug(
                    "event=discover_schedule_queue_depth project_module=%s queue=%s queue_depth=%s max_queue_depth=%s",
                    target_module,
                    settings.WORKER_QUEUE_NAME,
                    queue_depth,
                    max_queue_depth,
                )
                if queue_depth >= max_queue_depth:
                    logger.warning(
                        "event=discover_schedule_queue_full "
                        "project_module=%s queue=%s queue_depth=%s "
                        "max_queue_depth=%s action=stop",
                        target_module,
                        settings.WORKER_QUEUE_NAME,
                        queue_depth,
                        max_queue_depth,
                    )
                    skipped_due_to_queue_full = True
                    break

        if max_in_flight_batches is not None:
            in_flight_batches = await estimate_discovery_in_flight_batches(
                db,
                redis,
                queue_name=settings.WORKER_QUEUE_NAME,
            )
            if not can_admit_by_in_flight(
                current=in_flight_batches,
                max_in_flight=max_in_flight_batches,
            ):
                blocked_by_in_flight = True
                logger.warning(
                    "event=discover_schedule_in_flight_cap "
                    "project_module=%s in_flight_batches=%s max_in_flight_batches=%s action=stop",
                    target_module,
                    in_flight_batches,
                    max_in_flight_batches,
                )
                break
        if project_in_flight_cap is not None and project_module is not None:
            project_in_flight_batches = await estimate_discovery_in_flight_batches(
                db,
                redis,
                queue_name=settings.WORKER_QUEUE_NAME,
                project_module=project_module,
            )
            if not can_admit_by_in_flight(
                current=project_in_flight_batches,
                max_in_flight=project_in_flight_cap,
            ):
                blocked_by_in_flight = True
                logger.warning(
                    "event=discover_schedule_project_in_flight_cap "
                    "project_module=%s in_flight_batches=%s max_in_flight_batches=%s action=stop",
                    target_module,
                    project_in_flight_batches,
                    project_in_flight_cap,
                )
                break

        claim_token = None
        batch_limit = min(batch_size, remaining_sources)
        try:
            claim_token, claimed_rows = await source_registry_service.claim_source_rows_for_discovery(
                db=db,
                project_module=project_module,
                stale_after_hours=stale_after_hours,
                limit=batch_limit,
                lease_ttl_minutes=(
                    int(policy["claim_ttl_minutes"])
                    if policy and policy.get("claim_ttl_minutes") is not None
                    else settings.DISCOVERY_CLAIM_TTL_MINUTES
                ),
                commit=False,
            )
            if claimed_rows:
                await db.commit()
        except Exception as exc:
            await db.rollback()
            logger.error(
                "event=discover_schedule_claim_error project_module=%s batch_limit=%s error=%s",
                target_module,
                batch_limit,
                exc,
                exc_info=True,
            )
            enqueue_failures += 1
            failed_batches.append(
                {
                    "project_module": target_module,
                    "batch_size": batch_limit,
                    "source_identifiers": [],
                }
            )
            break

        if not claimed_rows:
            break

        pending_batches = _group_claimed_rows_by_module(claimed_rows)
        for index, (module_name, batch) in enumerate(pending_batches):
            logger.debug(
                "event=discover_schedule_enqueue_attempt project_module=%s batch_size=%s claim_token=%s",
                module_name,
                len(batch),
                claim_token,
            )

            job = None
            for attempt in range(2):
                try:
                    job = await redis.enqueue_job(
                        "discover_batch",
                        module_name,
                        batch,
                        claim_token,
                        _queue_name=settings.WORKER_QUEUE_NAME,
                    )
                    if job is not None:
                        break

                    logger.error(
                        "event=discover_schedule_enqueue_none project_module=%s batch_size=%s attempt=%s",
                        module_name,
                        len(batch),
                        attempt + 1,
                    )
                except Exception as exc:
                    logger.error(
                        "event=discover_schedule_enqueue_error project_module=%s batch_size=%s attempt=%s error=%s",
                        module_name,
                        len(batch),
                        attempt + 1,
                        exc,
                        exc_info=True,
                    )
                if attempt == 0:
                    await asyncio.sleep(1)

            if job:
                logger.debug(
                    "event=discover_schedule_enqueue_success project_module=%s batch_size=%s job_id=%s claim_token=%s",
                    module_name,
                    len(batch),
                    job.job_id,
                    claim_token,
                )
                job_ids.append(job.job_id)
                total_sources += len(batch)
                remaining_sources -= len(batch)
                discovery_jobs_this_tick += 1
                await shaping_enqueue_pace()
                if (
                    max_discovery_jobs_per_tick is not None
                    and discovery_jobs_this_tick >= max_discovery_jobs_per_tick
                ):
                    logger.warning(
                        "event=discover_schedule_max_batches_per_tick "
                        "project_module=%s jobs_this_tick=%s max=%s action=stop",
                        target_module,
                        discovery_jobs_this_tick,
                        max_discovery_jobs_per_tick,
                    )
                    for ridx in range(index + 1, len(pending_batches)):
                        release_module, release_batch = pending_batches[ridx]
                        try:
                            await source_registry_service.release_discovery_claim(
                                db=db,
                                project_module=release_module,
                                source_identifiers=release_batch,
                                claim_token=claim_token,
                                commit=False,
                            )
                            await db.commit()
                        except Exception as exc:
                            await db.rollback()
                            logger.warning(
                                "event=discover_schedule_release_claim_error "
                                "project_module=%s batch_size=%s claim_token=%s error=%s",
                                release_module,
                                len(release_batch),
                                claim_token,
                                exc,
                                exc_info=True,
                            )
                    if queue_depth is None:
                        try:
                            queue_depth = await redis.zcard(settings.WORKER_QUEUE_NAME)
                        except Exception:
                            pass
                    return {
                        "ok": True,
                        "scheduled_at": scheduled_at,
                        "project_module": target_module,
                        "total_sources": total_sources,
                        "total_jobs": len(job_ids),
                        "job_ids": job_ids,
                        "enqueue_failures": enqueue_failures,
                        "failed_batches": failed_batches,
                        "max_sources_per_run": max_sources_per_run,
                        "queue_depth": queue_depth,
                        "skipped_due_to_queue_full": skipped_due_to_queue_full,
                        "skipped_due_to_tick_discovery_batch_limit": True,
                        "admitted_by_rate": admitted_by_rate,
                        "blocked_by_rate": blocked_by_rate,
                        "blocked_by_in_flight": blocked_by_in_flight,
                    }
                continue

            enqueue_failures += 1
            failed_batches.append(
                {
                    "project_module": module_name,
                    "batch_size": len(batch),
                    "source_identifiers": batch,
                }
            )
            batches_to_release = pending_batches[index:]
            for release_module, release_batch in batches_to_release:
                try:
                    await source_registry_service.release_discovery_claim(
                        db=db,
                        project_module=release_module,
                        source_identifiers=release_batch,
                        claim_token=claim_token,
                        commit=False,
                    )
                    await db.commit()
                except Exception as exc:
                    await db.rollback()
                    logger.warning(
                        "event=discover_schedule_release_claim_error "
                        "project_module=%s batch_size=%s claim_token=%s error=%s",
                        release_module,
                        len(release_batch),
                        claim_token,
                        exc,
                        exc_info=True,
                    )
            return {
                "ok": True,
                "scheduled_at": scheduled_at,
                "project_module": target_module,
                "total_sources": total_sources,
                "total_jobs": len(job_ids),
                "job_ids": job_ids,
                "enqueue_failures": enqueue_failures,
                "failed_batches": failed_batches,
                "max_sources_per_run": max_sources_per_run,
                "queue_depth": queue_depth,
                "skipped_due_to_queue_full": skipped_due_to_queue_full,
                "skipped_due_to_tick_discovery_batch_limit": False,
                "admitted_by_rate": admitted_by_rate,
                "blocked_by_rate": blocked_by_rate,
                "blocked_by_in_flight": blocked_by_in_flight,
            }

    if queue_depth is None:
        try:
            queue_depth = await redis.zcard(settings.WORKER_QUEUE_NAME)
        except Exception:
            pass

    if (
        total_sources == 0
        and enqueue_failures == 0
        and not skipped_due_to_queue_full
        and not blocked_by_rate
        and not blocked_by_in_flight
    ):
        logger.debug(
            "event=discover_schedule_skipped project_module=%s reason=no_stale_sources",
            target_module,
        )
        return {
            "ok": True,
            "scheduled_at": scheduled_at,
            "project_module": target_module,
            "total_sources": 0,
            "total_jobs": 0,
            "job_ids": [],
            "enqueue_failures": 0,
            "failed_batches": [],
            "max_sources_per_run": max_sources_per_run,
            "queue_depth": queue_depth,
            "skipped_due_to_queue_full": False,
            "skipped_due_to_tick_discovery_batch_limit": False,
            "admitted_by_rate": admitted_by_rate,
            "blocked_by_rate": blocked_by_rate,
            "blocked_by_in_flight": blocked_by_in_flight,
        }

    logger.info(
        "event=discover_schedule_completed "
        "project_module=%s scheduled_at=%s total_sources=%s total_jobs=%s "
        "enqueue_failures=%s queue_depth=%s skipped_due_to_queue_full=%s "
        "requested_sources_tick=%s requested_batches_tick=%s admitted_by_rate=%s "
        "blocked_by_rate=%s blocked_by_in_flight=%s",
        target_module,
        scheduled_at,
        total_sources,
        len(job_ids),
        enqueue_failures,
        queue_depth,
        skipped_due_to_queue_full,
        requested_sources_tick,
        requested_batches_tick,
        admitted_by_rate,
        blocked_by_rate,
        blocked_by_in_flight,
    )

    return {
        "ok": True,
        "scheduled_at": scheduled_at,
        "project_module": target_module,
        "total_sources": total_sources,
        "total_jobs": len(job_ids),
        "job_ids": job_ids,
        "enqueue_failures": enqueue_failures,
        "failed_batches": failed_batches,
        "max_sources_per_run": max_sources_per_run,
        "queue_depth": queue_depth,
        "skipped_due_to_queue_full": skipped_due_to_queue_full,
        "skipped_due_to_tick_discovery_batch_limit": False,
        "admitted_by_rate": admitted_by_rate,
        "blocked_by_rate": blocked_by_rate,
        "blocked_by_in_flight": blocked_by_in_flight,
    }
