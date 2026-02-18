"""Archive discovery service.

Handles polling and event-driven discovery of newly deposited datasets.
"""
# - Polling-based discovery for CASDA
# using the ARQ queue system in /workers and /tasks
# using the source_registry_service to get sources for discovery and module grouping
import asyncio
import logging
import re
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

from arq.connections import ArqRedis
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..registry.service import source_registry_service
from .tap_health import all_taps_reachable, get_tap_health, unreachable_taps

logger = logging.getLogger(__name__)


def extract_filename_from_url(url: str) -> Optional[str]:
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


def _chunk_list(items: list[Any], chunk_size: int) -> list[list[Any]]:
    """Split items into chunks of given size."""
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)] if chunk_size > 0 else [items]


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
    max_queue_depth = settings.DISCOVERY_MAX_QUEUE_DEPTH

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
        }

    if not redis:
        error = "Redis queue is not available for discovery scheduling"
        logger.error("event=discover_schedule_error project_module=%s error=%s", target_module, error)
        return _error_result(error)

    batch_size = batch_size or settings.DISCOVERY_BATCH_SIZE
    stale_after_hours = stale_after_hours or settings.DISCOVERY_STALE_HOURS
    queue_depth: int | None = None

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
                }
        except Exception as exc:
            logger.warning(
                "event=discover_schedule_tap_health_error project_module=%s error=%s action=continue",
                target_module,
                exc,
            )
            # On health-check failure, continue with scheduling (fail open)

    try:
        sources = await source_registry_service.get_sources_for_discovery(
            db=db,
            project_module=project_module,
            stale_after_hours=stale_after_hours,
            limit=max_sources_per_run,
        )
    except Exception as exc:
        logger.error(
            "event=discover_schedule_error project_module=%s stage=get_sources error=%s",
            target_module,
            exc,
            exc_info=True,
        )
        return _error_result(str(exc))

    if not sources:
        logger.info(
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
        }

    logger.info(
        "event=discover_schedule_sources_loaded "
        "project_module=%s discovered_sources=%s batch_size=%s "
        "stale_after_hours=%s max_sources_per_run=%s",
        target_module,
        len(sources),
        batch_size,
        stale_after_hours,
        max_sources_per_run,
    )

    # Group sources by project_module
    grouped = defaultdict(list)
    for source in sources:
        grouped[source.project_module].append(source)

    logger.debug(
        "event=discover_schedule_sources_by_module project_module=%s module_counts=%s",
        target_module,
        {k: len(v) for k, v in grouped.items()},
    )

    job_ids = []
    total_sources = 0
    enqueue_failures = 0
    failed_batches: list[dict[str, Any]] = []

    for module_name, module_sources in grouped.items():
        # Extract identifiers just once for all sources in this module
        identifiers = [source.source_identifier for source in module_sources]
        # Batch the identifiers and enqueue each batch
        for batch in _chunk_list(identifiers, batch_size):
            logger.info(
                "event=discover_schedule_enqueue_attempt project_module=%s batch_size=%s",
                module_name,
                len(batch),
            )

            job = None
            for attempt in range(2):
                try:
                    job = await redis.enqueue_job(
                        "discover_batch",
                        module_name,
                        batch,
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
                    "event=discover_schedule_enqueue_success project_module=%s batch_size=%s job_id=%s",
                    module_name,
                    len(batch),
                    job.job_id,
                )
                job_ids.append(job.job_id)
                total_sources += len(batch)
            else:
                enqueue_failures += 1
                failed_batches.append(
                    {
                        "project_module": module_name,
                        "batch_size": len(batch),
                        "source_identifiers": batch,
                    }
                )

    if queue_depth is None:
        try:
            queue_depth = await redis.zcard(settings.WORKER_QUEUE_NAME)
        except Exception:
            pass

    logger.info(
        "event=discover_schedule_completed "
        "project_module=%s scheduled_at=%s total_sources=%s total_jobs=%s "
        "enqueue_failures=%s queue_depth=%s skipped_due_to_queue_full=%s",
        target_module,
        scheduled_at,
        total_sources,
        len(job_ids),
        enqueue_failures,
        queue_depth,
        False,
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
        "skipped_due_to_queue_full": False,
    }
