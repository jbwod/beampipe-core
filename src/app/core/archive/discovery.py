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

    def _error_result(error: str) -> dict[str, Any]:
        return {
            "ok": False,
            "error": error,
            "scheduled_at": scheduled_at,
            "total_sources": 0,
            "total_jobs": 0,
            "job_ids": [],
            "enqueue_failures": 0,
            "failed_batches": [],
        }

    if not redis:
        error = "Redis queue is not available for discovery scheduling"
        logger.error(f"discover_schedule: {error}")
        return _error_result(error)

    batch_size = batch_size or settings.DISCOVERY_BATCH_SIZE
    stale_after_hours = stale_after_hours or settings.DISCOVERY_STALE_HOURS

    try:
        sources = await source_registry_service.get_sources_for_discovery(
            db=db,
            project_module=project_module,
            stale_after_hours=stale_after_hours,
        )
    except Exception as exc:
        logger.error(
            "discover_schedule: failed to fetch sources for discovery: %s",
            exc,
            exc_info=True,
        )
        return _error_result(str(exc))

    logger.info(f"discover_schedule: {len(sources)} sources marked for discovery")

    # Group sources by project_module
    grouped = defaultdict(list)
    for source in sources:
        grouped[source.project_module].append(source)

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
                f"discover_schedule: enqueue discover_batch for module={module_name} "
                f"sources={len(batch)}"
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
                        "discover_schedule: enqueue returned None for module=%s batch_size=%s attempt=%s",
                        module_name,
                        len(batch),
                        attempt + 1,
                    )
                except Exception as exc:
                    logger.error(
                        "discover_schedule: enqueue failed for module=%s batch_size=%s attempt=%s error=%s",
                        module_name,
                        len(batch),
                        attempt + 1,
                        exc,
                        exc_info=True,
                    )
                if attempt == 0:
                    await asyncio.sleep(1)

            if job:
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

    return {
        "ok": True,
        "scheduled_at": scheduled_at,
        "total_sources": total_sources,
        "total_jobs": len(job_ids),
        "job_ids": job_ids,
        "enqueue_failures": enqueue_failures,
        "failed_batches": failed_batches,
    }

# going to use this for discovery and polling tasks