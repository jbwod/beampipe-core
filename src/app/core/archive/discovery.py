"""Archive discovery service.

Handles polling and event-driven discovery of newly deposited datasets.
"""
# - Polling-based discovery for CASDA
# using the ARQ queue system in /workers and /tasks
# using the source_registry_service to get sources for discovery and module grouping
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from arq.connections import ArqRedis
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..registry.service import source_registry_service


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
    if not redis:
        raise RuntimeError("Redis queue is not available for discovery scheduling")

    batch_size = batch_size or settings.DISCOVERY_BATCH_SIZE
    stale_after_hours = stale_after_hours or settings.DISCOVERY_STALE_HOURS

    sources = await source_registry_service.get_sources_for_discovery(
        db=db,
        project_module=project_module,
        stale_after_hours=stale_after_hours,
    )

    print(f"discover_schedule: {len(sources)} sources marked for discovery")

    # Group sources by project_module
    grouped = defaultdict(list)
    for source in sources:
        grouped[source.project_module].append(source)

    job_ids = []
    total_sources = 0

    for module_name, module_sources in grouped.items():
        # Extract identifiers just once for all sources in this module
        identifiers = [source.source_identifier for source in module_sources]
        # Batch the identifiers and enqueue each batch
        for batch in _chunk_list(identifiers, batch_size):
            print(
                f"discover_schedule: enqueue discover_batch for module={module_name} "
                f"sources={len(batch)}"
            )
            job = await redis.enqueue_job(
                "discover_batch",
                module_name,
                batch,
                _queue_name=settings.WORKER_QUEUE_NAME,
            )
            if job:
                job_ids.append(job.job_id)
            total_sources += len(batch)

    return {
        "scheduled_at": datetime.now(UTC).isoformat(),
        "total_sources": total_sources,
        "total_jobs": len(job_ids),
        "job_ids": job_ids,
    }

# going to use this for discovery and polling tasks