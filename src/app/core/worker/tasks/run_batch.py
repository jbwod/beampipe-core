import logging
from datetime import UTC, datetime
from typing import Any

from ...config import settings
from ...projects import list_project_modules
from .run_outcomes import build_workflow_schedule_response
from .run_process import process_workflow_module_for_schedule

logger = logging.getLogger(__name__)


async def workflow_run_schedule(
    db: Any,
    redis: Any,
    project_module: str | None = None,
) -> dict[str, Any]:
    scheduled_at = datetime.now(UTC).isoformat()
    target_modules = [project_module] if project_module else list_project_modules()
    created_runs: list[str] = []
    enqueued_jobs: list[str] = []
    skipped_modules: list[str] = []
    reason_counts: dict[str, int] = {}
    total_sources = 0

    for module_name in target_modules:
        total_sources += await process_workflow_module_for_schedule(
            db,
            redis,
            module_name,
            created_runs=created_runs,
            enqueued_jobs=enqueued_jobs,
            skipped_modules=skipped_modules,
            reason_counts=reason_counts,
        )

    if settings.CASDA_USERNAME is None:
        logger.warning("event=workflow_run_schedule_missing_casda_username")

    return build_workflow_schedule_response(
        scheduled_at=scheduled_at,
        project_module=project_module,
        created_runs=created_runs,
        total_sources=total_sources,
        enqueued_jobs=enqueued_jobs,
        skipped_modules=skipped_modules,
        reason_counts=reason_counts,
    )
