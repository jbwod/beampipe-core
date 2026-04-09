import logging
from datetime import UTC, datetime
from typing import Any

from ...config import settings
from ...projects import list_project_modules
from .execution_outcomes import build_workflow_execution_schedule_response
from .execution_process import process_workflow_module_for_execution_schedule

logger = logging.getLogger(__name__)


async def workflow_execution_schedule(
    db: Any,
    redis: Any,
    project_module: str | None = None,
) -> dict[str, Any]:
    scheduled_at = datetime.now(UTC).isoformat()
    if not settings.WORKFLOW_EXECUTION_AUTOMATION_ENABLED:
        logger.debug(
            "event=workflow_execution_schedule_disabled_global project_module=%s",
            project_module or "all",
        )
        return build_workflow_execution_schedule_response(
            scheduled_at=scheduled_at,
            project_module=project_module,
            created_executions=[],
            total_sources=0,
            enqueued_jobs=[],
            skipped_modules=[],
            reason_counts={"automation_disabled_global": 1},
        )

    target_modules = [project_module] if project_module else list_project_modules()
    created_executions: list[str] = []
    enqueued_jobs: list[str] = []
    skipped_modules: list[str] = []
    reason_counts: dict[str, int] = {}
    total_sources = 0

    for module_name in target_modules:
        total_sources += await process_workflow_module_for_execution_schedule(
            db,
            redis,
            module_name,
            created_executions=created_executions,
            enqueued_jobs=enqueued_jobs,
            skipped_modules=skipped_modules,
            reason_counts=reason_counts,
        )

    if settings.CASDA_USERNAME is None:
        logger.warning("event=workflow_execution_schedule_missing_casda_username")

    return build_workflow_execution_schedule_response(
        scheduled_at=scheduled_at,
        project_module=project_module,
        created_executions=created_executions,
        total_sources=total_sources,
        enqueued_jobs=enqueued_jobs,
        skipped_modules=skipped_modules,
        reason_counts=reason_counts,
    )
