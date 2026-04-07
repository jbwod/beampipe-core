from typing import Any


def build_workflow_execution_schedule_response(
    scheduled_at: str,
    project_module: str | None,
    created_executions: list[str],
    total_sources: int,
    enqueued_jobs: list[str],
    skipped_modules: list[str],
    reason_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    return {
        "ok": True,
        "scheduled_at": scheduled_at,
        "project_module": project_module or "all",
        "execution_count": len(created_executions),
        "total_sources": total_sources,
        "execution_ids": created_executions,
        "job_ids": enqueued_jobs,
        "skipped_modules": skipped_modules,
        "reason_counts": reason_counts or {},
    }
