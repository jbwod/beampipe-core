import logging
from typing import Any
from uuid import UUID

from ...config import settings
from ...db.database import local_session
from ...log_context import bind_execution_log_context
from ...restate_invoke import invoke_restate_workflow

logger = logging.getLogger(__name__)


def _parse_arq_job_context(ctx: Any) -> tuple[str | None, int | None]:
    """Best-effort ARQ job id and try number (dict ctx or object with attributes)."""
    if isinstance(ctx, dict):
        job_id = ctx.get("job_id")
        job_try = ctx.get("job_try")
    else:
        job_id = getattr(ctx, "job_id", None)
        job_try = getattr(ctx, "job_try", None)
    if job_id is not None:
        job_id = str(job_id)
    if job_try is not None and not isinstance(job_try, int):
        try:
            job_try = int(job_try)
        except (TypeError, ValueError):
            job_try = None
    return job_id, job_try


async def execute_run_job(
    ctx: Any,
    run_id: str,
    *,
    do_stage: bool = True,
    do_submit: bool = True,
) -> dict[str, Any]:
    run_uuid = UUID(str(run_id))
    arq_job_id, job_try = _parse_arq_job_context(ctx)

    with bind_execution_log_context(
        run_id=str(run_uuid),
        arq_job_id=arq_job_id,
        job_try=job_try,
    ):
        logger.info(
            "event=execute_run_job_start do_stage=%s do_submit=%s",
            do_stage,
            do_submit,
        )
        if (
            settings.WORKFLOW_ENGINE_EXECUTION == "restate"
            and settings.RESTATE_INGRESS_BASE_URL
        ):
            out = await invoke_restate_workflow(
                workflow_name=settings.RESTATE_EXECUTION_WORKFLOW_NAME,
                workflow_id=str(run_uuid),
                handler_name=settings.RESTATE_EXECUTION_WORKFLOW_HANDLER,
                payload={"do_stage": do_stage, "do_submit": do_submit},
                arq_job_id=arq_job_id,
                job_try=job_try,
            )
            return {"ok": True, "run_id": str(run_uuid), "restate": out}
        from ...orchestration.service import execute_run as orchestration_execute_run

        async with local_session() as db:
            result = await orchestration_execute_run(
                db=db, run_id=run_uuid, do_stage=do_stage, do_submit=do_submit
            )
            return {"ok": True, "run_id": str(run_uuid), "result": result}
