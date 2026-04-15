import logging
from typing import Any
from uuid import UUID

from arq import Worker

from ...config import settings
from ...db.database import local_session
from ...log_context import bind_execution_log_context_from_arq
from ...restate_invoke import invoke_restate_workflow

logger = logging.getLogger(__name__)


async def execute_execution_job(
    ctx: Worker,
    execution_id: str,
    *,
    do_stage: bool = True,
    do_submit: bool = True,
) -> dict[str, Any]:
    execution_uuid = UUID(str(execution_id))
    with bind_execution_log_context_from_arq(
        ctx=ctx,
        execution_id=str(execution_uuid),
    ) as (arq_job_id, job_try):
        logger.info(
            "event=execute_execution_job_start do_stage=%s do_submit=%s",
            do_stage,
            do_submit,
        )
        if (
            settings.WORKFLOW_ENGINE_EXECUTION == "restate"
            and settings.RESTATE_INGRESS_BASE_URL
        ):
            out = await invoke_restate_workflow(
                workflow_name=settings.RESTATE_EXECUTION_WORKFLOW_NAME,
                workflow_id=str(execution_uuid),
                handler_name=settings.RESTATE_EXECUTION_WORKFLOW_HANDLER,
                payload={"do_stage": do_stage, "do_submit": do_submit},
                arq_job_id=arq_job_id,
                job_try=job_try,
            )
            return {"ok": True, "execution_id": str(execution_uuid), "restate": out}
        from ...orchestration.service import execute_execution as orchestration_execute_execution

        async with local_session() as db:
            result = await orchestration_execute_execution(
                db=db,
                execution_id=execution_uuid,
                do_stage=do_stage,
                do_submit=do_submit,
            )
            return {"ok": True, "execution_id": str(execution_uuid), "result": result}
