"""Execute-run DIM translate/deploy/poll."""

from datetime import timedelta
from typing import Any
from uuid import UUID

import restate
from pydantic import BaseModel, ConfigDict, ValidationError

from ..core.config import settings
from ..core.db.database import local_session
from ..core.exceptions.workflow_exceptions import WorkflowErrorCode, WorkflowFailure
from ..core.log_context import bind_execution_log_context
from ..core.orchestration import service as orchestration_service
from .options import _run_opts_database, _run_opts_external_io, _run_opts_poll
from .runtime import _ingress_terminal, _run_step

ExecuteRunWorkflow = restate.Workflow("ExecuteRunWorkflow")


class ExecuteRunWorkflowInput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    do_stage: bool = True
    do_submit: bool = True
    arq_job_id: str | None = None
    arq_job_try: int | None = None


def _require_uuid_workflow_key(run_id: str) -> None:
    try:
        UUID(str(run_id))
    except ValueError as e:
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.EXEC_RUN_INVALID_WORKFLOW_KEY,
                f"Workflow key must be a UUID string (run id); got {run_id!r}",
                cause=e,
            )
        )


async def _execution_ack_started(run_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.begin_restate_execution_for_run(
            db=db, run_id=UUID(run_id)
        )


async def _execution_read_snapshot(run_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.read_run_ledger_snapshot(
            db=db, run_id=UUID(run_id)
        )


async def _execution_read_existing_manifest(run_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.read_existing_workflow_manifest(
            db=db, run_id=UUID(run_id)
        )


async def _execution_stage_sources(run_id: str, do_stage: bool) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.stage_sources_for_run(
            db=db,
            run_id=UUID(run_id),
            do_stage=do_stage,
        )


async def _execution_build_manifest(run_id: str, stage_out: dict[str, Any]) -> dict[str, Any]:
    stage_out = stage_out if isinstance(stage_out, dict) else {}
    async with local_session() as db:
        return await orchestration_service.build_manifest_for_run(
            db=db,
            run_id=UUID(run_id),
            staged_urls_by_scan_id=stage_out.get("staged_urls_by_scan_id") or {},
            eval_urls_by_sbid=stage_out.get("eval_urls_by_sbid") or {},
            checksum_urls_by_scan_id=stage_out.get("checksum_urls_by_scan_id") or {},
            eval_checksum_urls_by_sbid=stage_out.get("eval_checksum_urls_by_sbid") or {},
        )


async def _execution_translate_dim(run_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.translate_dim_session_for_run(
            db=db, run_id=UUID(run_id)
        )


async def _execution_deploy_dim(
    run_id: str,
    session_id: str,
    pg_spec: list[Any],
    roots: list[Any],
    dim_base: str,
    verify_ssl: bool,
) -> dict[str, Any]:
    async with local_session() as db:
        await orchestration_service.deploy_dim_session_payload_for_run(
            db=db,
            run_id=UUID(run_id),
            session_id=session_id,
            pg_spec=list(pg_spec),
            roots=list(roots),
            dim_base=dim_base,
            verify_ssl=verify_ssl,
        )
    return {"session_id": session_id}


async def _execution_poll_dim(run_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.poll_dim_session_for_run(
            db=db, run_id=UUID(run_id)
        )


async def _execute_run_no_submit_step(run_id: str, do_stage: bool) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.execute_run(
            db=db,
            run_id=UUID(str(run_id)),
            do_stage=do_stage,
            do_submit=False,
        )


async def _execute_run_workflow_body(
    ctx: restate.WorkflowContext,
    run_id: str,
    exec_req: ExecuteRunWorkflowInput,
) -> dict[str, Any]:
    do_stage = exec_req.do_stage
    do_submit = exec_req.do_submit

    if not do_submit:
        await _run_step(
            ctx,
            "execution.ack_started",
            _run_opts_database(),
            _execution_ack_started,
            run_id=run_id,
        )
        out = await _run_step(
            ctx,
            "execute_run_no_submit_step",
            _run_opts_external_io(),
            _execute_run_no_submit_step,
            run_id=run_id,
            do_stage=do_stage,
        )
        ledger = await _run_step(
            ctx,
            "execution.read_snapshot",
            _run_opts_database(),
            _execution_read_snapshot,
            run_id=run_id,
        )
        return {**out, "run_id": run_id, "ledger": ledger}

    await _run_step(
        ctx,
        "execution.ack_started",
        _run_opts_database(),
        _execution_ack_started,
        run_id=run_id,
    )

    # Extra visibility early
    await _run_step(
        ctx,
        "execution.read_snapshot_start",
        _run_opts_database(),
        _execution_read_snapshot,
        run_id=run_id,
    )

    manifest = await _run_step(
        ctx,
        "execution.probe_manifest",
        _run_opts_database(),
        _execution_read_existing_manifest,
        run_id=run_id,
    )
    if not manifest:
        stage_out = await _run_step(
            ctx,
            "execution.stage_sources",
            _run_opts_external_io(),
            _execution_stage_sources,
            run_id=run_id,
            do_stage=do_stage,
        )
        await _run_step(
            ctx,
            "execution.build_manifest",
            _run_opts_database(),
            _execution_build_manifest,
            run_id=run_id,
            stage_out=stage_out,
        )

    tr = await _run_step(
        ctx,
        "execution.translate_dim",
        _run_opts_external_io(),
        _execution_translate_dim,
        run_id=run_id,
    )
    if tr["status"] == "ready":
        await _run_step(
            ctx,
            "execution.deploy_dim",
            _run_opts_external_io(),
            _execution_deploy_dim,
            run_id=run_id,
            session_id=tr["session_id"],
            pg_spec=tr["pg_spec"],
            roots=tr["roots"],
            dim_base=tr["dim_base"],
            verify_ssl=tr["verify_ssl"],
        )

    poll_round = 0
    while True:
        poll = await _run_step(
            ctx,
            f"execution.poll_dim.{poll_round}",
            _run_opts_poll(),
            _execution_poll_dim,
            run_id=run_id,
        )
        poll_round += 1
        if poll.get("terminal"):
            ledger = await _run_step(
                ctx,
                "execution.read_snapshot",
                _run_opts_database(),
                _execution_read_snapshot,
                run_id=run_id,
            )
            return {**poll, "run_id": run_id, "ledger": ledger}
        await ctx.sleep(delta=timedelta(seconds=settings.RESTATE_DIM_POLL_INTERVAL_SECONDS))


@ExecuteRunWorkflow.main()
async def execute_run_workflow(
    ctx: restate.WorkflowContext,
    req: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run_id = ctx.key()
    _require_uuid_workflow_key(run_id)

    raw = req if req is not None else {}
    if not isinstance(raw, dict):
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.EXEC_RUN_INVALID_PAYLOAD,
                "ExecuteRunWorkflow payload must be a JSON object or omitted",
            )
        )
    try:
        exec_req = ExecuteRunWorkflowInput.model_validate(raw)
    except ValidationError as e:
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.EXEC_RUN_INVALID_PAYLOAD,
                f"Invalid execute_run workflow payload: {e}",
                cause=e,
            )
        )

    with bind_execution_log_context(
        run_id=str(run_id),
        arq_job_id=exec_req.arq_job_id,
        job_try=exec_req.arq_job_try,
    ):
        return await _execute_run_workflow_body(ctx, run_id, exec_req)
