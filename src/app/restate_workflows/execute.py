"""Execute DIM translate/deploy/poll."""

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
from ..core.projects import resolve_workflow_execute_step_overrides
from .options import _run_opts_database, _run_opts_external_io, _run_opts_poll
from .runtime import _ingress_terminal, _run_step

ExecutionBatchWorkflow = restate.Workflow("ExecutionBatchWorkflow")


class ExecutionBatchWorkflowInput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    do_stage: bool = True
    do_submit: bool = True
    arq_job_id: str | None = None
    arq_job_try: int | None = None


def _require_uuid_workflow_key(execution_id: str) -> None:
    try:
        UUID(str(execution_id))
    except ValueError as e:
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.EXECUTION_INVALID_WORKFLOW_KEY,
                f"Workflow key must be a UUID string (execution id); got {execution_id!r}",
                cause=e,
            )
        )


async def _execution_ack_started(execution_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.begin_restate_execution_for_execution(
            db=db, execution_id=UUID(execution_id)
        )


async def _execution_read_snapshot(execution_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.read_execution_ledger_snapshot(
            db=db, execution_id=UUID(execution_id)
        )


async def _execution_read_existing_manifest(execution_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.read_existing_workflow_manifest(
            db=db, execution_id=UUID(execution_id)
        )


async def _execution_stage_sources(execution_id: str, do_stage: bool) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.stage_sources_for_execution(
            db=db,
            execution_id=UUID(execution_id),
            do_stage=do_stage,
        )


async def _execution_build_manifest(execution_id: str, stage_out: dict[str, Any]) -> dict[str, Any]:
    stage_out = stage_out if isinstance(stage_out, dict) else {}
    async with local_session() as db:
        return await orchestration_service.build_manifest_for_execution(
            db=db,
            execution_id=UUID(execution_id),
            staged_urls_by_scan_id=stage_out.get("staged_urls_by_scan_id") or {},
            eval_urls_by_sbid=stage_out.get("eval_urls_by_sbid") or {},
            checksum_urls_by_scan_id=stage_out.get("checksum_urls_by_scan_id") or {},
            eval_checksum_urls_by_sbid=stage_out.get("eval_checksum_urls_by_sbid") or {},
        )


async def _execution_translate_dim(execution_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.translate_dim_session_for_execution(
            db=db, execution_id=UUID(execution_id)
        )


async def _execution_deploy_dim(
    execution_id: str,
    session_id: str,
    pg_spec: list[Any],
    roots: list[Any],
    dim_base: str,
    verify_ssl: bool,
) -> dict[str, Any]:
    async with local_session() as db:
        await orchestration_service.deploy_dim_session_payload_for_execution(
            db=db,
            execution_id=UUID(execution_id),
            session_id=session_id,
            pg_spec=list(pg_spec),
            roots=list(roots),
            dim_base=dim_base,
            verify_ssl=verify_ssl,
        )
    return {"session_id": session_id}


async def _execution_poll_dim(execution_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.poll_dim_session_for_execution(
            db=db, execution_id=UUID(execution_id)
        )


async def _execute_execution_no_submit_step(execution_id: str, do_stage: bool) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.execute_execution(
            db=db,
            execution_id=UUID(str(execution_id)),
            do_stage=do_stage,
            do_submit=False,
        )


async def _execute_execution_workflow_body(
    ctx: restate.WorkflowContext,
    execution_id: str,
    exec_req: ExecutionBatchWorkflowInput,
) -> dict[str, Any]:
    do_stage = exec_req.do_stage
    do_submit = exec_req.do_submit
    run_policy_overrides: dict[str, Any] = {}

    def _extract_overrides(snapshot: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(snapshot, dict):
            return {}
        project_module = snapshot.get("project_module")
        if not isinstance(project_module, str) or not project_module:
            return {}
        return resolve_workflow_execute_step_overrides(project_module)

    if not do_submit:
        await _run_step(
            ctx,
            "execution.ack_started",
            _run_opts_database(),
            _execution_ack_started,
            execution_id=execution_id,
        )
        snapshot_start = await _run_step(
            ctx,
            "execution.read_snapshot_start",
            _run_opts_database(),
            _execution_read_snapshot,
            execution_id=execution_id,
        )
        run_policy_overrides = _extract_overrides(snapshot_start)
        out = await _run_step(
            ctx,
            "execute_execution_no_submit_step",
            _run_opts_external_io(run_policy_overrides),
            _execute_execution_no_submit_step,
            execution_id=execution_id,
            do_stage=do_stage,
        )
        ledger = await _run_step(
            ctx,
            "execution.read_snapshot",
            _run_opts_database(run_policy_overrides),
            _execution_read_snapshot,
            execution_id=execution_id,
        )
        return {**out, "execution_id": execution_id, "ledger": ledger}

    await _run_step(
        ctx,
        "execution.ack_started",
        _run_opts_database(),
        _execution_ack_started,
        execution_id=execution_id,
    )

    # Extra visibility early
    snapshot_start = await _run_step(
        ctx,
        "execution.read_snapshot_start",
        _run_opts_database(),
        _execution_read_snapshot,
        execution_id=execution_id,
    )
    run_policy_overrides = _extract_overrides(snapshot_start)

    manifest = await _run_step(
        ctx,
        "execution.probe_manifest",
        _run_opts_database(run_policy_overrides),
        _execution_read_existing_manifest,
        execution_id=execution_id,
    )
    if not manifest:
        stage_out = await _run_step(
            ctx,
            "execution.stage_sources",
            _run_opts_external_io(run_policy_overrides),
            _execution_stage_sources,
            execution_id=execution_id,
            do_stage=do_stage,
        )
        await _run_step(
            ctx,
            "execution.build_manifest",
            _run_opts_database(run_policy_overrides),
            _execution_build_manifest,
            execution_id=execution_id,
            stage_out=stage_out,
        )

    tr = await _run_step(
        ctx,
        "execution.translate_dim",
        _run_opts_external_io(run_policy_overrides),
        _execution_translate_dim,
        execution_id=execution_id,
    )
    if tr["status"] == "ready":
        await _run_step(
            ctx,
            "execution.deploy_dim",
            _run_opts_external_io(run_policy_overrides),
            _execution_deploy_dim,
            execution_id=execution_id,
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
            _run_opts_poll(run_policy_overrides),
            _execution_poll_dim,
            execution_id=execution_id,
        )
        poll_round += 1
        if poll.get("terminal"):
            ledger = await _run_step(
                ctx,
                "execution.read_snapshot",
                _run_opts_database(run_policy_overrides),
                _execution_read_snapshot,
                execution_id=execution_id,
            )
            return {**poll, "execution_id": execution_id, "ledger": ledger}
        await ctx.sleep(delta=timedelta(seconds=settings.RESTATE_DIM_POLL_INTERVAL_SECONDS))


@ExecutionBatchWorkflow.main()
async def execute_execution_workflow(
    ctx: restate.WorkflowContext,
    req: dict[str, Any] | None = None,
) -> dict[str, Any]:
    execution_id = ctx.key()
    _require_uuid_workflow_key(execution_id)

    raw = req if req is not None else {}
    if not isinstance(raw, dict):
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.EXECUTION_INVALID_PAYLOAD,
                "ExecutionBatchWorkflow payload must be a JSON object or omitted",
            )
        )
    try:
        exec_req = ExecutionBatchWorkflowInput.model_validate(raw)
    except ValidationError as e:
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.EXECUTION_INVALID_PAYLOAD,
                f"Invalid execute_execution workflow payload: {e}",
                cause=e,
            )
        )

    with bind_execution_log_context(
        execution_id=str(execution_id),
        arq_job_id=exec_req.arq_job_id,
        job_try=exec_req.arq_job_try,
    ):
        return await _execute_execution_workflow_body(ctx, execution_id, exec_req)
