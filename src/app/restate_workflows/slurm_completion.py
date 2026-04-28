"""Poll a previously submitted SLURM job until it reaches a terminal state.

1. Restate workflow id = execution_id
2. Read snapshot load ledger row; if already terminal, return immediately.
3. Poll loop call poll_dim_session_for_execution (routes to
   slurm.poll_session until terminal
4. Sleep between polls
5. Timeout if max rounds exceeded, mark ledger failed (if still non-terminal).

start off from execution workflow after submit moves status to AWAITING_SCHEDULER. 
RESTATE_SLURM_REMOTE_POLL_MAX_ROUNDS RESTATE_SLURM_REMOTE_POLL_INTERVAL_SECONDS resolve_workflow_execute_step_overrides
invoke_restate_workflow(..., workflow_name=..., workflow_id=execution_id)

    async def handler(ctx: restate.WorkflowContext, req: dict | None) -> dict:
        execution_id = ctx.key()  # UUID string
        # validate key + payload, bind log context, delegate to body
"""

import logging
from datetime import timedelta
from typing import Any
from uuid import UUID

import restate
from restate.exceptions import TerminalError

from ..core.config import settings
from ..core.db.database import local_session
from ..core.exceptions.workflow_exceptions import WorkflowErrorCode, WorkflowFailure
from ..crud.crud_execution_record import crud_batch_execution_records
from ..core.ledger.run_record import merge_restate_slurm_completion_timeout_into_manifest
from ..core.ledger.service import execution_ledger_service
from ..core.log_context import bind_execution_log_context
from ..core.orchestration import service as orchestration_service
from ..core.positive_policy import positive_int
from ..core.projects import resolve_workflow_execute_step_overrides
from ..models.ledger import ExecutionStatus
from ..schemas.ledger import BatchExecutionRecordRead
from .options import _run_opts_database, _run_opts_poll
from .runtime import _ingress_terminal, _run_step

logger = logging.getLogger(__name__)

SlurmCompletionWorkflow = restate.Workflow("SlurmCompletionWorkflow")


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


async def _completion_read_snapshot(execution_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.read_execution_ledger_snapshot(
            db=db, execution_id=UUID(execution_id)
        )


async def _completion_poll_slurm(execution_id: str) -> dict[str, Any]:
    async with local_session() as db:
        return await orchestration_service.poll_dim_session_for_execution(
            db=db, execution_id=UUID(execution_id)
        )


async def _completion_mark_failed_if_non_terminal(
    execution_id: str,
    *,
    error: str,
) -> None:
    async with local_session() as db:
        snapshot = await orchestration_service.read_execution_ledger_snapshot(
            db=db, execution_id=UUID(execution_id)
        )
        status = str(snapshot.get("status") or "")
        if status in {
            ExecutionStatus.COMPLETED.value,
            ExecutionStatus.FAILED.value,
            ExecutionStatus.CANCELLED.value,
        }:
            return
        row = await crud_batch_execution_records.get(
            db=db,
            uuid=UUID(execution_id),
            schema_to_select=BatchExecutionRecordRead,
        )
        wm = row.get("workflow_manifest") if isinstance(row, dict) else None
        merged_manifest = merge_restate_slurm_completion_timeout_into_manifest(wm, error=error)
        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=UUID(execution_id),
            status=ExecutionStatus.FAILED,
            error=error,
            execution_phase=None,
            workflow_manifest=merged_manifest,
        )



async def _slurm_completion_workflow_body(
    ctx: restate.WorkflowContext,
    execution_id: str,
) -> dict[str, Any]:
    snapshot = await _run_step(
        ctx,
        "slurm_completion.read_snapshot_start",
        _run_opts_database(),
        _completion_read_snapshot,
        execution_id=execution_id,
    )

    run_policy_overrides: dict[str, Any] = {}
    project_module = snapshot.get("project_module") if isinstance(snapshot, dict) else None
    if isinstance(project_module, str) and project_module:
        run_policy_overrides = resolve_workflow_execute_step_overrides(project_module)

    if isinstance(snapshot, dict) and snapshot.get("status") in {"completed", "failed", "cancelled"}:
        return {
            **snapshot,
            "terminal": True,
            "execution_id": execution_id,
            "ledger": snapshot,
        }

    max_polls = positive_int(
        run_policy_overrides,
        "slurm_remote_poll_max_rounds",
        settings.RESTATE_SLURM_REMOTE_POLL_MAX_ROUNDS,
    )
    poll_interval = settings.RESTATE_SLURM_REMOTE_POLL_INTERVAL_SECONDS

    poll_round = 0
    while poll_round < max_polls:
        poll = await _run_step(
            ctx,
            f"slurm_completion.poll.{poll_round}",
            _run_opts_poll(run_policy_overrides),
            _completion_poll_slurm,
            execution_id=execution_id,
        )
        poll_round += 1
        if poll.get("terminal"):
            ledger = await _run_step(
                ctx,
                "slurm_completion.read_snapshot",
                _run_opts_database(run_policy_overrides),
                _completion_read_snapshot,
                execution_id=execution_id,
            )
            return {**poll, "execution_id": execution_id, "ledger": ledger}
        await ctx.sleep(delta=timedelta(seconds=poll_interval))

    _ingress_terminal(
        WorkflowFailure(
            WorkflowErrorCode.EXECUTION_SLURM_STATE,
            f"SLURM job (slurm_completion.poll) exceeded {max_polls} rounds "
            f"({max_polls * poll_interval:.0f}s) without reaching a terminal state",
        )
    )


@SlurmCompletionWorkflow.main()
async def slurm_completion_workflow(
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
                "SlurmCompletionWorkflow payload must be a JSON object or omitted",
            )
        )

    arq_job_id = raw.get("arq_job_id") if isinstance(raw.get("arq_job_id"), str) else None
    job_try_raw = raw.get("arq_job_try")
    job_try = int(job_try_raw) if isinstance(job_try_raw, (int, str)) and str(job_try_raw).isdigit() else None

    with bind_execution_log_context(
        execution_id=str(execution_id),
        arq_job_id=arq_job_id,
        job_try=job_try,
    ):
        try:
            return await _slurm_completion_workflow_body(ctx, execution_id)
        except TerminalError as e:
            await _run_step(
                ctx,
                "slurm_completion.mark_failed",
                _run_opts_database(),
                _completion_mark_failed_if_non_terminal,
                execution_id=execution_id,
                error=str(e),
            )
            logger.exception(
                "event=slurm_completion_terminal execution_id=%s", execution_id
            )
            raise
