"""
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


from typing import Any

async def slurm_completion_read_snapshot(execution_id: str) -> dict[str, Any]:
    raise NotImplementedError


async def slurm_completion_poll_tick(execution_id: str) -> dict[str, Any]:
    raise NotImplementedError


async def slurm_completion_mark_failed_if_non_terminal(
    execution_id: str, *, error: str
) -> None:
    raise NotImplementedError


async def slurm_completion_workflow_body(ctx: Any, execution_id: str) -> dict[str, Any]:
    raise NotImplementedError


async def slurm_completion_main(
    ctx: Any, req: dict[str, Any] | None = None
) -> dict[str, Any]:
    raise NotImplementedError


__all__ = [
    "slurm_completion_main",
    "slurm_completion_mark_failed_if_non_terminal",
    "slurm_completion_poll_tick",
    "slurm_completion_read_snapshot",
    "slurm_completion_workflow_body",
]
