"""Hello-world Workflow"""

from typing import Any

import restate
from pydantic import BaseModel, ConfigDict, ValidationError

from ..core.exceptions.workflow_exceptions import WorkflowErrorCode, WorkflowFailure
from .options import _run_opts_database
from .runtime import _ingress_terminal, _run_step

HelloWorldWorkflow = restate.Workflow("HelloWorldWorkflow")


class HelloWorldWorkflowInput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str = "world"


async def _hello_world_greet(name: str) -> dict[str, str]:
    return {"message": f"hello, {name}"}


@HelloWorldWorkflow.main()
async def hello_world_workflow(
    ctx: restate.WorkflowContext,
    req: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw = req if req is not None else {}
    if not isinstance(raw, dict):
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.EXECUTION_INVALID_PAYLOAD,
                "HelloWorldWorkflow payload must be a JSON object or omitted",
            )
        )
    try:
        body = HelloWorldWorkflowInput.model_validate(raw)
    except ValidationError as e:
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.EXECUTION_INVALID_PAYLOAD,
                f"Invalid hello_world workflow payload: {e}",
                cause=e,
            )
        )

    out = await _run_step(
        ctx,
        "hello_world.greet",
        _run_opts_database(),
        _hello_world_greet,
        name=body.name,
    )
    return {**out, "workflow_key": ctx.key()}
