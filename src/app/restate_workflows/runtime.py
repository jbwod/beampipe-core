from typing import Any, NoReturn

import restate
from restate.exceptions import TerminalError

from ..core.exceptions.workflow_exceptions import WorkflowFailure


def _ingress_terminal(wf: WorkflowFailure) -> NoReturn:
    raise TerminalError(wf.format_for_terminal()) from wf


async def _run_step(
    #
    ctx: restate.WorkflowContext,
    step_name: str,
    opts: restate.RunOptions[Any],
    fn: Any,
    /,
    **kwargs: Any,
) -> Any:
    try:
        return await ctx.run_typed(step_name, fn, opts, **kwargs)
    except WorkflowFailure as wf:
        raise TerminalError(wf.format_for_terminal()) from wf
## _run_step ends here