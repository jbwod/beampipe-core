import inspect
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
    async def _invoke() -> Any:
        try:
            if inspect.iscoroutinefunction(fn):
                return await fn(**kwargs)
            result = fn(**kwargs)
            if inspect.isawaitable(result):
                return await result
            return result
        except WorkflowFailure as wf:
            # Convert the WorkflowFailure to a TerminalError
            raise TerminalError(wf.format_for_terminal()) from wf

    return await ctx.run_typed(step_name, _invoke, opts)
