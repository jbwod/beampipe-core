"""Restate Engine

  https://docs.restate.dev/develop/python/concurrent-tasks
  https://docs.restate.dev/develop/python/services
  https://docs.restate.dev/develop/python/durable-steps
  https://docs.restate.dev/develop/python/error-handling
  https://docs.restate.dev/develop/python/serialization
  https://docs.restate.dev/develop/python/serving
  https://docs.restate.dev/foundations/key-concepts
"""
from datetime import UTC, datetime

import restate
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from .restate_workflows.discovery import DiscoveryBatchWorkflow
from .restate_workflows.execute import ExecutionBatchWorkflow
from .restate_workflows.hello import HelloWorldWorkflow
from .restate_workflows.slurm_completion import SlurmCompletionWorkflow


async def _health(_request: Request) -> JSONResponse:
    """Docker probe no Restate RPC paths."""
    return JSONResponse(
        {
            "status": "healthy",
            "service": "beamcore_rs",
            "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        }
    )


_restate_app = restate.app(
    [
        ExecutionBatchWorkflow,
        DiscoveryBatchWorkflow,
        HelloWorldWorkflow,
        SlurmCompletionWorkflow,
    ]
)
app = Starlette(
    routes=[
        Route("/health", _health, methods=["GET", "HEAD"]),
        Mount("/", _restate_app),
    ],
)
