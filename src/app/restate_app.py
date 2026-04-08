"""Restate Engine

  https://docs.restate.dev/develop/python/concurrent-tasks
  https://docs.restate.dev/develop/python/services
  https://docs.restate.dev/develop/python/durable-steps
  https://docs.restate.dev/develop/python/error-handling
  https://docs.restate.dev/develop/python/serialization
  https://docs.restate.dev/develop/python/serving
  https://docs.restate.dev/foundations/key-concepts
"""

import restate

from .restate_workflows import discovery as _restate_discovery
from .restate_workflows import execute as _restate_execute
from .restate_workflows import hello as _restate_hello
from .restate_workflows.discovery import DiscoveryBatchWorkflow
from .restate_workflows.execute import ExecutionBatchWorkflow
from .restate_workflows.hello import HelloWorldWorkflow

# Root ASGI app served by `uvicorn app.restate_app:app ...`
app = restate.app(
    [
        
        ExecutionBatchWorkflow,
        DiscoveryBatchWorkflow,
        HelloWorldWorkflow,
    ]
)
