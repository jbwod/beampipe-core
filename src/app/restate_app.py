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

from .restate_workflows import hello as _restate_hello
from .restate_workflows.hello import HelloWorldWorkflow

# Root ASGI app served by `uvicorn app.restate_app:app ...`
app = restate.app(
    [
        HelloWorldWorkflow
    ]
)
