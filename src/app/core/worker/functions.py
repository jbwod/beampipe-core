import asyncio

import uvloop

from .tasks import (
    discover_batch,
    discover_schedule_task,
    enqueue_timer_task,
    execute_execution_job,
    sample_background_task,
    shutdown,
    startup,
    timer_task,
    workflow_execution_schedule_task,
)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

__all__ = [
    "discover_batch",
    "discover_schedule_task",
    "execute_execution_job",
    "enqueue_timer_task",
    "sample_background_task",
    "shutdown",
    "startup",
    "timer_task",
    "workflow_execution_schedule_task",
]
