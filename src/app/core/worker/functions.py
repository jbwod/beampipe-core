import asyncio

import uvloop

from .tasks import (
    discover_batch,
    discover_schedule_task,
    enqueue_timer_task,
    sample_background_task,
    shutdown,
    startup,
    timer_task,
)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

__all__ = [
    "discover_batch",
    "discover_schedule_task",
    "enqueue_timer_task",
    "sample_background_task",
    "shutdown",
    "startup",
    "timer_task",
]
