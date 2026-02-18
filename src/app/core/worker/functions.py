import asyncio
import logging

import uvloop
from ...core.logger import LOGGING_LEVEL, LOGGING_FORMAT

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

__all__ = [
    "discover_batch",
    "discover_schedule_task",
    "enqueue_timer_task",
    "sample_background_task",
    "shutdown",
    "startup",
    "timer_task",
]
