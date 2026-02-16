import asyncio
import logging

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

__all__ = [
    "discover_batch",
    "discover_schedule_task",
    "enqueue_timer_task",
    "sample_background_task",
    "shutdown",
    "startup",
    "timer_task",
]
