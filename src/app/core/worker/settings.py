from arq import cron
from arq.connections import RedisSettings

from ...core.config import settings
from .functions import (
    discover_batch,
    discover_schedule_task,
    enqueue_timer_task,
    sample_background_task,
    shutdown,
    startup,
    timer_task,
)

REDIS_QUEUE_HOST = settings.REDIS_QUEUE_HOST
REDIS_QUEUE_PORT = settings.REDIS_QUEUE_PORT


class WorkerSettings:
    functions = [sample_background_task, discover_batch, timer_task]
    redis_settings = RedisSettings(host=REDIS_QUEUE_HOST, port=REDIS_QUEUE_PORT)
    queue_name = settings.WORKER_QUEUE_NAME
    on_startup = startup
    on_shutdown = shutdown
    handle_signals = False


class SchedulerSettings:
    functions = [sample_background_task, discover_schedule_task, enqueue_timer_task]
    redis_settings = RedisSettings(host=REDIS_QUEUE_HOST, port=REDIS_QUEUE_PORT)
    queue_name = settings.SCHEDULER_QUEUE_NAME
    on_startup = startup
    on_shutdown = shutdown
    handle_signals = False

    cron_jobs = [
        cron(discover_schedule_task, minute=settings.DISCOVERY_SCHEDULE_MINUTES),
        cron(enqueue_timer_task, second={0}),
    ]
