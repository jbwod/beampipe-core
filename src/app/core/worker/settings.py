from arq import cron
from arq.connections import RedisSettings

from ...core import logger as _app_logger  # noqa: F401

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


def _discovery_schedule_minutes() -> set[int]:
    interval = max(1, settings.DISCOVERY_SCHEDULE_MINUTES)
    if interval >= 60:
        return {0}
    return set(range(0, 60, interval))


class WorkerSettings:
    functions = [sample_background_task, discover_batch, timer_task]
    redis_settings = RedisSettings(host=REDIS_QUEUE_HOST, port=REDIS_QUEUE_PORT)
    queue_name = settings.WORKER_QUEUE_NAME
    on_startup = startup
    on_shutdown = shutdown
    handle_signals = False
    job_try = 3
    job_result_ttl = 3600


class SchedulerSettings:
    functions = [sample_background_task, discover_schedule_task, enqueue_timer_task]
    redis_settings = RedisSettings(host=REDIS_QUEUE_HOST, port=REDIS_QUEUE_PORT)
    queue_name = settings.SCHEDULER_QUEUE_NAME
    on_startup = startup
    on_shutdown = shutdown
    handle_signals = False
    job_try = 3
    job_result_ttl = 3600

    cron_jobs = [
        cron(
            discover_schedule_task,  # type: ignore[arg-type]
            minute=_discovery_schedule_minutes(),
            second={0},
        ),
        # cron(enqueue_timer_task, second={0}),
    ]
