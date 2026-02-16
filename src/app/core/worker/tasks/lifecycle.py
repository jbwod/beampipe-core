import logging

from arq.worker import Worker

logger = logging.getLogger(__name__)


async def startup(ctx: Worker) -> None:
    _ = ctx
    logger.info("event=worker_lifecycle action=started")


async def shutdown(ctx: Worker) -> None:
    _ = ctx
    logger.info("event=worker_lifecycle action=shutdown")
