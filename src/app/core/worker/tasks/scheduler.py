import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from arq.worker import Worker

from ...archive.discovery import discover_schedule
from ...config import settings
from ...db.database import local_session
from ...projects import list_project_modules, load_project_module

logger = logging.getLogger(__name__)


async def sample_background_task(ctx: Worker, name: str) -> str:
    _ = ctx
    await asyncio.sleep(5)
    return f"Task {name} is complete!"


async def discover_schedule_task(ctx: Worker, project_module: str | None = None) -> dict[str, Any]:
    try:
        async with local_session() as db:
            result = await discover_schedule(db=db, redis=ctx["redis"], project_module=project_module)
            if "ok" not in result:
                result["ok"] = True
            total_sources = result.get("total_sources", 0)
            total_jobs = result.get("total_jobs", 0)
            is_skipped = (
                result.get("ok")
                and total_sources == 0
                and total_jobs == 0
                and not result.get("enqueue_failures")
                and not result.get("skipped_due_to_queue_full")
                and not result.get("skipped_due_to_tap_unreachable")
            )
            if is_skipped:
                logger.info(
                    "event=discover_schedule_task project_module=%s skipped",
                    project_module or "all",
                )
            else:
                logger.info(
                    "event=discover_schedule_task_result "
                    "project_module=%s scheduled_at=%s ok=%s total_sources=%s "
                    "total_jobs=%s enqueue_failures=%s skipped_due_to_queue_full=%s skipped_due_to_tap_unreachable=%s tap_unreachable=%s",
                    project_module or "all",
                    result.get("scheduled_at"),
                    result.get("ok"),
                    total_sources,
                    total_jobs,
                    result.get("enqueue_failures"),
                    result.get("skipped_due_to_queue_full"),
                    result.get("skipped_due_to_tap_unreachable"),
                    result.get("tap_unreachable"),
                )
            return result
    except Exception as exc:
        logger.exception(
            "event=discover_schedule_task_error project_module=%s error=%s",
            project_module,
            exc,
        )
        return {
            "ok": False,
            "error": str(exc),
            "project_module": project_module,
            "scheduled_at": datetime.now(UTC).isoformat(),
        }


async def enqueue_timer_task(ctx: Worker) -> dict[str, Any]:
    redis = ctx.get("redis")
    if redis is None:
        raise RuntimeError("Redis queue is not available for timer enqueue")

    job = await redis.enqueue_job(
        "timer_task",
        _queue_name=settings.WORKER_QUEUE_NAME,
    )
    return {"status": "ok", "job_id": job.job_id if job else None}


async def timer_task(ctx: Worker) -> dict[str, Any]:
    _ = ctx
    modules = list_project_modules()
    if not modules:
        logger.info("event=timer_task_no_modules")
        return {"status": "ok", "modules": []}

    for name in modules:
        try:
            module = load_project_module(name)
        except Exception as exc:
            logger.warning(
                "event=timer_task_module_load_failed module=%s error=%s",
                name,
                exc,
            )
            continue
        ping_fn = getattr(module, "ping", None)
        if callable(ping_fn):
            logger.info("event=timer_task_ping module=%s", name)
            ping_fn()

    return {"status": "ok", "modules": modules}
