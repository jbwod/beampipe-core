import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import uvloop
from arq.worker import Worker

from ..archive.adapters import test as discovery_test
from ..archive.discovery import discover_schedule
from ..archive.service import archive_metadata_service
from ..db.database import local_session
from ..projects import list_project_modules, load_project_module
from ..registry.service import source_registry_service

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# -------- background tasks --------
async def sample_background_task(ctx: Worker, name: str) -> str:
    await asyncio.sleep(5)
    return f"Task {name} is complete!"

async def discover_schedule_task(ctx: Worker, project_module: str | None = None) -> dict[str, Any]:
    async with local_session() as db:
        return await discover_schedule(db=db, redis=ctx["redis"], project_module=project_module)


async def discover_batch(
    ctx: Worker, project_module: str, source_identifiers: list[str]
) -> dict[str, Any]:
    print(
        f"discover_batch: project_module={project_module} sources={len(source_identifiers)}"
    )
    return {
        "project_module": project_module,
        "total_sources": len(source_identifiers),
    }


async def timer_task(ctx: Worker) -> dict[str, Any]:
    modules = list_project_modules()
    if not modules:
        print("timer_task: no project modules registered")
        return {"status": "ok", "modules": []}

    for name in modules:
        try:
            module = load_project_module(name)
        except Exception as exc:
            print(f"timer_task: failed to load project module {name}: {exc}")
            continue
        ping_fn = getattr(module, "ping", None)
        if callable(ping_fn):
            print(f"timer_task: ping {name}")
            ping_fn()

    return {"status": "ok", "modules": modules}


# -------- base functions --------
async def startup(ctx: Worker) -> None:
    logging.info("Worker Started")


async def shutdown(ctx: Worker) -> None:
    logging.info("Worker end")
