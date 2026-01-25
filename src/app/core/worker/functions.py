import asyncio
import logging
from typing import Any

import uvloop
from arq.worker import Worker

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# -------- background tasks --------
async def sample_background_task(ctx: Worker, name: str) -> str:
    await asyncio.sleep(5)
    return f"Task {name} is complete!"

async def discover_schedule_task(ctx: Worker, project_module: str | None = None) -> dict[str, Any]:
    print(f"discover_schedule_task: project_module={project_module}")
    return {"status": "ok", "project_module": project_module}


async def discover_batch(
    ctx: Worker, project_module: str, source_identifiers: list[str]
) -> dict[str, Any]:
    print(
        f"discover_batch: project_module={project_module} sources={len(source_identifiers)}"
    )
    return {
        "status": "ok",
        "project_module": project_module,
        "total_sources": len(source_identifiers),
    }


# -------- base functions --------
async def startup(ctx: Worker) -> None:
    logging.info("Worker Started")


async def shutdown(ctx: Worker) -> None:
    logging.info("Worker end")
