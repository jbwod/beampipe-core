import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import uvloop
from arq.worker import Worker

from ..archive.adapters import test as discovery_test
from ..archive.discovery import discover_schedule
from ..archive.service import archive_metadata_service
from ..config import settings
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

async def enqueue_timer_task(ctx: Worker) -> dict[str, Any]:
    redis = ctx.get("redis")
    if redis is None:
        raise RuntimeError("Redis queue is not available for timer enqueue")

    job = await redis.enqueue_job(
        "timer_task",
        _queue_name=settings.WORKER_QUEUE_NAME,
    )
    return {"status": "ok", "job_id": job.job_id if job else None}


def _group_metadata_by_sbid(metadata_list: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in metadata_list:
        sbid = item.get("sbid")
        if sbid is None:
            logger.warning(f"Dataset missing SBID: {item.get('dataset_id', 'unknown')}")
            continue
        key = str(sbid)
        grouped.setdefault(key, []).append(item)
    return grouped


async def discover_batch(
    ctx: Worker, project_module: str, source_identifiers: list[str]
) -> dict[str, Any]:
    module = load_project_module(project_module)
    now = datetime.now(UTC)

    processed_source_ids: list[Any] = []
    total_datasets = 0
    total_sbids = 0

    async with local_session() as db:
        logger.info(
            f"discover_batch: project_module={project_module} sources={len(source_identifiers)}"
        )
        for source_identifier in source_identifiers:
            try:
                # module.discover() to get query results
                discover_fn = getattr(module, "discover", None)
                if discover_fn:
                    query_results = discover_fn(source_identifier)
                else:
                    logger.warning(f"Module {project_module} has no discover() function, using fallback")
                    query_results = discovery_test.query_casda_visibility_files(source_identifier)

                if len(query_results) == 0:
                    logger.info(f"No datasets found for {source_identifier}")
                    source = await source_registry_service.check_existing_source(
                        db, project_module, source_identifier
                    )
                    if source and source.get("uuid"):
                        processed_source_ids.append(source["uuid"])
                    continue

                # later
                data_url_by_scan_id: dict[str, str] | None = None
                checksum_url_by_scan_id: dict[str, str] | None = None

                #  module.prepare_metadata()
                prepare_fn = getattr(module, "prepare_metadata", None)
                if prepare_fn:
                    metadata_list = prepare_fn(
                        source_identifier=source_identifier,
                        query_results=query_results,
                        data_url_by_scan_id=data_url_by_scan_id,
                        checksum_url_by_scan_id=checksum_url_by_scan_id,
                    )
                else:
                    logger.warning(f"Module {project_module} has no prepare_metadata() function, using fallback")
                    metadata_list = discovery_test.prepare_metadata(
                        source_identifier=source_identifier,
                        query_results=query_results,
                        data_url_by_scan_id=data_url_by_scan_id,
                        checksum_url_by_scan_id=checksum_url_by_scan_id,
                    )

                grouped = _group_metadata_by_sbid(metadata_list)
                logger.info(
                    f"discover_batch: {project_module}/{source_identifier} "
                    f"sbids={len(grouped)} datasets={len(metadata_list)}"
                )

                # Upsert
                for sbid, datasets in grouped.items():
                    try:
                        await archive_metadata_service.upsert_metadata(
                            db=db,
                            project_module=project_module,
                            source_identifier=source_identifier,
                            sbid=str(sbid),
                            metadata_json={"datasets": datasets},
                        )
                    except Exception as e:
                        logger.error(
                            f"Error upserting metadata for {source_identifier} SBID {sbid}: {e}",
                            exc_info=True,
                        )
                        await db.rollback()
                        raise
                
                await db.commit()
                logger.info(f"Committed metadata for {source_identifier}: {len(grouped)} SBIDs, {len(metadata_list)} datasets")
                
                total_sbids += len(grouped)
                total_datasets += len(metadata_list)

                source = await source_registry_service.check_existing_source(
                    db, project_module, source_identifier
                )
                if source and source.get("uuid"):
                    processed_source_ids.append(source["uuid"])

            except Exception as e:
                logger.error(
                    f"Error processing source {source_identifier} in module {project_module}: {e}",
                    exc_info=True,
                )
                try:
                    await db.rollback()
                except Exception as rollback_error:
                    logger.warning(f"Error during rollback: {rollback_error}")
                continue

        await source_registry_service.mark_sources_checked(
            db, processed_source_ids, checked_at=now
        )
        logger.info(
            f"discover_batch: marked last_checked_at for {len(processed_source_ids)} sources"
        )

    return {
        "project_module": project_module,
        "total_sources": len(source_identifiers),
        "total_sbids": total_sbids,
        "total_datasets": total_datasets,
    }


async def timer_task(ctx: Worker) -> dict[str, Any]:
    modules = list_project_modules()
    if not modules:
        logger.info("timer_task: no project modules registered")
        return {"status": "ok", "modules": []}

    for name in modules:
        try:
            module = load_project_module(name)
        except Exception as exc:
            logger.warning(f"timer_task: failed to load project module {name}: {exc}")
            continue
        ping_fn = getattr(module, "ping", None)
        if callable(ping_fn):
            logger.info(f"timer_task: ping {name}")
            ping_fn()

    return {"status": "ok", "modules": modules}


# -------- base functions --------
async def startup(ctx: Worker) -> None:
    logging.info("Worker Started")


async def shutdown(ctx: Worker) -> None:
    logging.info("Worker end")
