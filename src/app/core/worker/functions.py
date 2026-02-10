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
    available_modules = list_project_modules()
    if project_module not in available_modules:
        raise ValueError(
            f"Project module '{project_module}' not found. Available: {available_modules}"
        )

    module = load_project_module(project_module)
    now = datetime.now(UTC)

    processed_source_ids: list[Any] = []
    total_datasets = 0
    total_sbids = 0

    tap_timeout = getattr(settings, "DISCOVERY_TAP_TIMEOUT_SECONDS", 120)

    async with local_session() as db:
        logger.info(
            f"discover_batch: project_module={project_module} sources={len(source_identifiers)}"
        )
        for source_identifier in source_identifiers:
            try:
                # Run blocking TAP discover in thread with timeout so worker cannot hang
                discover_fn = getattr(module, "discover", None)
                if discover_fn:
                    query_results = await asyncio.wait_for(
                        asyncio.to_thread(discover_fn, source_identifier),
                        timeout=tap_timeout,
                    )
                else:
                    logger.warning(f"Module {project_module} has no discover() function, using fallback")
                    query_results = await asyncio.wait_for(
                        asyncio.to_thread(
                            discovery_test.query_casda_visibility_files,
                            source_identifier,
                        ),
                        timeout=tap_timeout,
                    )

                if len(query_results) == 0:
                    logger.info(f"discover_batch: outcome=no_datasets source={source_identifier}")
                    await archive_metadata_service.upsert_metadata(
                        db=db,
                        project_module=project_module,
                        source_identifier=source_identifier,
                        sbid="0",
                        metadata_json={"datasets": [], "discovery_status": "no_datasets"},
                    )
                    await db.commit()
                    source = await source_registry_service.check_existing_source(
                        db, project_module, source_identifier
                    )
                    if source and source.get("uuid"):
                        processed_source_ids.append(source["uuid"])
                    continue

                data_url_by_scan_id: dict[str, str] | None = None
                checksum_url_by_scan_id: dict[str, str] | None = None

                # Run blocking prepare_metadata (Vizier/CASDA TAP) in thread with timeout
                prepare_fn = getattr(module, "prepare_metadata", None)
                if prepare_fn:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(
                            prepare_fn,
                            source_identifier=source_identifier,
                            query_results=query_results,
                            data_url_by_scan_id=data_url_by_scan_id,
                            checksum_url_by_scan_id=checksum_url_by_scan_id,
                        ),
                        timeout=tap_timeout,
                    )
                else:
                    logger.warning(f"Module {project_module} has no prepare_metadata() function, using fallback")
                    result = await asyncio.wait_for(
                        asyncio.to_thread(
                            discovery_test.prepare_metadata,
                            source_identifier=source_identifier,
                            query_results=query_results,
                            data_url_by_scan_id=data_url_by_scan_id,
                            checksum_url_by_scan_id=checksum_url_by_scan_id,
                        ),
                        timeout=tap_timeout,
                    )

                if isinstance(result, tuple):
                    metadata_list = result[0]
                    discovery_flags = result[1] if len(result) > 1 else {}
                else:
                    metadata_list = result
                    discovery_flags = {}
                if not discovery_flags and metadata_list and isinstance(metadata_list[0], dict):
                    discovery_flags = {"ra_dec_vsys_complete": "ra_degrees" in metadata_list[0]}

                grouped = _group_metadata_by_sbid(metadata_list)
                logger.info(
                    f"discover_batch: {project_module}/{source_identifier} "
                    f"sbids={len(grouped)} datasets={len(metadata_list)}"
                )

                # Upsert
                for sbid, datasets in grouped.items():
                    metadata_json: dict[str, Any] = {"datasets": datasets}
                    if discovery_flags:
                        metadata_json["discovery_flags"] = discovery_flags
                    try:
                        await archive_metadata_service.upsert_metadata(
                            db=db,
                            project_module=project_module,
                            source_identifier=source_identifier,
                            sbid=str(sbid),
                            metadata_json=metadata_json,
                        )
                    except Exception as e:
                        logger.error(
                            f"Error upserting metadata for {source_identifier} SBID {sbid}: {e}",
                            exc_info=True,
                        )
                        await db.rollback()
                        raise
                
                await db.commit()
                logger.info(
                    f"discover_batch: outcome=has_metadata source={source_identifier} "
                    f"sbids={len(grouped)} datasets={len(metadata_list)}"
                )
                total_sbids += len(grouped)
                total_datasets += len(metadata_list)

                source = await source_registry_service.check_existing_source(
                    db, project_module, source_identifier
                )
                if source and source.get("uuid"):
                    processed_source_ids.append(source["uuid"])
# going to look into https://github.com/jd/tenacity for retry
            except asyncio.TimeoutError:
                logger.error(
                    f"discover_batch: outcome=timeout source={source_identifier} "
                    f"TAP timeout ({tap_timeout}s); will retry next run"
                )
                continue
            except Exception as e:
                logger.error(
                    f"discover_batch: outcome=error source={source_identifier} error={e}",
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
