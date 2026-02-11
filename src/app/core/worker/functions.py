import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any

import uvloop
from arq.worker import Worker
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

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
    try:
        async with local_session() as db:
            result = await discover_schedule(db=db, redis=ctx["redis"], project_module=project_module)
            if "ok" not in result:
                result["ok"] = True
            logger.info(
                "event=discover_schedule_task_result "
                "project_module=%s scheduled_at=%s ok=%s total_sources=%s "
                "total_jobs=%s enqueue_failures=%s skipped_due_to_queue_full=%s skipped_due_to_tap_unreachable=%s tap_unreachable=%s",
                project_module or "all",
                result.get("scheduled_at"),
                result.get("ok"),
                result.get("total_sources"),
                result.get("total_jobs"),
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


async def _run_discover_once(
    discover_callable: Any,
    source_identifier: str,
    tap_timeout: int,
) -> Any:
    return await asyncio.wait_for(
        asyncio.to_thread(discover_callable, source_identifier),
        timeout=tap_timeout,
    )


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(3),
    reraise=True,
)
async def _run_discover_with_retry(
    discover_callable: Any,
    source_identifier: str,
    tap_timeout: int,
) -> Any:
    return await _run_discover_once(
        discover_callable=discover_callable,
        source_identifier=source_identifier,
        tap_timeout=tap_timeout,
    )


async def _run_prepare_once(
    prepare_callable: Any,
    source_identifier: str,
    query_results: Any,
    data_url_by_scan_id: dict[str, str] | None,
    checksum_url_by_scan_id: dict[str, str] | None,
    tap_timeout: int,
) -> Any:
    return await asyncio.wait_for(
        asyncio.to_thread(
            prepare_callable,
            source_identifier=source_identifier,
            query_results=query_results,
            data_url_by_scan_id=data_url_by_scan_id,
            checksum_url_by_scan_id=checksum_url_by_scan_id,
        ),
        timeout=tap_timeout,
    )


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
            logger.warning(
                "event=discover_batch_missing_sbid dataset_id=%s",
                item.get("dataset_id", "unknown"),
            )
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
    failed_source_identifiers: list[str] = []
    total_datasets = 0
    total_sbids = 0

    tap_timeout = getattr(settings, "DISCOVERY_TAP_TIMEOUT_SECONDS", 120)
    job_started_at = time.perf_counter()
    async with local_session() as db:
        logger.info(
            "event=discover_batch_started project_module=%s total_sources=%s",
            project_module,
            len(source_identifiers),
        )
        for source_identifier in source_identifiers:
            source_started_at = time.perf_counter()
            try:
                # Run blocking TAP discover in thread with timeout so worker cannot hang
                discover_fn = getattr(module, "discover", None)
                if discover_fn:
                    query_results = await _run_discover_with_retry(
                        discover_callable=discover_fn,
                        source_identifier=source_identifier,
                        tap_timeout=tap_timeout,
                    )
                else:
                    logger.warning(
                        "event=discover_batch_no_discover_fallback project_module=%s",
                        project_module,
                    )
                    query_results = await _run_discover_with_retry(
                        discover_callable=discovery_test.query_casda_visibility_files,
                        source_identifier=source_identifier,
                        tap_timeout=tap_timeout,
                    )

                if len(query_results) == 0:
                    logger.info(
                        "event=discover_batch_source_outcome "
                        "project_module=%s source_identifier=%s "
                        "outcome=no_datasets duration_ms=%s",
                        project_module,
                        source_identifier,
                        int((time.perf_counter() - source_started_at) * 1000),
                    )
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

                # Run blocking prepare_metadata (Vizier/CASDA TAP) in thread with timeout (no retry)
                prepare_fn = getattr(module, "prepare_metadata", None)
                if prepare_fn:
                    result = await _run_prepare_once(
                        prepare_callable=prepare_fn,
                        source_identifier=source_identifier,
                        query_results=query_results,
                        data_url_by_scan_id=data_url_by_scan_id,
                        checksum_url_by_scan_id=checksum_url_by_scan_id,
                        tap_timeout=tap_timeout,
                    )
                else:
                    logger.warning(
                        "event=discover_batch_no_prepare_fallback project_module=%s",
                        project_module,
                    )
                    result = await _run_prepare_once(
                        prepare_callable=discovery_test.prepare_metadata,
                        source_identifier=source_identifier,
                        query_results=query_results,
                        data_url_by_scan_id=data_url_by_scan_id,
                        checksum_url_by_scan_id=checksum_url_by_scan_id,
                        tap_timeout=tap_timeout,
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
                    "event=discover_batch_source_grouped project_module=%s source_identifier=%s sbids=%s datasets=%s",
                    project_module,
                    source_identifier,
                    len(grouped),
                    len(metadata_list),
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
                            "event=discover_batch_upsert_error "
                            "project_module=%s source_identifier=%s sbid=%s error=%s",
                            project_module,
                            source_identifier,
                            str(sbid),
                            e,
                            exc_info=True,
                        )
                        await db.rollback()
                        raise

                await db.commit()
                logger.info(
                    "event=discover_batch_source_outcome "
                    "project_module=%s source_identifier=%s outcome=has_metadata "
                    "sbids=%s datasets=%s duration_ms=%s",
                    project_module,
                    source_identifier,
                    len(grouped),
                    len(metadata_list),
                    int((time.perf_counter() - source_started_at) * 1000),
                )
                total_sbids += len(grouped)
                total_datasets += len(metadata_list)

                source = await source_registry_service.check_existing_source(
                    db, project_module, source_identifier
                )
                if source and source.get("uuid"):
                    processed_source_ids.append(source["uuid"])
            except TimeoutError:
                logger.error(
                    "event=discover_batch_source_outcome "
                    "project_module=%s source_identifier=%s outcome=timeout "
                    "tap_timeout_seconds=%s duration_ms=%s",
                    project_module,
                    source_identifier,
                    tap_timeout,
                    int((time.perf_counter() - source_started_at) * 1000),
                )
                failed_source_identifiers.append(source_identifier)
                continue
            except Exception as e:
                logger.error(
                    "event=discover_batch_source_outcome "
                    "project_module=%s source_identifier=%s outcome=error "
                    "error=%s duration_ms=%s",
                    project_module,
                    source_identifier,
                    e,
                    int((time.perf_counter() - source_started_at) * 1000),
                )
                try:
                    await db.rollback()
                except Exception as rollback_error:
                    logger.warning(
                        "event=discover_batch_rollback_error error=%s",
                        rollback_error,
                    )
                failed_source_identifiers.append(source_identifier)
                continue

        await source_registry_service.mark_sources_checked(
            db, processed_source_ids, checked_at=now
        )
        logger.info(
            "event=discover_batch_marked_checked project_module=%s processed_count=%s",
            project_module,
            len(processed_source_ids),
        )

    total_duration_ms = int((time.perf_counter() - job_started_at) * 1000)
    logger.info(
        "event=discover_batch_completed "
        "project_module=%s total_sources=%s total_sbids=%s total_datasets=%s "
        "processed_count=%s failed_count=%s duration_ms=%s",
        project_module,
        len(source_identifiers),
        total_sbids,
        total_datasets,
        len(processed_source_ids),
        len(failed_source_identifiers),
        total_duration_ms,
    )

    return {
        "project_module": project_module,
        "total_sources": len(source_identifiers),
        "total_sbids": total_sbids,
        "total_datasets": total_datasets,
        "failed_sources": failed_source_identifiers,
        "failed_count": len(failed_source_identifiers),
    }


async def timer_task(ctx: Worker) -> dict[str, Any]:
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


# -------- base functions --------
async def startup(ctx: Worker) -> None:
    logger.info("event=worker_lifecycle action=started")


async def shutdown(ctx: Worker) -> None:
    logger.info("event=worker_lifecycle action=shutdown")
