import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any

from arq.worker import Worker
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ...archive.adapters import get_adapter
from ...archive.service import archive_metadata_service
from ...config import settings
from ...db.database import local_session
from ...projects import list_project_modules, load_project_module
from ...registry.service import source_registry_service

logger = logging.getLogger(__name__)


async def _run_discover_once(
    discover_callable: Any,
    source_identifier: str,
    tap_timeout: int,
    adapters: dict[str, Any] | None = None,
) -> Any:
    if adapters is None:
        call = asyncio.to_thread(discover_callable, source_identifier)
    else:
        call = asyncio.to_thread(discover_callable, source_identifier, adapters=adapters)
    return await asyncio.wait_for(call, timeout=tap_timeout)


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
    adapters: dict[str, Any] | None = None,
) -> Any:
    return await _run_discover_once(
        discover_callable=discover_callable,
        source_identifier=source_identifier,
        tap_timeout=tap_timeout,
        adapters=adapters,
    )


async def _run_prepare_once(
    prepare_callable: Any,
    source_identifier: str,
    query_results: Any,
    data_url_by_scan_id: dict[str, str] | None,
    checksum_url_by_scan_id: dict[str, str] | None,
    tap_timeout: int,
    adapters: dict[str, Any] | None = None,
) -> Any:
    kwargs = {
        "source_identifier": source_identifier,
        "query_results": query_results,
        "data_url_by_scan_id": data_url_by_scan_id,
        "checksum_url_by_scan_id": checksum_url_by_scan_id,
    }
    if adapters is not None:
        kwargs["adapters"] = adapters
    return await asyncio.wait_for(asyncio.to_thread(prepare_callable, **kwargs), timeout=tap_timeout)


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
        grouped.setdefault(str(sbid), []).append(item)
    return grouped


def _resolve_module_adapters(module: Any) -> dict[str, Any] | None:
    required_adapters = getattr(module, "REQUIRED_ADAPTERS", [])
    if not required_adapters:
        return None
    adapters: dict[str, Any] = {}
    for adapter_name in required_adapters:
        adapter = get_adapter(adapter_name)
        if adapter is None:
            raise ValueError(
                f"Required adapter '{adapter_name}' is not registered for module '{module.__name__}'"
            )
        adapters[adapter_name] = adapter
    return adapters


def _extract_prepare_result(result: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if isinstance(result, tuple):
        metadata_list = result[0]
        discovery_flags = result[1] if len(result) > 1 else {}
    else:
        metadata_list = result
        discovery_flags = {}
    return metadata_list, discovery_flags


async def _process_source(
    module: Any,
    project_module: str,
    source_identifier: str,
    tap_timeout: int,
    adapters: dict[str, Any] | None,
) -> dict[str, Any]:
    source_started_at = time.perf_counter()
    discover_fn = getattr(module, "discover", None)
    if not discover_fn or not callable(discover_fn):
        raise ValueError(
            f"Project module '{project_module}' must implement discover(source_identifier)"
        )

    prepare_fn = getattr(module, "prepare_metadata", None)
    if not prepare_fn or not callable(prepare_fn):
        raise ValueError(
            "Project module "
            f"'{project_module}' must implement prepare_metadata(source_identifier, query_results, ...)"
        )

    query_results = await _run_discover_with_retry(
        discover_callable=discover_fn,
        source_identifier=source_identifier,
        tap_timeout=tap_timeout,
        adapters=adapters,
    )

    if len(query_results) == 0:
        return {
            "source_identifier": source_identifier,
            "outcome": "no_datasets",
            "metadata_list": [],
            "discovery_flags": {},
            "duration_ms": int((time.perf_counter() - source_started_at) * 1000),
        }

    result = await _run_prepare_once(
        prepare_callable=prepare_fn,
        source_identifier=source_identifier,
        query_results=query_results,
        data_url_by_scan_id=None,
        checksum_url_by_scan_id=None,
        tap_timeout=tap_timeout,
        adapters=adapters,
    )
    metadata_list, discovery_flags = _extract_prepare_result(result)
    return {
        "source_identifier": source_identifier,
        "outcome": "has_metadata",
        "metadata_list": metadata_list,
        "discovery_flags": discovery_flags,
        "duration_ms": int((time.perf_counter() - source_started_at) * 1000),
    }


async def discover_batch(
    ctx: Worker, project_module: str, source_identifiers: list[str]
) -> dict[str, Any]:
    _ = ctx
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
    batch_concurrency = max(1, getattr(settings, "DISCOVERY_BATCH_CONCURRENCY", 1))
    module_adapters = _resolve_module_adapters(module)
    job_started_at = time.perf_counter()
    logger.info(
        "event=discover_batch_started project_module=%s total_sources=%s concurrency=%s",
        project_module,
        len(source_identifiers),
        batch_concurrency,
    )

    semaphore = asyncio.Semaphore(batch_concurrency)

    async def _run_with_limit(source_identifier: str) -> dict[str, Any]:
        async with semaphore:
            try:
                return await _process_source(
                    module=module,
                    project_module=project_module,
                    source_identifier=source_identifier,
                    tap_timeout=tap_timeout,
                    adapters=module_adapters,
                )
            except TimeoutError:
                return {
                    "source_identifier": source_identifier,
                    "outcome": "timeout",
                    "error": f"timed out after {tap_timeout}s",
                    "duration_ms": None,
                }
            except Exception as e:
                return {
                    "source_identifier": source_identifier,
                    "outcome": "error",
                    "error": str(e),
                    "duration_ms": None,
                }

    source_results = await asyncio.gather(*[_run_with_limit(sid) for sid in source_identifiers])

    async with local_session() as db:
        for source_result in source_results:
            source_identifier = source_result["source_identifier"]
            outcome = source_result["outcome"]

            if outcome in ("timeout", "error"):
                logger.error(
                    "event=discover_batch_source_outcome "
                    "project_module=%s source_identifier=%s outcome=%s error=%s",
                    project_module,
                    source_identifier,
                    outcome,
                    source_result.get("error"),
                )
                failed_source_identifiers.append(source_identifier)
                continue

            if outcome == "no_datasets":
                logger.info(
                    "event=discover_batch_source_outcome "
                    "project_module=%s source_identifier=%s outcome=no_datasets duration_ms=%s",
                    project_module,
                    source_identifier,
                    source_result.get("duration_ms"),
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

            metadata_list = source_result.get("metadata_list", [])
            discovery_flags = source_result.get("discovery_flags", {})
            grouped = _group_metadata_by_sbid(metadata_list)
            logger.info(
                "event=discover_batch_source_grouped project_module=%s source_identifier=%s sbids=%s datasets=%s",
                project_module,
                source_identifier,
                len(grouped),
                len(metadata_list),
            )

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
                    failed_source_identifiers.append(source_identifier)
                    break
            else:
                await db.commit()
                logger.info(
                    "event=discover_batch_source_outcome "
                    "project_module=%s source_identifier=%s outcome=has_metadata "
                    "sbids=%s datasets=%s duration_ms=%s",
                    project_module,
                    source_identifier,
                    len(grouped),
                    len(metadata_list),
                    source_result.get("duration_ms"),
                )
                total_sbids += len(grouped)
                total_datasets += len(metadata_list)

                source = await source_registry_service.check_existing_source(
                    db, project_module, source_identifier
                )
                if source and source.get("uuid"):
                    processed_source_ids.append(source["uuid"])

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
