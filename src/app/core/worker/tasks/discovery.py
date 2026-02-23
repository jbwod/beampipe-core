import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from arq.worker import Worker
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ...archive.adapters import get_adapter
from ...archive.service import archive_metadata_service
from ...config import settings
from ...db.database import local_session
from ...projects import list_project_modules, load_project_module
from ...registry.service import source_registry_service
from ...utils.discovery import (
    NO_DATASETS_SIGNATURE,
    discovery_signature,
    existing_signature_from_records,
    group_metadata_by_sbid,
)

logger = logging.getLogger(__name__)

# transient errors we retry on (network/timeouts only)
DISCOVERY_EXCEPTIONS = (TimeoutError, ConnectionError)


# log each retry attempt before sleeping (debug level)
def _log_discover_retry(retry_state: RetryCallState) -> None:
    """Log when discovery retries after a transient failure."""
    if retry_state.outcome is None or not retry_state.outcome.failed:
        return
    exc = retry_state.outcome.exception()
    source_identifier = retry_state.kwargs.get("source_identifier", "?")
    logger.debug(
        "event=discover_retry_attempt attempt=%s source_identifier=%s error=%s",
        retry_state.attempt_number,
        source_identifier,
        exc,
    )


# single attempt: run discover in thread, enforce tap timeout
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


# --- tenacity retry for discovery ---
# We only retry on TimeoutError and ConnectionError (transient). Other exceptions
# (e.g. ValueError, auth errors) are raised immediately. Exponential backoff:
# 2s, 4s, ... up to 60s between attempts; max 3 attempts then reraise. before_sleep
# logs each retry at debug so we can see which source and attempt failed.
@retry(
    retry=retry_if_exception_type(DISCOVERY_EXCEPTIONS),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(3),
    reraise=True,
    before_sleep=_log_discover_retry,
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


# one-shot prepare in thread with timeout (no retry)
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




# build adapter dict from module.REQUIRED_ADAPTERS or None if none
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


# normalise prepare_metadata return to (metadata_list, discovery_flags)
def _extract_prepare_result(result: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if isinstance(result, tuple):
        metadata_list = result[0]
        discovery_flags = result[1] if len(result) > 1 else {}
    else:
        metadata_list = result
        discovery_flags = {}
    return metadata_list, discovery_flags


def _log_missing_source(project_module: str, source_identifier: str, outcome: str) -> None:
    logger.warning(
        "event=discover_batch_source_missing_registry project_module=%s source_identifier=%s outcome=%s action=skip",
        project_module,
        source_identifier,
        outcome,
    )


# log unchanged outcome, no db writes; return source_uuid for mark_sources_checked
def _handle_unchanged_metadata(
    project_module: str,
    source_identifier: str,
    source_uuid: Any,
    grouped: dict[str, list[dict[str, Any]]],
    metadata_list: list[dict[str, Any]],
    duration_ms: Any,
    *,
    outcome_label: str,
) -> Any:
    logger.info(
        "event=discover_batch_source_outcome "
        "project_module=%s source_identifier=%s outcome=%s changed=%s "
        "sbids=%s datasets=%s duration_ms=%s",
        project_module,
        source_identifier,
        outcome_label,
        False,
        len(grouped),
        len(metadata_list),
        duration_ms,
    )
    return source_uuid


# upsert no_datasets and update discovery state when sig changes from previous data
async def _handle_no_datasets(
    db: Any,
    project_module: str,
    source_identifier: str,
    source: dict[str, Any] | None,
    duration_ms: Any,
    now: datetime,
) -> tuple[bool, Any | None]:
    if not source or not source.get("uuid"):
        _log_missing_source(project_module, source_identifier, "no_datasets")
        return False, None

    source_uuid = source["uuid"]
    stored_sig = source.get("discovery_signature")
    if stored_sig is None:
        existing_records = await archive_metadata_service.list_metadata_for_source(
            db=db,
            project_module=project_module,
            source_identifier=source_identifier,
        )
        stored_sig = existing_signature_from_records(existing_records)

    if stored_sig == NO_DATASETS_SIGNATURE:
        logger.info(
            "event=discover_batch_source_outcome "
            "project_module=%s source_identifier=%s outcome=no_datasets changed=%s duration_ms=%s",
            project_module,
            source_identifier,
            False,
            duration_ms,
        )
        return False, source_uuid

    logger.debug(
        "event=discover_batch_signature_changed project_module=%s source_identifier=%s "
        "existing_sig=%s new_sig=%s outcome=no_datasets",
        project_module,
        source_identifier,
        stored_sig,
        NO_DATASETS_SIGNATURE,
    )
    await archive_metadata_service.upsert_metadata(
        db=db,
        project_module=project_module,
        source_identifier=source_identifier,
        sbid="0",
        metadata_json={"datasets": [], "discovery_status": "no_datasets"},
    )
    await source_registry_service.update_source_discovery_state(
        db=db,
        source_id=source_uuid,
        checked_at=now,
        discovery_signature=NO_DATASETS_SIGNATURE,
    )
    logger.info(
        "event=discover_batch_source_outcome "
        "project_module=%s source_identifier=%s outcome=no_datasets changed=%s duration_ms=%s",
        project_module,
        source_identifier,
        True,
        duration_ms,
    )
    return True, None


# upsert metadata per sbid and update source discovery state/signature
async def _handle_changed_metadata(
    db: Any,
    project_module: str,
    source_identifier: str,
    source: dict[str, Any] | None,
    grouped: dict[str, list[dict[str, Any]]],
    discovery_flags: dict[str, Any],
    new_sig: str,
    duration_ms: Any,
    now: datetime,
) -> bool:
    if not source or not source.get("uuid"):
        _log_missing_source(project_module, source_identifier, "has_metadata")
        return False

    source_uuid = source["uuid"]
    for sbid, datasets in grouped.items():
        metadata_json: dict[str, Any] = {"datasets": datasets}
        if discovery_flags:
            metadata_json["discovery_flags"] = discovery_flags
        await archive_metadata_service.upsert_metadata(
            db=db,
            project_module=project_module,
            source_identifier=source_identifier,
            sbid=str(sbid),
            metadata_json=metadata_json,
        )

    await source_registry_service.update_source_discovery_state(
        db=db,
        source_id=source_uuid,
        checked_at=now,
        discovery_signature=new_sig,
    )
    logger.info(
        "event=discover_batch_source_outcome "
        "project_module=%s source_identifier=%s outcome=has_metadata changed=%s "
        "sbids=%s datasets=%s duration_ms=%s",
        project_module,
        source_identifier,
        True,
        len(grouped),
        sum(len(datasets) for datasets in grouped.values()),
        duration_ms,
    )
    return True


# discover (with retry) then prepare for one source; returns outcome dict
async def _process_source(
    module: Any,
    project_module: str,
    source_identifier: str,
    tap_timeout: int,
    adapters: dict[str, Any] | None,
) -> dict[str, Any]:
    source_started_at = time.perf_counter()
    # require discover and prepare_metadata callables on module
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

    logger.debug(
        "event=discover_batch_source_discover_start project_module=%s source_identifier=%s tap_timeout=%s",
        project_module,
        source_identifier,
        tap_timeout,
    )
    query_results = await _run_discover_with_retry(
        discover_callable=discover_fn,
        source_identifier=source_identifier,
        tap_timeout=tap_timeout,
        adapters=adapters,
    )

    # empty discover result therefore no_datasets path (no prepare call)
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
    duration_ms = int((time.perf_counter() - source_started_at) * 1000)
    logger.debug(
        "event=discover_batch_source_prepare_complete project_module=%s source_identifier=%s duration_ms=%s metadata_count=%s",
        project_module,
        source_identifier,
        duration_ms,
        len(metadata_list),
    )
    return {
        "source_identifier": source_identifier,
        "outcome": "has_metadata",
        "metadata_list": metadata_list,
        "discovery_flags": discovery_flags,
        "duration_ms": int((time.perf_counter() - source_started_at) * 1000),
    }


# arq task: run discovery for many sources, concurrency-limited, then persist outcomes
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

    processed_unchanged_ids: list[Any] = []  # only these need mark_sources_checked at end
    changed_count = 0
    unchanged_count = 0
    no_datasets_count = 0
    timeout_count = 0
    error_count = 0
    missing_registry_count = 0
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

    # cap concurrent discover+prepare per batch
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

    # run all sources with concurrency limit, then process results in one db session
    source_results = await asyncio.gather(*[_run_with_limit(sid) for sid in source_identifiers])

    async with local_session() as db:
        for source_result in source_results:
            source_identifier = source_result["source_identifier"]
            outcome = source_result["outcome"]
            duration_ms = source_result.get("duration_ms")

            if outcome == "timeout":
                timeout_count += 1
                failed_source_identifiers.append(source_identifier)
                logger.error(
                    "event=discover_batch_source_outcome "
                    "project_module=%s source_identifier=%s outcome=%s error=%s duration_ms=%s",
                    project_module,
                    source_identifier,
                    outcome,
                    source_result.get("error"),
                    duration_ms,
                )
                continue
            if outcome == "error":
                error_count += 1
                failed_source_identifiers.append(source_identifier)
                logger.error(
                    "event=discover_batch_source_outcome "
                    "project_module=%s source_identifier=%s outcome=%s error=%s duration_ms=%s",
                    project_module,
                    source_identifier,
                    outcome,
                    source_result.get("error"),
                    duration_ms,
                )
                continue

            if outcome == "no_datasets":
                source = await source_registry_service.check_existing_source(
                    db, project_module, source_identifier
                )
                try:
                    changed, maybe_unchanged_id = await _handle_no_datasets(
                        db=db,
                        project_module=project_module,
                        source_identifier=source_identifier,
                        source=source,
                        duration_ms=duration_ms,
                        now=now,
                    )
                    if changed:
                        await db.commit()
                except Exception as e:
                    logger.error(
                        "event=discover_batch_upsert_error "
                        "project_module=%s source_identifier=%s error=%s",
                        project_module,
                        source_identifier,
                        e,
                        exc_info=True,
                    )
                    await db.rollback()
                    error_count += 1
                    failed_source_identifiers.append(source_identifier)
                    continue
                no_datasets_count += 1
                if changed:
                    changed_count += 1
                elif maybe_unchanged_id is not None:
                    unchanged_count += 1
                    processed_unchanged_ids.append(maybe_unchanged_id)
                else:
                    missing_registry_count += 1
                continue

            # has_metadata path: group by sbid, compare signature, upsert if changed
            metadata_list = source_result.get("metadata_list", [])
            discovery_flags = source_result.get("discovery_flags", {})
            grouped = group_metadata_by_sbid(metadata_list)
            logger.debug(
                "event=discover_batch_source_grouped project_module=%s source_identifier=%s sbids=%s sbid_list=%s datasets=%s",
                project_module,
                source_identifier,
                len(grouped),
                list(grouped.keys()),
                len(metadata_list),
            )

            source = await source_registry_service.check_existing_source(
                db, project_module, source_identifier
            )
            if not source or not source.get("uuid"):
                _log_missing_source(project_module, source_identifier, "has_metadata")
                missing_registry_count += 1
                continue

            new_sig = discovery_signature(grouped)
            existing_sig = source.get("discovery_signature")
            if existing_sig is None:
                existing_records = await archive_metadata_service.list_metadata_for_source(
                    db=db,
                    project_module=project_module,
                    source_identifier=source_identifier,
                )
                existing_sig = existing_signature_from_records(existing_records)
                logger.debug(
                    "event=discover_batch_signature_from_records project_module=%s source_identifier=%s "
                    "record_count=%s computed_sig=%s",
                    project_module,
                    source_identifier,
                    len(existing_records),
                    existing_sig,
                )

            if new_sig == existing_sig:
                logger.debug(
                    "event=discover_batch_signature_unchanged project_module=%s source_identifier=%s existing_sig=%s new_sig=%s",
                    project_module,
                    source_identifier,
                    existing_sig,
                    new_sig,
                )
                unchanged_id = _handle_unchanged_metadata(
                    project_module=project_module,
                    source_identifier=source_identifier,
                    source_uuid=source["uuid"],
                    grouped=grouped,
                    metadata_list=metadata_list,
                    duration_ms=duration_ms,
                    outcome_label="has_metadata",
                )
                unchanged_count += 1
                processed_unchanged_ids.append(unchanged_id)
                total_sbids += len(grouped)
                total_datasets += len(metadata_list)
                continue

            logger.debug(
                "event=discover_batch_signature_changed project_module=%s source_identifier=%s existing_sig=%s new_sig=%s",
                project_module,
                source_identifier,
                existing_sig,
                new_sig,
            )
            try:
                changed = await _handle_changed_metadata(
                    db=db,
                    project_module=project_module,
                    source_identifier=source_identifier,
                    source=source,
                    grouped=grouped,
                    discovery_flags=discovery_flags,
                    new_sig=new_sig,
                    duration_ms=duration_ms,
                    now=now,
                )
                if changed:
                    await db.commit()
            except Exception as e:
                logger.error(
                    "event=discover_batch_upsert_error "
                    "project_module=%s source_identifier=%s error=%s",
                    project_module,
                    source_identifier,
                    e,
                    exc_info=True,
                )
                await db.rollback()
                error_count += 1
                failed_source_identifiers.append(source_identifier)
                continue

            if changed:
                changed_count += 1
                total_sbids += len(grouped)
                total_datasets += len(metadata_list)

        # mark unchanged sources checked and failed sources attempted, then commit
        try:
            logger.debug(
                "event=discover_batch_mark_checked_start project_module=%s processed_count=%s failed_count=%s",
                project_module,
                len(processed_unchanged_ids),
                len(failed_source_identifiers),
            )
            await source_registry_service.mark_sources_checked(
                db, processed_unchanged_ids, checked_at=now, commit=False
            )
            await source_registry_service.mark_sources_attempted(
                db,
                project_module,
                list(set(failed_source_identifiers)),
                attempted_at=now,
                commit=False,
            )
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        processed_count = changed_count + unchanged_count
        logger.debug(
            "event=discover_batch_mark_checked_done project_module=%s processed_count=%s",
            project_module,
            len(processed_unchanged_ids),
        )
        logger.info(
            "event=discover_batch_marked_checked project_module=%s processed_count=%s missing_registry_count=%s",
            project_module,
            processed_count,
            missing_registry_count,
        )

    total_duration_ms = int((time.perf_counter() - job_started_at) * 1000)
    logger.info(
        "event=discover_batch_completed "
        "project_module=%s total_sources=%s total_sbids=%s total_datasets=%s "
        "processed_count=%s changed_count=%s unchanged_count=%s no_datasets_count=%s "
        "error_count=%s timeout_count=%s failed_count=%s missing_registry_count=%s duration_ms=%s",
        project_module,
        len(source_identifiers),
        total_sbids,
        total_datasets,
        processed_count,
        changed_count,
        unchanged_count,
        no_datasets_count,
        error_count,
        timeout_count,
        len(failed_source_identifiers),
        missing_registry_count,
        total_duration_ms,
    )

    result = {
        "project_module": project_module,
        "total_sources": len(source_identifiers),
        "total_sbids": total_sbids,
        "total_datasets": total_datasets,
        "changed_count": changed_count,
        "unchanged_count": unchanged_count,
        "no_datasets_count": no_datasets_count,
        "error_count": error_count,
        "timeout_count": timeout_count,
        "failed_sources": failed_source_identifiers,
        "failed_count": len(failed_source_identifiers),
        "missing_registry_count": missing_registry_count,
    }

    #emit the full result here for brevity.
    logger.info(
        "event=discover_batch_result project_module=%s result_json=%s",
        project_module,
        json.dumps(result, sort_keys=True, separators=(",", ":")),
    )

    return result
