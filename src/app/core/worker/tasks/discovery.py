import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from arq.worker import Worker

from ...archive.adapters import get_adapter
from ...config import settings
from ...db.database import local_session
from ...projects import list_project_modules, load_project_module
from ...projects.contracts import extract_discover_bundle
from ...registry.service import source_registry_service
from ...utils.discovery import (
    discovery_signature,
    group_metadata_by_sbid,
    validate_prepared_metadata_records,
)
from .discovery_execution import (
    extract_prepare_result as _extract_prepare_result,
    run_discover_with_retry as _run_discover_with_retry,
    run_prepare_once as _run_prepare_once,
)
from .discovery_outcomes import (
    handle_changed_metadata as _handle_changed_metadata,
    handle_no_datasets as _handle_no_datasets,
    handle_unchanged_metadata as _handle_unchanged_metadata,
    log_missing_source as _log_missing_source,
    resolve_existing_signature as _resolve_existing_signature,
)

logger = logging.getLogger(__name__)

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


@dataclass
class DiscoveryBatchStats:
    # batch-level counters
    processed_unchanged_ids: list[Any] = field(default_factory=list)
    changed_count: int = 0
    unchanged_count: int = 0
    no_datasets_count: int = 0
    timeout_count: int = 0
    error_count: int = 0
    missing_registry_count: int = 0
    failed_source_identifiers: list[str] = field(default_factory=list)
    total_datasets: int = 0
    total_sbids: int = 0


def _record_failed_result(
    stats: DiscoveryBatchStats,
    project_module: str,
    source_identifier: str,
    outcome: str,
    error: Any,
    duration_ms: Any,
) -> None:
    # normalize timeout/error counting and failed-source logging
    if outcome == "timeout":
        stats.timeout_count += 1
    else:
        stats.error_count += 1
    stats.failed_source_identifiers.append(source_identifier)
    logger.error(
        "event=discover_batch_source_outcome "
        "project_module=%s source_identifier=%s outcome=%s error=%s duration_ms=%s",
        project_module,
        source_identifier,
        outcome,
        error,
        duration_ms,
    )


async def _finalize_source_marks(
    db: Any,
    stats: DiscoveryBatchStats,
    project_module: str,
    now: datetime,
) -> int:
    # mark unchanged as checked and failed as attempted in one final commit
    logger.debug(
        "event=discover_batch_mark_checked_start project_module=%s processed_count=%s failed_count=%s",
        project_module,
        len(stats.processed_unchanged_ids),
        len(stats.failed_source_identifiers),
    )
    await source_registry_service.mark_sources_checked(
        db, stats.processed_unchanged_ids, checked_at=now, commit=False
    )
    await source_registry_service.mark_sources_attempted(
        db,
        project_module,
        list(set(stats.failed_source_identifiers)),
        attempted_at=now,
        commit=False,
    )
    await db.commit()
    processed_count = stats.changed_count + stats.unchanged_count
    logger.debug(
        "event=discover_batch_mark_checked_done project_module=%s processed_count=%s",
        project_module,
        len(stats.processed_unchanged_ids),
    )
    logger.info(
        "event=discover_batch_marked_checked project_module=%s processed_count=%s missing_registry_count=%s",
        project_module,
        processed_count,
        stats.missing_registry_count,
    )
    return processed_count


def _build_discovery_result(
    project_module: str,
    source_identifiers: list[str],
    stats: DiscoveryBatchStats,
) -> dict[str, Any]:
    # keep result payload shape stable for callers/ops logs
    return {
        "project_module": project_module,
        "total_sources": len(source_identifiers),
        "total_sbids": stats.total_sbids,
        "total_datasets": stats.total_datasets,
        "changed_count": stats.changed_count,
        "unchanged_count": stats.unchanged_count,
        "no_datasets_count": stats.no_datasets_count,
        "error_count": stats.error_count,
        "timeout_count": stats.timeout_count,
        "failed_sources": stats.failed_source_identifiers,
        "failed_count": len(stats.failed_source_identifiers),
        "missing_registry_count": stats.missing_registry_count,
    }

# discover (with retry) then prepare for one source; returns outcome dict
async def _process_source(
    module: Any,
    project_module: str,
    source_identifier: str,
    tap_timeout: int,
    adapters: dict[str, Any] | None,
) -> dict[str, Any]:
    source_started_at = time.perf_counter()
    discover_fn = getattr(module, "discover")
    prepare_fn = getattr(module, "prepare_metadata")

    logger.debug(
        "event=discover_batch_source_discover_start project_module=%s source_identifier=%s tap_timeout=%s",
        project_module,
        source_identifier,
        tap_timeout,
    )
    discover_output_raw = await _run_discover_with_retry(
        discover_callable=discover_fn,
        source_identifier=source_identifier,
        tap_timeout=tap_timeout,
        adapters=adapters,
    )
    discover_output = extract_discover_bundle(discover_output_raw, project_module)
    query_results = discover_output["query_results"]
    if not hasattr(query_results, "__len__"):
        raise ValueError(
            f"module '{project_module}' discover() must return bundle['query_results'] "
            "as a length-checkable collection"
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
        query_results=discover_output,
        data_url_by_scan_id=None,
        checksum_url_by_scan_id=None,
        tap_timeout=tap_timeout,
        adapters=adapters,
    )
    metadata_list, discovery_flags = _extract_prepare_result(result)
    metadata_list = validate_prepared_metadata_records(
        metadata_list,
        project_module=project_module,
        source_identifier=source_identifier,
    )
    duration_ms = int((time.perf_counter() - source_started_at) * 1000)
    logger.debug(
        "event=discover_batch_source_prepare_complete project_module=%s source_identifier=%s "
        "duration_ms=%s metadata_count=%s",
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

    stats = DiscoveryBatchStats()

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

            if outcome in {"timeout", "error"}:
                _record_failed_result(
                    stats=stats,
                    project_module=project_module,
                    source_identifier=source_identifier,
                    outcome=outcome,
                    error=source_result.get("error"),
                    duration_ms=duration_ms,
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
                    _record_failed_result(
                        stats=stats,
                        project_module=project_module,
                        source_identifier=source_identifier,
                        outcome="error",
                        error=e,
                        duration_ms=duration_ms,
                    )
                    continue
                stats.no_datasets_count += 1
                if changed:
                    stats.changed_count += 1
                elif maybe_unchanged_id is not None:
                    stats.unchanged_count += 1
                    stats.processed_unchanged_ids.append(maybe_unchanged_id)
                else:
                    stats.missing_registry_count += 1
                continue

            # has_metadata path: group by sbid, compare signature, upsert if changed
            metadata_list = source_result.get("metadata_list", [])
            discovery_flags = source_result.get("discovery_flags", {})
            grouped = group_metadata_by_sbid(metadata_list)
            logger.debug(
                "event=discover_batch_source_grouped project_module=%s source_identifier=%s "
                "sbids=%s sbid_list=%s datasets=%s",
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
                stats.missing_registry_count += 1
                continue

            new_sig = discovery_signature(grouped)
            existing_sig = await _resolve_existing_signature(
                db=db,
                source=source,
                project_module=project_module,
                source_identifier=source_identifier,
            )

            if new_sig == existing_sig:
                logger.debug(
                    "event=discover_batch_signature_unchanged project_module=%s source_identifier=%s "
                    "existing_sig=%s new_sig=%s",
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
                stats.unchanged_count += 1
                stats.processed_unchanged_ids.append(unchanged_id)
                stats.total_sbids += len(grouped)
                stats.total_datasets += len(metadata_list)
                continue

            logger.debug(
                "event=discover_batch_signature_changed project_module=%s source_identifier=%s "
                "existing_sig=%s new_sig=%s",
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
                _record_failed_result(
                    stats=stats,
                    project_module=project_module,
                    source_identifier=source_identifier,
                    outcome="error",
                    error=e,
                    duration_ms=duration_ms,
                )
                continue

            if changed:
                stats.changed_count += 1
                stats.total_sbids += len(grouped)
                stats.total_datasets += len(metadata_list)

        try:
            processed_count = await _finalize_source_marks(
                db=db,
                stats=stats,
                project_module=project_module,
                now=now,
            )
        except Exception:
            await db.rollback()
            raise

    total_duration_ms = int((time.perf_counter() - job_started_at) * 1000)
    logger.info(
        "event=discover_batch_completed "
        "project_module=%s total_sources=%s total_sbids=%s total_datasets=%s "
        "processed_count=%s changed_count=%s unchanged_count=%s no_datasets_count=%s "
        "error_count=%s timeout_count=%s failed_count=%s missing_registry_count=%s duration_ms=%s",
        project_module,
        len(source_identifiers),
        stats.total_sbids,
        stats.total_datasets,
        processed_count,
        stats.changed_count,
        stats.unchanged_count,
        stats.no_datasets_count,
        stats.error_count,
        stats.timeout_count,
        len(stats.failed_source_identifiers),
        stats.missing_registry_count,
        total_duration_ms,
    )

    result = _build_discovery_result(
        project_module=project_module,
        source_identifiers=source_identifiers,
        stats=stats,
    )

    # ARQ truncates its own "job result" log line; emit the full result here.
    logger.info(
        "event=discover_batch_result project_module=%s result_json=%s",
        project_module,
        json.dumps(result, sort_keys=True, separators=(",", ":")),
    )

    return result
