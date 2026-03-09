import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from arq.worker import Worker

from ...config import settings
from ...db.database import local_session
from ...projects import list_project_modules, load_project_module
from ...registry.service import invalid_project_module_message, source_registry_service
from ...utils.discovery import (
    discovery_signature,
    group_metadata_by_sbid,
    metadata_payload_by_sbid,
)
from .discovery_batch import (
    DiscoveryBatchStats,
    build_discovery_result,
    finalize_source_marks,
    persist_source_result,
    record_failed_result,
    resolve_module_adapters,
)
from .discovery_outcomes import (
    handle_changed_metadata as _handle_changed_metadata,
    handle_no_datasets as _handle_no_datasets,
    handle_unchanged_metadata as _handle_unchanged_metadata,
    log_missing_source as _log_missing_source,
    resolve_existing_signature as _resolve_existing_signature,
)
from .discovery_process import process_source

logger = logging.getLogger(__name__)


async def discover_batch(
    ctx: Worker,
    project_module: str,
    source_identifiers: list[str],
    claim_token: str | None = None,
) -> dict[str, Any]:
    _ = ctx
    processed_count = 0
    claim_released = False
    job_started_at = time.perf_counter()
    try:
        available_modules = list_project_modules()
        if project_module not in available_modules:
            raise ValueError(invalid_project_module_message(project_module, available_modules))

        module = load_project_module(project_module)
        now = datetime.now(UTC)

        stats = DiscoveryBatchStats()

        tap_timeout = getattr(settings, "DISCOVERY_TAP_TIMEOUT_SECONDS", 120)
        batch_concurrency = max(1, getattr(settings, "DISCOVERY_BATCH_CONCURRENCY", 1))
        module_adapters = resolve_module_adapters(module)
        logger.info(
            "event=discover_batch_started project_module=%s total_sources=%s concurrency=%s claim_token=%s",
            project_module,
            len(source_identifiers),
            batch_concurrency,
            claim_token,
        )

        # cap concurrent discover+prepare per batch
        semaphore = asyncio.Semaphore(batch_concurrency)

        async def _run_with_limit(source_identifier: str) -> dict[str, Any]:
            async with semaphore:
                try:
                    return await process_source(
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
                    record_failed_result(
                        stats=stats,
                        project_module=project_module,
                        source_identifier=source_identifier,
                        outcome=outcome,
                        error=source_result.get("error"),
                        duration_ms=duration_ms,
                    )
                    continue

                # no_datasets path: persist sentinel and update registry
                if outcome == "no_datasets":
                    source = await source_registry_service.check_existing_source(
                        db, project_module, source_identifier
                    )
                    persisted_result = await persist_source_result(
                        db=db,
                        stats=stats,
                        project_module=project_module,
                        source_identifier=source_identifier,
                        duration_ms=duration_ms,
                        persist=lambda: _handle_no_datasets(
                            db=db,
                            project_module=project_module,
                            source_identifier=source_identifier,
                            source=source,
                            claim_token=claim_token,
                            duration_ms=duration_ms,
                            now=now,
                        ),
                        should_commit=lambda result: bool(result and result[0]),
                    )
                    if persisted_result is None:
                        continue

                    changed, maybe_unchanged_id = persisted_result
                    stats.no_datasets_count += 1
                    if changed:
                        stats.changed_count += 1
                    elif maybe_unchanged_id is not None:
                        stats.unchanged_count += 1
                        stats.processed_unchanged_identifiers.append(maybe_unchanged_id)
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

                payload_by_sbid = metadata_payload_by_sbid(grouped, discovery_flags)
                new_sig = discovery_signature(payload_by_sbid)
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
                        grouped=grouped,
                        metadata_list=metadata_list,
                        duration_ms=duration_ms,
                        outcome_label="has_metadata",
                    )
                    stats.unchanged_count += 1
                    stats.processed_unchanged_identifiers.append(unchanged_id)
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
                changed = await persist_source_result(
                    db=db,
                    stats=stats,
                    project_module=project_module,
                    source_identifier=source_identifier,
                    duration_ms=duration_ms,
                    persist=lambda: _handle_changed_metadata(
                        db=db,
                        project_module=project_module,
                        source_identifier=source_identifier,
                        source=source,
                        grouped=grouped,
                        discovery_flags=discovery_flags,
                        new_sig=new_sig,
                        claim_token=claim_token,
                        duration_ms=duration_ms,
                        now=now,
                    ),
                    should_commit=bool,
                )
                if changed is None:
                    continue

                if changed:
                    stats.changed_count += 1
                    stats.total_sbids += len(grouped)
                    stats.total_datasets += len(metadata_list)

            # Release the discovery lease for this exact claimed batch before final commit
            released_count = 0
            try:
                processed_count = await finalize_source_marks(
                    db=db,
                    stats=stats,
                    project_module=project_module,
                    now=now,
                    claim_token=claim_token,
                )
                released_count = await source_registry_service.release_discovery_claim(
                    db=db,
                    project_module=project_module,
                    source_identifiers=source_identifiers,
                    claim_token=claim_token,
                    commit=False,
                )
            except Exception as exc:
                logger.warning(
                    "event=discover_batch_release_claim_error project_module=%s count=%s claim_token=%s error=%s",
                    project_module,
                    len(source_identifiers),
                    claim_token,
                    exc,
                    exc_info=True,
                )
                await db.rollback()
                raise
            if claim_token and released_count != len(source_identifiers):
                logger.warning(
                    "event=discover_batch_release_claim_partial project_module=%s expected=%s released=%s claim_token=%s",
                    project_module,
                    len(source_identifiers),
                    released_count,
                    claim_token,
                )
            await db.commit()
            claim_released = claim_token is None or released_count == len(source_identifiers)

        total_duration_ms = int((time.perf_counter() - job_started_at) * 1000)
        failed_count = len(stats.failed_source_identifiers)
        if failed_count > 0 and failed_count == len(source_identifiers):
            logger.warning(
                "event=discover_batch_fully_failed project_module=%s total_sources=%s "
                "error_count=%s timeout_count=%s duration_ms=%s",
                project_module,
                len(source_identifiers),
                stats.error_count,
                stats.timeout_count,
                total_duration_ms,
            )
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

        result = build_discovery_result(
            project_module=project_module,
            source_identifiers=source_identifiers,
            stats=stats,
        )

        # Full result JSON at DEBUG; discover_batch_completed above is the INFO summary
        logger.debug(
            "event=discover_batch_result project_module=%s result_json=%s",
            project_module,
            json.dumps(result, sort_keys=True, separators=(",", ":")),
        )

        return result
    finally:
        if claim_token and not claim_released:
            try:
                async with local_session() as cleanup_db:
                    await source_registry_service.release_discovery_claim(
                        db=cleanup_db,
                        project_module=project_module,
                        source_identifiers=source_identifiers,
                        claim_token=claim_token,
                        commit=True,
                    )
            except Exception as exc:
                logger.warning(
                    "event=discover_batch_release_claim_fallback_error project_module=%s count=%s claim_token=%s error=%s",
                    project_module,
                    len(source_identifiers),
                    claim_token,
                    exc,
                    exc_info=True,
                )
