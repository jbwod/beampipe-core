"""Batch-level helpers for the discovery worker task: stats, marking, persistence, result shape."""

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ...archive.adapters import get_adapter
from ...exceptions.workflow_exceptions import WorkflowErrorCode, WorkflowFailure
from ...registry.service import source_registry_service

logger = logging.getLogger(__name__)


def resolve_module_adapters(module: Any) -> dict[str, Any] | None:
    """Build adapter dict from module.REQUIRED_ADAPTERS or None if none."""
    required_adapters = getattr(module, "REQUIRED_ADAPTERS", [])
    if not required_adapters:
        return None
    adapters: dict[str, Any] = {}
    for adapter_name in required_adapters:
        adapter = get_adapter(adapter_name)
        if adapter is None:
            raise WorkflowFailure(
                WorkflowErrorCode.DISCOVERY_ADAPTER_NOT_REGISTERED,
                f"Required adapter '{adapter_name}' is not registered for module '{module.__name__}'",
            )
        adapters[adapter_name] = adapter
    return adapters


@dataclass
class DiscoveryBatchStats:
    """Batch-level counters for a discover_batch run."""

    processed_unchanged_identifiers: list[str] = field(default_factory=list)
    changed_count: int = 0
    unchanged_count: int = 0
    no_datasets_count: int = 0
    timeout_count: int = 0
    error_count: int = 0
    missing_registry_count: int = 0
    failed_source_identifiers: list[str] = field(default_factory=list)
    total_datasets: int = 0
    total_sbids: int = 0

def record_failed_result(
    stats: DiscoveryBatchStats,
    project_module: str,
    source_identifier: str,
    outcome: str,
    error: Any,
    duration_ms: Any,
) -> None:
    """Normalize timeout/error counting and failed-source logging."""
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


async def finalize_source_marks(
    db: Any,
    stats: DiscoveryBatchStats,
    project_module: str,
    now: datetime,
    claim_token: str | None,
) -> int:
    """Mark unchanged as checked and failed as attempted while the claim is still owned."""
    logger.debug(
        "event=discover_batch_mark_checked_start project_module=%s processed_count=%s failed_count=%s",
        project_module,
        len(stats.processed_unchanged_identifiers),
        len(stats.failed_source_identifiers),
    )
    # Without claim: resolve IDs then mark checked/attempted with claim: use claim-scoped updates
    if claim_token is None:
        sources = await source_registry_service.get_sources_by_identifiers(
            db,
            project_module,
            stats.processed_unchanged_identifiers,
        )
        source_id_by_identifier = {
            source["source_identifier"]: source["uuid"]
            for source in sources
            if source.get("uuid") and source.get("source_identifier")
        }
        source_ids = [
            source_id_by_identifier[sid]
            for sid in stats.processed_unchanged_identifiers
            if sid in source_id_by_identifier
        ]
        await source_registry_service.mark_sources_checked(
            db, source_ids, checked_at=now, commit=False
        )
        await source_registry_service.mark_sources_attempted(
            db,
            project_module,
            list(set(stats.failed_source_identifiers)),
            attempted_at=now,
            commit=False,
        )
    else:
        await source_registry_service.mark_sources_checked_if_claimed(
            db,
            project_module,
            stats.processed_unchanged_identifiers,
            claim_token=claim_token,
            checked_at=now,
            commit=False,
        )
        await source_registry_service.mark_sources_attempted_if_claimed(
            db,
            project_module,
            list(set(stats.failed_source_identifiers)),
            claim_token=claim_token,
            attempted_at=now,
            commit=False,
        )
    processed_count = stats.changed_count + stats.unchanged_count
    logger.debug(
        "event=discover_batch_mark_checked_done project_module=%s processed_count=%s",
        project_module,
        len(stats.processed_unchanged_identifiers),
    )
    logger.debug(
        "event=discover_batch_marked_checked project_module=%s processed_count=%s missing_registry_count=%s",
        project_module,
        processed_count,
        stats.missing_registry_count,
    )
    return processed_count


async def persist_source_result(
    *,
    db: Any,
    stats: DiscoveryBatchStats,
    project_module: str,
    source_identifier: str,
    duration_ms: Any,
    persist: Callable[[], Awaitable[Any]],
    should_commit: Callable[[Any], bool],
) -> Any | None:
    """Run persist(), commit if should_commit(result), else rollback and record failed."""
    try:
        result = await persist()
        if should_commit(result):
            await db.commit()
        return result
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
        record_failed_result(
            stats=stats,
            project_module=project_module,
            source_identifier=source_identifier,
            outcome="error",
            error=e,
            duration_ms=duration_ms,
        )
        return None


def build_discovery_result(
    project_module: str,
    source_identifiers: list[str],
    stats: DiscoveryBatchStats,
) -> dict[str, Any]:
    """Build the stable result payload for callers/ops logs."""
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
