import logging
from datetime import datetime
from typing import Any, cast

from ...archive.service import archive_metadata_service
from ...registry.service import source_registry_service
from ...utils import (
    NO_DATASETS_PAYLOAD,
    NO_DATASETS_SIGNATURE,
    existing_signature_from_records,
    metadata_payload_by_sbid,
)

logger = logging.getLogger(__name__)


def _raise_claim_lost(project_module: str, source_identifier: str, claim_token: str | None) -> None:
    raise RuntimeError(
        "Discovery claim lost before persistence for "
        f"project_module='{project_module}' source_identifier='{source_identifier}' claim_token='{claim_token}'"
    )


def log_missing_source(project_module: str, source_identifier: str, outcome: str) -> None:
    logger.warning(
        "event=discover_batch_source_missing_registry project_module=%s source_identifier=%s outcome=%s action=skip",
        project_module,
        source_identifier,
        outcome,
    )


async def _resolve_persistable_source(
    db: Any,
    project_module: str,
    source_identifier: str,
    *,
    source: dict[str, Any] | None,
    claim_token: str | None,
    outcome: str,
) -> dict[str, Any] | None:
    if not source or not source.get("uuid"):
        log_missing_source(project_module, source_identifier, outcome)
        return None
    if claim_token is None:
        return source

    claimed_source = await source_registry_service.get_claimed_source_for_update(
        db=db,
        project_module=project_module,
        source_identifier=source_identifier,
        claim_token=claim_token,
    )
    if claimed_source is None:
        _raise_claim_lost(project_module, source_identifier, claim_token)
    return claimed_source

# use stored signature when present, otherwise derive from persisted records
async def resolve_existing_signature(
    db: Any,
    source: dict[str, Any],
    project_module: str,
    source_identifier: str,
) -> str:
    existing_sig = source.get("discovery_signature")
    if existing_sig is not None:
        return cast(str, existing_sig)

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
    return cast(str, existing_sig)

# log unchanged outcome, no db writes; return source_uuid for mark_sources_checked
def handle_unchanged_metadata(
    project_module: str,
    source_identifier: str,
    grouped: dict[str, list[dict[str, Any]]],
    metadata_list: list[dict[str, Any]],
    duration_ms: Any,
    *,
    outcome_label: str,
) -> Any:
    logger.debug(
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
    return source_identifier

# upsert no_datasets and update discovery state when sig changes from previous data
async def handle_no_datasets(
    db: Any,
    project_module: str,
    source_identifier: str,
    source: dict[str, Any] | None,
    claim_token: str | None,
    duration_ms: Any,
    now: datetime,
) -> tuple[bool, Any | None]:
    persisted_source = await _resolve_persistable_source(
        db=db,
        project_module=project_module,
        source_identifier=source_identifier,
        source=source,
        claim_token=claim_token,
        outcome="no_datasets",
    )
    if persisted_source is None:
        return False, None

    stored_sig = await resolve_existing_signature(
        db=db,
        source=persisted_source,
        project_module=project_module,
        source_identifier=source_identifier,
    )

    if stored_sig == NO_DATASETS_SIGNATURE:
        logger.debug(
            "event=discover_batch_source_outcome "
            "project_module=%s source_identifier=%s outcome=no_datasets changed=%s duration_ms=%s",
            project_module,
            source_identifier,
            False,
            duration_ms,
        )
        return False, source_identifier

    logger.debug(
        "event=discover_batch_signature_changed project_module=%s source_identifier=%s "
        "existing_sig=%s new_sig=%s outcome=no_datasets",
        project_module,
        source_identifier,
        stored_sig,
        NO_DATASETS_SIGNATURE,
    )
    await archive_metadata_service.delete_metadata_for_source_except_sbids(
        db=db,
        project_module=project_module,
        source_identifier=source_identifier,
        keep_sbids=["0"],
    )
    await archive_metadata_service.upsert_metadata(
        db=db,
        project_module=project_module,
        source_identifier=source_identifier,
        sbid="0",
        metadata_json=NO_DATASETS_PAYLOAD["0"],
    )
    await source_registry_service.update_source_discovery_state(
        db=db,
        source_id=persisted_source["uuid"],
        checked_at=now,
        discovery_signature=NO_DATASETS_SIGNATURE,
    )
    logger.debug(
        "event=discover_batch_source_outcome "
        "project_module=%s source_identifier=%s outcome=no_datasets changed=%s duration_ms=%s",
        project_module,
        source_identifier,
        True,
        duration_ms,
    )
    return True, None


async def handle_changed_metadata(
    db: Any,
    project_module: str,
    source_identifier: str,
    source: dict[str, Any] | None,
    grouped: dict[str, list[dict[str, Any]]],
    discovery_flags: dict[str, Any],
    new_sig: str,
    claim_token: str | None,
    duration_ms: Any,
    now: datetime,
) -> bool:
    # upsert metadata as a full source snapshot and remove stale SBIDs/sentinels
    persisted_source = await _resolve_persistable_source(
        db=db,
        project_module=project_module,
        source_identifier=source_identifier,
        source=source,
        claim_token=claim_token,
        outcome="has_metadata",
    )
    if persisted_source is None:
        return False

    payload_by_sbid = metadata_payload_by_sbid(grouped, discovery_flags)
    keep_sbids = [str(sbid) for sbid in grouped]
    deleted_count = await archive_metadata_service.delete_metadata_for_source_except_sbids(
        db=db,
        project_module=project_module,
        source_identifier=source_identifier,
        keep_sbids=keep_sbids,
    )
    for sbid, metadata_json in payload_by_sbid.items():
        await archive_metadata_service.upsert_metadata(
            db=db,
            project_module=project_module,
            source_identifier=source_identifier,
            sbid=str(sbid),
            metadata_json=metadata_json,
        )

    await source_registry_service.update_source_discovery_state(
        db=db,
        source_id=persisted_source["uuid"],
        checked_at=now,
        discovery_signature=new_sig,
    )
    await source_registry_service.mark_source_pending_workflow_run(
        db=db,
        source_id=persisted_source["uuid"],
        pending_at=now,
    )
    logger.debug(
        "event=discover_batch_source_outcome "
        "project_module=%s source_identifier=%s outcome=has_metadata changed=%s "
        "sbids=%s datasets=%s deleted_sbids=%s duration_ms=%s",
        project_module,
        source_identifier,
        True,
        len(grouped),
        sum(len(datasets) for datasets in grouped.values()),
        deleted_count,
        duration_ms,
    )
    return True
