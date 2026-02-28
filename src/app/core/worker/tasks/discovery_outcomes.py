import logging
from datetime import datetime
from typing import Any, cast

from ...archive.service import archive_metadata_service
from ...registry.service import source_registry_service
from ...utils.discovery import NO_DATASETS_SIGNATURE, existing_signature_from_records

logger = logging.getLogger(__name__)


def log_missing_source(project_module: str, source_identifier: str, outcome: str) -> None:
    logger.warning(
        "event=discover_batch_source_missing_registry project_module=%s source_identifier=%s outcome=%s action=skip",
        project_module,
        source_identifier,
        outcome,
    )

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
async def handle_no_datasets(
    db: Any,
    project_module: str,
    source_identifier: str,
    source: dict[str, Any] | None,
    duration_ms: Any,
    now: datetime,
) -> tuple[bool, Any | None]:
    if not source or not source.get("uuid"):
        log_missing_source(project_module, source_identifier, "no_datasets")
        return False, None

    source_uuid = source["uuid"]
    stored_sig = await resolve_existing_signature(
        db=db,
        source=source,
        project_module=project_module,
        source_identifier=source_identifier,
    )

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


async def handle_changed_metadata(
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
    # upsert metadata per sbid and update source discovery state/signature
    if not source or not source.get("uuid"):
        log_missing_source(project_module, source_identifier, "has_metadata")
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
