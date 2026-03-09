"""Discovery helpers."""
import hashlib
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def group_metadata_by_sbid(metadata_list: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group metadata items by SBID."""
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


def _to_jsonable(value: Any) -> Any:
    """Convert to JSON-serializable form json.dumps(sort_keys=True)."""
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(x) for x in value]
    return value


def _stable_json_dumps(value: Any) -> str:
    """Canonical JSON string; sort_keys makes dict order deterministic."""
    return json.dumps(_to_jsonable(value), sort_keys=True, separators=(",", ":"))


def _payload_signature_and_raw(payload: dict[str, Any]) -> tuple[str, str]:
    raw = _stable_json_dumps(payload)
    sig = hashlib.sha256(raw.encode()).hexdigest()
    logger.debug(
        "event=discovery_payload_signature raw_len=%s hash=%s",
        len(raw),
        sig[:16] + "..." if len(sig) > 16 else sig,
    )
    return sig, raw


def _dataset_sort_key(normalized_dataset: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(normalized_dataset.get("dataset_id") or ""),
        str(normalized_dataset.get("visibility_filename") or ""),
        _stable_json_dumps(normalized_dataset),
    )


def metadata_payload_by_sbid(
    grouped: dict[str, list[dict[str, Any]]],
    discovery_flags: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """Build the exact persisted payload shape used for signature comparison."""
    payload_by_sbid: dict[str, dict[str, Any]] = {}
    normalized_flags = _to_jsonable(discovery_flags or {})
    for sbid, datasets in sorted(grouped.items()):
        normalized_datasets = [_to_jsonable(dataset) for dataset in datasets]
        normalized_datasets.sort(key=_dataset_sort_key)
        metadata_json: dict[str, Any] = {"datasets": normalized_datasets}
        if normalized_flags:
            metadata_json["discovery_flags"] = normalized_flags
        payload_by_sbid[str(sbid)] = metadata_json
    logger.debug(
        "event=discovery_metadata_payload_by_sbid sbids=%s dataset_counts=%s has_flags=%s",
        list(payload_by_sbid.keys()),
        [len(p.get("datasets", [])) for p in payload_by_sbid.values()],
        bool(normalized_flags),
    )
    return payload_by_sbid


def discovery_signature(payload_by_sbid: dict[str, dict[str, Any]]) -> str:
    """Canonical hash of a discovery state, logic goes, same structure => same hash ==> same data."""
    sig, raw = _payload_signature_and_raw(payload_by_sbid)
    sbid_list = list(payload_by_sbid.keys())
    logger.debug(
        "event=discovery_signature_computed sbids=%s input_len=%s hash_prefix=%s",
        sbid_list[:10] if len(sbid_list) > 10 else sbid_list,
        len(raw),
        sig[:16] + "..." if len(sig) > 16 else sig,
    )
    return sig


def existing_signature_from_records(records: list[dict[str, Any]]) -> str:
    """Build from stored archive metadata records."""
    canonical: dict[str, dict[str, Any]] = {}
    for rec in records:
        sbid = str(rec.get("sbid", ""))
        metadata_json = _to_jsonable(rec.get("metadata_json") or {})
        canonical[sbid] = metadata_json
    sig, _ = _payload_signature_and_raw(canonical)
    logger.debug(
        "event=discovery_existing_signature_from_records record_count=%s sbids=%s sig_prefix=%s",
        len(records),
        list(canonical.keys()),
        sig[:16] + "..." if len(sig) > 16 else sig,
    )
    return sig


def validate_prepared_metadata_records(
    metadata_list: list[dict[str, Any]],
    *,
    project_module: str,
    source_identifier: str,
) -> list[dict[str, Any]]:
    for i, rec in enumerate(metadata_list):
        if not isinstance(rec, dict):
            raise ValueError(
                f"module '{project_module}' prepare_metadata record[{i}] must be a dict"
            )
        if "sbid" not in rec or rec.get("sbid") is None:
            raise ValueError(
                f"module '{project_module}' prepare_metadata record[{i}] requires a non-null 'sbid'"
            )
        has_identity = (
            rec.get("dataset_id") is not None or rec.get("visibility_filename") is not None
        )
        if not has_identity:
            raise ValueError(
                f"module '{project_module}' prepare_metadata record[{i}] "
                "requires 'dataset_id' or 'visibility_filename'"
            )
    return metadata_list


NO_DATASETS_PAYLOAD = {"0": {"datasets": [], "discovery_status": "no_datasets"}}
NO_DATASETS_SIGNATURE = discovery_signature(NO_DATASETS_PAYLOAD)
