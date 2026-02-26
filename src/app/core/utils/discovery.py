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


def discovery_signature(grouped: dict[str, list[dict[str, Any]]]) -> str:
    """Canonical hash of a discovery state, logic goes, same structure => same hash ==> same data."""
    canonical: dict[str, list[str]] = {}
    for sbid, datasets in sorted(grouped.items()):
        keys = sorted(
            (d.get("dataset_id") or d.get("visibility_filename") or "" for d in datasets)
        )
        canonical[sbid] = keys
    raw = json.dumps(canonical, sort_keys=True)
    sig = hashlib.sha256(raw.encode()).hexdigest()
    sbid_list = list(grouped.keys())
    logger.debug(
        "event=discovery_signature_computed sbids=%s input_len=%s hash_prefix=%s",
        sbid_list[:10] if len(sbid_list) > 10 else sbid_list,
        len(raw),
        sig[:16] + "..." if len(sig) > 16 else sig,
    )
    return sig


def existing_signature_from_records(records: list[dict[str, Any]]) -> str:
    """Build from stored archive metadata records."""
    if not records:
        return hashlib.sha256(b"{}").hexdigest()
    canonical: dict[str, list[str]] = {}
    for rec in records:
        sbid = str(rec.get("sbid", ""))
        datasets = (rec.get("metadata_json") or {}).get("datasets") or []
        keys = sorted(
            (d.get("dataset_id") or d.get("visibility_filename") or "" for d in datasets)
        )
        canonical[sbid] = keys
    raw = json.dumps(canonical, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def validate_prepared_metadata_records(
    metadata_list: list[dict[str, Any]],
    *,
    project_module: str,
    source_identifier: str,
) -> list[dict[str, Any]]:
    return metadata_list


NO_DATASETS_SIGNATURE = discovery_signature({"0": []})
