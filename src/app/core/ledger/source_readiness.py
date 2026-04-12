from typing import Any


def _discovery_lease_blocks_execution(registered: dict[str, Any]) -> bool:
    """True when discovery workers hold an active claim (token set on the registry row)."""
    return bool(registered.get("discovery_claim_token"))


def registry_discovery_complete_message(registered: dict[str, Any]) -> str | None:
    if not registered.get("last_checked_at"):
        return (
            "Discovery has not yet run for this source (last_checked_at is unset). "
            "Run discovery first (POST /api/v1/sources/discover)."
        )
    if not registered.get("discovery_signature"):
        return (
            "Discovery signature is missing. "
            "Run discovery first (POST /api/v1/sources/discover)."
        )
    if _discovery_lease_blocks_execution(registered):
        return "Discovery is still in progress for this source (active lease). Wait and retry."
    return None


def metadata_discovery_flags_message(source_identifier: str, metadata_rows: list[dict[str, Any]]) -> str | None:
    """Return an error if any row has non-empty ``discovery_flags`` with a falsy value."""
    for rec in metadata_rows:
        mj = rec.get("metadata_json")
        if not isinstance(mj, dict):
            continue
        flags = mj.get("discovery_flags")
        if not isinstance(flags, dict) or not flags:
            continue
        bad_keys = [k for k, v in flags.items() if not bool(v)]
        if bad_keys:
            return (
                f"Source {source_identifier} metadata has discovery_flags that have not passed "
                f"(failed keys: {bad_keys}). Re-run discovery."
            )
    return None


def parse_execution_source_spec(spec: Any) -> tuple[str | None, str | None, list[str] | None]:
    """Parse ``source_identifier`` and optional ``sbids`` from a spec (dict or object).

    Returns:
        ``(error_message, sid, sbids)`` — on success ``error_message`` is None and ``sid`` is set.
    """
    sid = spec.get("source_identifier") if isinstance(spec, dict) else getattr(spec, "source_identifier", None)
    if not sid:
        return "Source spec missing source_identifier", None, None
    sbids = spec.get("sbids") if isinstance(spec, dict) else getattr(spec, "sbids", None)
    return None, str(sid), sbids


def filter_archive_rows_by_sbids(rows: list[dict[str, Any]], sbids: list[str] | None) -> list[dict[str, Any]]:
    if not sbids:
        return rows
    want = set(sbids)
    return [r for r in rows if str(r.get("sbid", "")) in want]


def parsed_source_readiness_error(
    sid: str,
    sbids: list[str] | None,
    registered: dict[str, Any] | None,
    all_rows_for_source: list[dict[str, Any]],
) -> str | None:
    """Return a human-readable reason if the source is not ready for execution, else None."""
    if not registered:
        return f"Source {sid} is not registered"
    if not registered.get("enabled", False):
        return f"Source {sid} is disabled"

    reg_msg = registry_discovery_complete_message(registered)
    if reg_msg:
        return f"Source {sid}: {reg_msg}"

    metadata = filter_archive_rows_by_sbids(all_rows_for_source, sbids)
    if not metadata:
        hint = f" (SBIDs: {sbids})" if sbids else ""
        return (
            f"Source {sid} has no discovered metadata{hint}. "
            "Run discovery first (POST /api/v1/sources/discover)."
        )

    flags_msg = metadata_discovery_flags_message(sid, metadata)
    if flags_msg:
        return flags_msg
    return None
