from typing import Any

def registry_discovery_complete_message(registered: dict[str, Any]) -> str | None:
    return None


def metadata_discovery_flags_message(source_identifier: str, metadata_rows: list[dict[str, Any]]) -> str | None:
    return None


def parse_execution_source_spec(spec: Any) -> tuple[str | None, str | None, list[str] | None]:
    return None


def filter_archive_rows_by_sbids(rows: list[dict[str, Any]], sbids: list[str] | None) -> list[dict[str, Any]]:
    return None


def parsed_source_readiness_error(
    sid: str,
    sbids: list[str] | None,
    registered: dict[str, Any] | None,
    all_rows_for_source: list[dict[str, Any]],
) -> str | None:
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
