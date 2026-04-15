"""Single-source discovery: run discover (with retry) then prepare_metadata for one source."""

import logging
import time
from typing import Any

from ...projects.contracts import extract_discover_bundle
from ...utils import validate_prepared_metadata_records
from .discovery_execution import (
    extract_prepare_result,
    run_discover_with_retry,
    run_prepare_once,
)

logger = logging.getLogger(__name__)


async def process_source(
    module: Any,
    project_module: str,
    source_identifier: str,
    tap_timeout: int,
    adapters: dict[str, Any] | None,
) -> dict[str, Any]:
    """Run discover (with retry) then prepare for one source; return outcome dict."""
    source_started_at = time.perf_counter()
    discover_fn = getattr(module, "discover")
    prepare_fn = getattr(module, "prepare_metadata")

    # Discover with retry (TimeoutError/ConnectionError only); then normalize bundle
    logger.debug(
        "event=discover_batch_source_discover_start project_module=%s source_identifier=%s tap_timeout=%s",
        project_module,
        source_identifier,
        tap_timeout,
    )
    discover_output_raw = await run_discover_with_retry(
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

    # Empty discover result therefore no_datasets path (no prepare call)
    if len(query_results) == 0:
        return {
            "source_identifier": source_identifier,
            "outcome": "no_datasets",
            "metadata_list": [],
            "discovery_flags": {},
            "duration_ms": int((time.perf_counter() - source_started_at) * 1000),
        }

    # Run prepare_metadata once (with timeout; no retry)
    result = await run_prepare_once(
        prepare_callable=prepare_fn,
        source_identifier=source_identifier,
        query_results=discover_output,
        data_url_by_scan_id=None,
        checksum_url_by_scan_id=None,
        tap_timeout=tap_timeout,
        adapters=adapters,
    )
    metadata_list, discovery_flags = extract_prepare_result(result)
    # Validate required fields (sbid, dataset_id or visibility_filename)
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
        "duration_ms": duration_ms,
    }
