import asyncio
import logging
from typing import Any, Callable

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# transient errors we retry on (network/timeouts only)
DISCOVERY_EXCEPTIONS = (TimeoutError, ConnectionError)

DiscoverCallable = Callable[[str, dict[str, Any] | None], Any]
PrepareCallable = Callable[..., Any]


# log each retry attempt before sleeping (debug level)
def _log_discover_retry(retry_state: RetryCallState) -> None:
    """Log when discovery retries after a transient failure."""
    if retry_state.outcome is None or not retry_state.outcome.failed:
        return
    exc = retry_state.outcome.exception()
    source_identifier = retry_state.kwargs.get("source_identifier", "?")
    logger.debug(
        "event=discover_retry_attempt attempt=%s source_identifier=%s error=%s",
        retry_state.attempt_number,
        source_identifier,
        exc,
    )


async def run_sync_with_timeout(
    callable_obj: Callable[..., Any], timeout_seconds: int, *args: Any, **kwargs: Any
) -> Any:
    """Run a sync callable in a thread and enforce timeout."""
    return await asyncio.wait_for(
        asyncio.to_thread(callable_obj, *args, **kwargs),
        timeout=timeout_seconds,
    )


# single attempt: run discover in thread, enforce tap timeout
async def _run_discover_once(
    discover_callable: DiscoverCallable,
    source_identifier: str,
    tap_timeout: int,
    adapters: dict[str, Any] | None = None,
) -> Any:
    if adapters is None:
        return await run_sync_with_timeout(discover_callable, tap_timeout, source_identifier)
    return await run_sync_with_timeout(
        discover_callable,
        tap_timeout,
        source_identifier,
        adapters=adapters,
    )


# --- tenacity retry for discovery ---
# We only retry on TimeoutError and ConnectionError (transient). Other exceptions
# (e.g. ValueError, auth errors) are raised immediately. Exponential backoff:
# 2s, 4s, ... up to 60s between attempts; max 3 attempts then reraise. before_sleep
# logs each retry at debug so we can see which source and attempt failed.
@retry(
    retry=retry_if_exception_type(DISCOVERY_EXCEPTIONS),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(3),
    reraise=True,
    before_sleep=_log_discover_retry,
)
async def run_discover_with_retry(
    discover_callable: DiscoverCallable,
    source_identifier: str,
    tap_timeout: int,
    adapters: dict[str, Any] | None = None,
) -> Any:
    return await _run_discover_once(
        discover_callable=discover_callable,
        source_identifier=source_identifier,
        tap_timeout=tap_timeout,
        adapters=adapters,
    )


# one-shot prepare in thread with timeout (no retry)
async def run_prepare_once(
    prepare_callable: PrepareCallable,
    source_identifier: str,
    query_results: Any,
    data_url_by_scan_id: dict[str, str] | None,
    checksum_url_by_scan_id: dict[str, str] | None,
    tap_timeout: int,
    adapters: dict[str, Any] | None = None,
) -> Any:
    kwargs = {
        "source_identifier": source_identifier,
        "query_results": query_results,
        "data_url_by_scan_id": data_url_by_scan_id,
        "checksum_url_by_scan_id": checksum_url_by_scan_id,
    }
    if adapters is not None:
        kwargs["adapters"] = adapters
    return await run_sync_with_timeout(prepare_callable, tap_timeout, **kwargs)

# normalise prepare_metadata return to (metadata_list, discovery_flags)
def extract_prepare_result(result: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if isinstance(result, tuple):
        metadata_list = result[0]
        discovery_flags = result[1] if len(result) > 1 else {}
    else:
        metadata_list = result
        discovery_flags = {}
    return metadata_list, discovery_flags
