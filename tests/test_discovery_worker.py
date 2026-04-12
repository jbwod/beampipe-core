"""Tests for discovery worker tasks."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.app.core.archive import discovery as discovery_service
from src.app.core.exceptions.workflow_exceptions import WorkflowErrorCode, WorkflowFailure
from src.app.core.worker.tasks import discovery as discovery_task
from src.app.core.worker.tasks import discovery_batch
from src.app.core.worker.tasks import discovery_execution
from src.app.core.worker.tasks import discovery_process


# ---- Retry / timeout ----

@pytest.mark.asyncio
async def test_run_discover_with_retry_does_not_retry_on_value_error():
    n = 0

    def discover(_sid):
        nonlocal n
        n += 1
        raise ValueError("bad")

    with pytest.raises(ValueError, match="bad"):
        await discovery_execution.run_discover_with_retry(
            discover_callable=discover,
            source_identifier="x",
            tap_timeout=1,
            adapters=None,
        )
    assert n == 1


@pytest.mark.asyncio
async def test_run_prepare_once_enforces_timeout():
    import time

    def slow(**kwargs):
        time.sleep(0.05)
        return ([], {})

    with pytest.raises(TimeoutError):
        await discovery_execution.run_prepare_once(
            prepare_callable=slow,
            source_identifier="x",
            query_results=[],
            data_url_by_scan_id=None,
            checksum_url_by_scan_id=None,
            tap_timeout=0.01,
            adapters=None,
        )


def test_resolve_module_adapters_raises_for_missing_adapter():
    module = SimpleNamespace(__name__="m", REQUIRED_ADAPTERS=["casda"])
    with patch.object(discovery_batch, "get_adapter", return_value=None):
        with pytest.raises(WorkflowFailure) as excinfo:
            discovery_batch.resolve_module_adapters(module)
    assert excinfo.value.code is WorkflowErrorCode.DISCOVERY_ADAPTER_NOT_REGISTERED
    assert "casda" in excinfo.value.detail


# ---- _process_source: bundle shape ----

def _fake_module():
    return SimpleNamespace(
        __name__="fake",
        discover=lambda sid: sid,
        prepare_metadata=lambda sid, qr: (sid, qr),
    )


@pytest.mark.asyncio
async def test_process_source_empty_query_results_returns_no_datasets():
    with (
        patch.object(discovery_process, "run_discover_with_retry", AsyncMock(return_value={"query_results": []})),
        patch.object(discovery_process, "run_prepare_once", AsyncMock()) as run_prepare,
    ):
        out = await discovery_process.process_source(
            module=_fake_module(),
            project_module="p",
            source_identifier="s",
            tap_timeout=1,
            adapters=None,
        )
    assert out["outcome"] == "no_datasets"
    assert out["metadata_list"] == []
    run_prepare.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_source_missing_query_results_raises():
    with patch.object(
        discovery_process, "run_discover_with_retry", AsyncMock(return_value={"enrichments": {}})
    ):
        with pytest.raises(ValueError, match="missing bundle keys"):
            await discovery_process.process_source(
                module=_fake_module(),
                project_module="p",
                source_identifier="s",
                tap_timeout=1,
                adapters=None,
            )


@pytest.mark.asyncio
async def test_process_source_query_results_must_be_length_checkable():
    with patch.object(
        discovery_process,
        "run_discover_with_retry",
        AsyncMock(return_value={"query_results": object(), "enrichments": {}}),
    ):
        with pytest.raises(ValueError, match="length-checkable"):
            await discovery_process.process_source(
                module=_fake_module(),
                project_module="p",
                source_identifier="s",
                tap_timeout=1,
                adapters=None,
            )


@pytest.mark.asyncio
async def test_process_source_prepare_missing_identity_raises():
    bundle = {"query_results": [{"f": "a"}], "enrichments": {}}
    prepared = ([{"sbid": "1"}], {})  # missing dataset_id / visibility_filename
    with (
        patch.object(discovery_process, "run_discover_with_retry", AsyncMock(return_value=bundle)),
        patch.object(discovery_process, "run_prepare_once", AsyncMock(return_value=prepared)),
    ):
        with pytest.raises(ValueError, match="dataset_id.*visibility_filename"):
            await discovery_process.process_source(
                module=_fake_module(),
                project_module="p",
                source_identifier="s",
                tap_timeout=1,
                adapters=None,
            )


@pytest.mark.asyncio
async def test_process_source_passes_bundle_to_prepare():
    bundle = {"query_results": [{"f": "a"}], "enrichments": {}}
    prepared = ([{"sbid": "1", "dataset_id": "d1"}], {"flag": True})
    with (
        patch.object(discovery_process, "run_discover_with_retry", AsyncMock(return_value=bundle)),
        patch.object(discovery_process, "run_prepare_once", AsyncMock(return_value=prepared)) as run_prepare,
    ):
        out = await discovery_process.process_source(
            module=_fake_module(),
            project_module="p",
            source_identifier="s",
            tap_timeout=1,
            adapters=None,
        )
    assert out["outcome"] == "has_metadata"
    assert out["metadata_list"] == prepared[0]
    assert run_prepare.await_args.kwargs["query_results"] is bundle