from types import SimpleNamespace
import time
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from src.app.core.worker.tasks import discovery as discovery_task


class _SessionContext:
    def __init__(self, db):
        self.db = db

    async def __aenter__(self):
        return self.db

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_run_discover_with_retry_does_not_retry_non_transient_errors():
    call_count = {"count": 0}

    def discover_raises_value_error(source_identifier: str):
        _ = source_identifier
        call_count["count"] += 1
        raise ValueError("invalid query")

    with pytest.raises(ValueError, match="invalid query"):
        await discovery_task._run_discover_with_retry(
            discover_callable=discover_raises_value_error,
            source_identifier="source-1",
            tap_timeout=1,
            adapters=None,
        )

    assert call_count["count"] == 1


@pytest.mark.asyncio
async def test_discover_batch_missing_registry_is_not_failed():
    db = AsyncMock()
    module = SimpleNamespace(__name__="fake.module")

    source_result = {
        "source_identifier": "source-a",
        "outcome": "has_metadata",
        "metadata_list": [{"sbid": "1001", "dataset_id": "dataset-1"}],
        "discovery_flags": {},
        "duration_ms": 1,
    }

    with (
        patch.object(discovery_task, "list_project_modules", return_value=["wallaby"]),
        patch.object(discovery_task, "load_project_module", return_value=module),
        patch.object(discovery_task, "local_session", return_value=_SessionContext(db)),
        patch.object(discovery_task, "_process_source", AsyncMock(return_value=source_result)),
        patch.object(
            discovery_task.source_registry_service,
            "check_existing_source",
            AsyncMock(return_value=None),
        ),
        patch.object(
            discovery_task.source_registry_service,
            "mark_sources_checked",
            AsyncMock(),
        ) as mark_sources_checked,
        patch.object(
            discovery_task.source_registry_service,
            "mark_sources_attempted",
            AsyncMock(),
        ) as mark_sources_attempted,
    ):
        result = await discovery_task.discover_batch(None, "wallaby", ["source-a"])

    assert result["missing_registry_count"] == 1
    assert result["error_count"] == 0
    assert result["failed_count"] == 0
    assert result["failed_sources"] == []
    mark_sources_checked.assert_awaited_once()
    assert mark_sources_checked.await_args.args[1] == []
    mark_sources_attempted.assert_awaited_once()
    assert mark_sources_attempted.await_args.args[2] == []


@pytest.mark.asyncio
async def test_discover_batch_rolls_back_failed_write_and_continues():
    db = AsyncMock()
    module = SimpleNamespace(__name__="fake.module")

    source_results = [
        {
            "source_identifier": "source-fail",
            "outcome": "has_metadata",
            "metadata_list": [{"sbid": "2001", "dataset_id": "dataset-a"}],
            "discovery_flags": {},
            "duration_ms": 1,
        },
        {
            "source_identifier": "source-ok",
            "outcome": "has_metadata",
            "metadata_list": [{"sbid": "2002", "dataset_id": "dataset-b"}],
            "discovery_flags": {},
            "duration_ms": 1,
        },
    ]

    check_existing_source_results = [
        {"uuid": uuid4(), "discovery_signature": "old-signature"},
        {"uuid": uuid4(), "discovery_signature": "old-signature"},
    ]

    with (
        patch.object(discovery_task, "list_project_modules", return_value=["wallaby"]),
        patch.object(discovery_task, "load_project_module", return_value=module),
        patch.object(discovery_task, "local_session", return_value=_SessionContext(db)),
        patch.object(
            discovery_task,
            "_process_source",
            AsyncMock(side_effect=source_results),
        ),
        patch.object(
            discovery_task.source_registry_service,
            "check_existing_source",
            AsyncMock(side_effect=check_existing_source_results),
        ),
        patch.object(discovery_task, "discovery_signature", return_value="new-signature"),
        patch.object(
            discovery_task,
            "_handle_changed_metadata",
            AsyncMock(side_effect=[Exception("db write failed"), True]),
        ),
        patch.object(
            discovery_task.source_registry_service,
            "mark_sources_checked",
            AsyncMock(),
        ),
        patch.object(
            discovery_task.source_registry_service,
            "mark_sources_attempted",
            AsyncMock(),
        ) as mark_sources_attempted,
    ):
        result = await discovery_task.discover_batch(
            None, "wallaby", ["source-fail", "source-ok"]
        )

    assert result["changed_count"] == 1
    assert result["error_count"] == 1
    assert result["failed_count"] == 1
    assert result["failed_sources"] == ["source-fail"]
    assert db.rollback.await_count == 1
    assert db.commit.await_count == 2
    mark_sources_attempted.assert_awaited_once()
    assert mark_sources_attempted.await_args.args[2] == ["source-fail"]


@pytest.mark.asyncio
async def test_run_prepare_once_enforces_timeout():
    def slow_prepare(**kwargs):
        _ = kwargs
        time.sleep(0.05)
        return []

    with pytest.raises(TimeoutError):
        await discovery_task._run_prepare_once(
            prepare_callable=slow_prepare,
            source_identifier="source-1",
            query_results=[],
            data_url_by_scan_id=None,
            checksum_url_by_scan_id=None,
            tap_timeout=0.01,
            adapters=None,
        )


def test_resolve_module_adapters_raises_for_missing_adapter():
    module = SimpleNamespace(__name__="fake.module", REQUIRED_ADAPTERS=["casda"])
    with patch.object(discovery_task, "get_adapter", return_value=None):
        with pytest.raises(
            ValueError, match="Required adapter 'casda' is not registered for module 'fake.module'"
        ):
            discovery_task._resolve_module_adapters(module)


@pytest.mark.asyncio
async def test_discover_batch_unchanged_signature_skips_changed_upserts():
    db = AsyncMock()
    module = SimpleNamespace(__name__="fake.module")

    source_result = {
        "source_identifier": "source-same",
        "outcome": "has_metadata",
        "metadata_list": [{"sbid": "3001", "dataset_id": "dataset-1"}],
        "discovery_flags": {},
        "duration_ms": 1,
    }
    source_row = {"uuid": uuid4(), "discovery_signature": "sig-123"}

    with (
        patch.object(discovery_task, "list_project_modules", return_value=["wallaby"]),
        patch.object(discovery_task, "load_project_module", return_value=module),
        patch.object(discovery_task, "local_session", return_value=_SessionContext(db)),
        patch.object(discovery_task, "_process_source", AsyncMock(return_value=source_result)),
        patch.object(
            discovery_task.source_registry_service,
            "check_existing_source",
            AsyncMock(return_value=source_row),
        ),
        patch.object(discovery_task, "discovery_signature", return_value="sig-123"),
        patch.object(discovery_task, "_handle_changed_metadata", AsyncMock()) as handle_changed,
        patch.object(
            discovery_task.source_registry_service,
            "mark_sources_checked",
            AsyncMock(),
        ) as mark_sources_checked,
        patch.object(
            discovery_task.source_registry_service,
            "mark_sources_attempted",
            AsyncMock(),
        ),
    ):
        result = await discovery_task.discover_batch(None, "wallaby", ["source-same"])

    handle_changed.assert_not_awaited()
    assert result["changed_count"] == 0
    assert result["unchanged_count"] == 1
    mark_sources_checked.assert_awaited_once()
    assert mark_sources_checked.await_args.args[1] == [source_row["uuid"]]


@pytest.mark.asyncio
async def test_discover_batch_no_datasets_unchanged_marks_checked():
    db = AsyncMock()
    module = SimpleNamespace(__name__="fake.module")
    source_uuid = uuid4()

    source_result = {
        "source_identifier": "source-empty",
        "outcome": "no_datasets",
        "metadata_list": [],
        "discovery_flags": {},
        "duration_ms": 1,
    }

    with (
        patch.object(discovery_task, "list_project_modules", return_value=["wallaby"]),
        patch.object(discovery_task, "load_project_module", return_value=module),
        patch.object(discovery_task, "local_session", return_value=_SessionContext(db)),
        patch.object(discovery_task, "_process_source", AsyncMock(return_value=source_result)),
        patch.object(
            discovery_task.source_registry_service,
            "check_existing_source",
            AsyncMock(return_value={"uuid": source_uuid}),
        ),
        patch.object(
            discovery_task,
            "_handle_no_datasets",
            AsyncMock(return_value=(False, source_uuid)),
        ),
        patch.object(
            discovery_task.source_registry_service,
            "mark_sources_checked",
            AsyncMock(),
        ) as mark_sources_checked,
        patch.object(
            discovery_task.source_registry_service,
            "mark_sources_attempted",
            AsyncMock(),
        ) as mark_sources_attempted,
    ):
        result = await discovery_task.discover_batch(None, "wallaby", ["source-empty"])

    assert result["no_datasets_count"] == 1
    assert result["changed_count"] == 0
    assert result["unchanged_count"] == 1
    assert result["failed_count"] == 0
    mark_sources_checked.assert_awaited_once()
    assert mark_sources_checked.await_args.args[1] == [source_uuid]
    mark_sources_attempted.assert_awaited_once()
    assert mark_sources_attempted.await_args.args[2] == []
