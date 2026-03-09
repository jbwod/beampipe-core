from unittest.mock import ANY, AsyncMock

import pytest

from app.core.registry.service import SourceRegistryService
from app.core.utils.discovery import (
    NO_DATASETS_PAYLOAD,
    discovery_signature,
    metadata_payload_by_sbid,
    validate_prepared_metadata_records,
)
from app.core.worker.tasks import discovery_outcomes


def test_discovery_signature_changes_when_payload_changes() -> None:
    grouped = {
        "123": [
            {
                "sbid": "123",
                "dataset_id": "dataset-1",
                "visibility_filename": "a.ms",
                "checksum": "old",
            }
        ]
    }
    changed_grouped = {
        "123": [
            {
                "sbid": "123",
                "dataset_id": "dataset-1",
                "visibility_filename": "a.ms",
                "checksum": "new",
            }
        ]
    }

    original_sig = discovery_signature(metadata_payload_by_sbid(grouped, {"casda": True}))
    changed_sig = discovery_signature(metadata_payload_by_sbid(changed_grouped, {"casda": True}))
    flags_sig = discovery_signature(metadata_payload_by_sbid(grouped, {"casda": False}))

    assert original_sig != changed_sig
    assert original_sig != flags_sig


def test_validate_prepared_metadata_records_rejects_null_sbid() -> None:
    with pytest.raises(ValueError, match="non-null 'sbid'"):
        validate_prepared_metadata_records(
            [{"sbid": None, "dataset_id": "dataset-1"}],
            project_module="wallaby_hires",
            source_identifier="HIPASSJ0000-00",
        )


def test_metadata_payload_by_sbid_is_order_stable() -> None:
    grouped_a = {
        "123": [
            {"sbid": "123", "dataset_id": "b", "visibility_filename": "b.ms"},
            {"sbid": "123", "dataset_id": "a", "visibility_filename": "a.ms"},
        ]
    }
    grouped_b = {
        "123": [
            {"sbid": "123", "dataset_id": "a", "visibility_filename": "a.ms"},
            {"sbid": "123", "dataset_id": "b", "visibility_filename": "b.ms"},
        ]
    }

    payload_a = metadata_payload_by_sbid(grouped_a, {"vizier": True})
    payload_b = metadata_payload_by_sbid(grouped_b, {"vizier": True})

    assert payload_a == payload_b
    assert discovery_signature(payload_a) == discovery_signature(payload_b)


@pytest.mark.asyncio
async def test_handle_no_datasets_reconciles_existing_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    delete_mock = AsyncMock(return_value=2)
    upsert_mock = AsyncMock(return_value={})
    update_state_mock = AsyncMock(return_value=None)
    resolve_sig_mock = AsyncMock(return_value="old-signature")

    monkeypatch.setattr(
        discovery_outcomes.archive_metadata_service,
        "delete_metadata_for_source_except_sbids",
        delete_mock,
    )
    monkeypatch.setattr(discovery_outcomes.archive_metadata_service, "upsert_metadata", upsert_mock)
    monkeypatch.setattr(
        discovery_outcomes.source_registry_service,
        "update_source_discovery_state",
        update_state_mock,
    )
    monkeypatch.setattr(discovery_outcomes, "resolve_existing_signature", resolve_sig_mock)

    changed, unchanged_id = await discovery_outcomes.handle_no_datasets(
        db=object(),
        project_module="wallaby_hires",
        source_identifier="HIPASSJ0000-00",
        source={"uuid": "source-uuid"},
        claim_token=None,
        duration_ms=10,
        now="now",
    )

    assert changed is True
    assert unchanged_id is None
    delete_mock.assert_awaited_once_with(
        db=ANY,
        project_module="wallaby_hires",
        source_identifier="HIPASSJ0000-00",
        keep_sbids=["0"],
    )
    upsert_mock.assert_awaited_once_with(
        db=ANY,
        project_module="wallaby_hires",
        source_identifier="HIPASSJ0000-00",
        sbid="0",
        metadata_json=NO_DATASETS_PAYLOAD["0"],
    )


@pytest.mark.asyncio
async def test_handle_changed_metadata_reconciles_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    delete_mock = AsyncMock(return_value=2)
    upsert_mock = AsyncMock(return_value={})
    update_state_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(
        discovery_outcomes.archive_metadata_service,
        "delete_metadata_for_source_except_sbids",
        delete_mock,
    )
    monkeypatch.setattr(discovery_outcomes.archive_metadata_service, "upsert_metadata", upsert_mock)
    monkeypatch.setattr(
        discovery_outcomes.source_registry_service,
        "update_source_discovery_state",
        update_state_mock,
    )

    changed = await discovery_outcomes.handle_changed_metadata(
        db=object(),
        project_module="wallaby_hires",
        source_identifier="HIPASSJ0000-00",
        source={"uuid": "source-uuid"},
        grouped={"111": [{"dataset_id": "dataset-1", "sbid": "111"}]},
        discovery_flags={"vizier": True},
        new_sig="sig",
        claim_token=None,
        duration_ms=10,
        now="now",
    )

    assert changed is True
    delete_mock.assert_awaited_once_with(
        db=ANY,
        project_module="wallaby_hires",
        source_identifier="HIPASSJ0000-00",
        keep_sbids=["111"],
    )
    upsert_mock.assert_awaited_once_with(
        db=ANY,
        project_module="wallaby_hires",
        source_identifier="HIPASSJ0000-00",
        sbid="111",
        metadata_json={"datasets": [{"dataset_id": "dataset-1", "sbid": "111"}], "discovery_flags": {"vizier": True}},
    )


@pytest.mark.asyncio
async def test_register_source_rejects_unknown_project_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.core.registry.service.list_project_modules", lambda: ["wallaby_hires"])

    with pytest.raises(ValueError, match="Project module 'unknown' not found"):
        await SourceRegistryService.register_source(
            db=object(),
            project_module="unknown",
            source_identifier="HIPASSJ0000-00",
            enabled=True,
        )


@pytest.mark.asyncio
async def test_handle_changed_metadata_raises_when_claim_is_lost(monkeypatch: pytest.MonkeyPatch) -> None:
    claim_guard_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        discovery_outcomes.source_registry_service,
        "get_claimed_source_for_update",
        claim_guard_mock,
    )

    with pytest.raises(RuntimeError, match="Discovery claim lost before persistence"):
        await discovery_outcomes.handle_changed_metadata(
            db=object(),
            project_module="wallaby_hires",
            source_identifier="HIPASSJ0000-00",
            source={"uuid": "source-uuid"},
            grouped={"111": [{"dataset_id": "dataset-1", "sbid": "111"}]},
            discovery_flags={"vizier": True},
            new_sig="sig",
            claim_token="claim-token",
            duration_ms=10,
            now="now",
        )
