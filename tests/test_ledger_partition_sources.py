"""Tests for ExecutionLedgerService.partition_sources_ready_for_execution."""
from unittest.mock import AsyncMock, patch

import pytest

from app.core.ledger.service import ExecutionLedgerService, execution_ledger_service
from app.core.exceptions.http_exceptions import BadRequestException


def _make_spec(sid: str, sbids=None):
    spec = {"source_identifier": sid}
    if sbids is not None:
        spec["sbids"] = sbids
    return spec


def _ready_registry(sid: str) -> dict:
    return {
        "source_identifier": sid,
        "enabled": True,
        "last_checked_at": "2024-01-01",
        "discovery_signature": "sig",
        "discovery_claim_token": None,
    }


def _ready_metadata_row(sid: str, sbid: str = "1") -> dict:
    return {
        "source_identifier": sid,
        "sbid": sbid,
        "metadata_json": {"discovery_flags": {"casda": True}},
    }


# ---- partition_sources_ready_for_execution ----


@pytest.mark.asyncio
async def test_partition_all_valid():
    db = object()
    specs = [_make_spec("S1"), _make_spec("S2")]
    registry_map = {"S1": _ready_registry("S1"), "S2": _ready_registry("S2")}
    metadata_map = {
        "S1": [_ready_metadata_row("S1")],
        "S2": [_ready_metadata_row("S2")],
    }
    with (
        patch(
            "app.core.ledger.service.source_registry_service.get_registry_read_by_identifiers",
            AsyncMock(return_value=registry_map),
        ),
        patch(
            "app.core.ledger.service.archive_metadata_service.list_metadata_grouped_by_sources",
            AsyncMock(return_value=metadata_map),
        ),
    ):
        valid, skipped = await execution_ledger_service.partition_sources_ready_for_execution(
            db=db, project_module="test_module", sources=specs
        )
    assert len(valid) == 2
    assert skipped == []


@pytest.mark.asyncio
async def test_partition_skips_missing_identifier():
    db = object()
    specs = [{"source_identifier": ""}]  # empty identifier → parse error
    with (
        patch(
            "app.core.ledger.service.source_registry_service.get_registry_read_by_identifiers",
            AsyncMock(return_value={}),
        ),
        patch(
            "app.core.ledger.service.archive_metadata_service.list_metadata_grouped_by_sources",
            AsyncMock(return_value={}),
        ),
    ):
        valid, skipped = await execution_ledger_service.partition_sources_ready_for_execution(
            db=db, project_module="test_module", sources=specs
        )
    assert valid == []
    assert len(skipped) == 1
    assert "reason" in skipped[0]


@pytest.mark.asyncio
async def test_partition_skips_unregistered_source():
    db = object()
    specs = [_make_spec("UNKNOWN")]
    with (
        patch(
            "app.core.ledger.service.source_registry_service.get_registry_read_by_identifiers",
            AsyncMock(return_value={}),  # no registry entry
        ),
        patch(
            "app.core.ledger.service.archive_metadata_service.list_metadata_grouped_by_sources",
            AsyncMock(return_value={}),
        ),
    ):
        valid, skipped = await execution_ledger_service.partition_sources_ready_for_execution(
            db=db, project_module="test_module", sources=specs
        )
    assert valid == []
    assert len(skipped) == 1
    assert "not registered" in skipped[0]["reason"]


@pytest.mark.asyncio
async def test_partition_skips_disabled_source():
    db = object()
    specs = [_make_spec("S1")]
    disabled_registry = {**_ready_registry("S1"), "enabled": False}
    with (
        patch(
            "app.core.ledger.service.source_registry_service.get_registry_read_by_identifiers",
            AsyncMock(return_value={"S1": disabled_registry}),
        ),
        patch(
            "app.core.ledger.service.archive_metadata_service.list_metadata_grouped_by_sources",
            AsyncMock(return_value={"S1": [_ready_metadata_row("S1")]}),
        ),
    ):
        valid, skipped = await execution_ledger_service.partition_sources_ready_for_execution(
            db=db, project_module="test_module", sources=specs
        )
    assert valid == []
    assert len(skipped) == 1
    assert "disabled" in skipped[0]["reason"]


@pytest.mark.asyncio
async def test_partition_skips_source_without_metadata():
    db = object()
    specs = [_make_spec("S1")]
    with (
        patch(
            "app.core.ledger.service.source_registry_service.get_registry_read_by_identifiers",
            AsyncMock(return_value={"S1": _ready_registry("S1")}),
        ),
        patch(
            "app.core.ledger.service.archive_metadata_service.list_metadata_grouped_by_sources",
            AsyncMock(return_value={}),  # no metadata
        ),
    ):
        valid, skipped = await execution_ledger_service.partition_sources_ready_for_execution(
            db=db, project_module="test_module", sources=specs
        )
    assert valid == []
    assert len(skipped) == 1


@pytest.mark.asyncio
async def test_partition_empty_source_list_returns_empty():
    db = object()
    valid, skipped = await execution_ledger_service.partition_sources_ready_for_execution(
        db=db, project_module="test_module", sources=[]
    )
    assert valid == []
    assert skipped == []


@pytest.mark.asyncio
async def test_partition_mixed_valid_and_skipped():
    db = object()
    specs = [_make_spec("GOOD"), _make_spec("BAD")]
    registry_map = {"GOOD": _ready_registry("GOOD")}  # BAD not registered
    metadata_map = {"GOOD": [_ready_metadata_row("GOOD")]}
    with (
        patch(
            "app.core.ledger.service.source_registry_service.get_registry_read_by_identifiers",
            AsyncMock(return_value=registry_map),
        ),
        patch(
            "app.core.ledger.service.archive_metadata_service.list_metadata_grouped_by_sources",
            AsyncMock(return_value=metadata_map),
        ),
    ):
        valid, skipped = await execution_ledger_service.partition_sources_ready_for_execution(
            db=db, project_module="test_module", sources=specs
        )
    assert len(valid) == 1
    assert len(skipped) == 1
    assert skipped[0]["source_identifier"] == "BAD"


# ---- _validate_status_transition ----


from app.models.ledger import ExecutionStatus


def test_valid_transitions():
    svc = ExecutionLedgerService
    assert svc._validate_status_transition(ExecutionStatus.PENDING, ExecutionStatus.RUNNING)
    assert svc._validate_status_transition(ExecutionStatus.RUNNING, ExecutionStatus.COMPLETED)
    assert svc._validate_status_transition(ExecutionStatus.RUNNING, ExecutionStatus.FAILED)
    assert svc._validate_status_transition(ExecutionStatus.FAILED, ExecutionStatus.RETRYING)
    assert svc._validate_status_transition(ExecutionStatus.RETRYING, ExecutionStatus.RUNNING)


def test_invalid_transitions():
    svc = ExecutionLedgerService
    assert not svc._validate_status_transition(ExecutionStatus.COMPLETED, ExecutionStatus.RUNNING)
    assert not svc._validate_status_transition(ExecutionStatus.CANCELLED, ExecutionStatus.RUNNING)
    assert not svc._validate_status_transition(ExecutionStatus.PENDING, ExecutionStatus.COMPLETED)


def test_cancel_allowed_from_multiple_states():
    svc = ExecutionLedgerService
    for state in (ExecutionStatus.PENDING, ExecutionStatus.RUNNING, ExecutionStatus.FAILED, ExecutionStatus.RETRYING):
        assert svc._validate_status_transition(state, ExecutionStatus.CANCELLED), f"cancel from {state}"
