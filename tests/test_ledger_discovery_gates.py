"""Tests for ledger/source_readiness.py (discovery gates for execution eligibility)."""

import pytest

from app.core.ledger.source_readiness import (
    filter_archive_rows_by_sbids,
    metadata_discovery_flags_message,
    parse_execution_source_spec,
    parsed_source_readiness_error,
    registry_discovery_complete_message,
)


# ---- registry_discovery_complete_message ----


def test_registry_complete_message_none_when_ready():
    registered = {
        "last_checked_at": "2024-01-01T00:00:00Z",
        "discovery_signature": "abc123",
        "discovery_claim_token": None,
    }
    assert registry_discovery_complete_message(registered) is None


def test_registry_complete_message_missing_last_checked_at():
    registered = {"last_checked_at": None, "discovery_signature": "sig"}
    msg = registry_discovery_complete_message(registered)
    assert msg is not None
    assert "last_checked_at" in msg or "discovery" in msg.lower()


def test_registry_complete_message_missing_signature():
    registered = {"last_checked_at": "2024-01-01", "discovery_signature": None}
    msg = registry_discovery_complete_message(registered)
    assert msg is not None
    assert "signature" in msg.lower() or "discovery" in msg.lower()


def test_registry_complete_message_active_lease():
    registered = {
        "last_checked_at": "2024-01-01",
        "discovery_signature": "sig",
        "discovery_claim_token": "active-token",
    }
    msg = registry_discovery_complete_message(registered)
    assert msg is not None
    assert "in progress" in msg.lower() or "lease" in msg.lower()


# ---- metadata_discovery_flags_message ----


def test_metadata_flags_message_none_when_all_pass():
    rows = [
        {"metadata_json": {"discovery_flags": {"casda": True, "vizier": True}}},
    ]
    assert metadata_discovery_flags_message("S1", rows) is None


def test_metadata_flags_message_returns_msg_for_false_flag():
    rows = [
        {"metadata_json": {"discovery_flags": {"casda": False}}},
    ]
    msg = metadata_discovery_flags_message("S1", rows)
    assert msg is not None
    assert "casda" in msg


def test_metadata_flags_message_none_for_empty_flags():
    rows = [{"metadata_json": {"discovery_flags": {}}}]
    assert metadata_discovery_flags_message("S1", rows) is None


def test_metadata_flags_message_none_for_no_flags_key():
    rows = [{"metadata_json": {"datasets": []}}]
    assert metadata_discovery_flags_message("S1", rows) is None


def test_metadata_flags_message_skips_non_dict_metadata_json():
    rows = [{"metadata_json": "not-a-dict"}]
    assert metadata_discovery_flags_message("S1", rows) is None


def test_metadata_flags_message_multiple_rows_one_bad():
    rows = [
        {"metadata_json": {"discovery_flags": {"casda": True}}},
        {"metadata_json": {"discovery_flags": {"vizier": False}}},
    ]
    msg = metadata_discovery_flags_message("S1", rows)
    assert msg is not None
    assert "vizier" in msg


# ---- parse_execution_source_spec ----


def test_parse_source_spec_from_dict():
    spec = {"source_identifier": "SBID-1234"}
    err, sid, sbids = parse_execution_source_spec(spec)
    assert err is None
    assert sid == "SBID-1234"
    assert sbids is None


def test_parse_source_spec_from_dict_with_sbids():
    spec = {"source_identifier": "SBID-1234", "sbids": ["1", "2"]}
    err, sid, sbids = parse_execution_source_spec(spec)
    assert err is None
    assert sbids == ["1", "2"]


def test_parse_source_spec_from_object():
    class Spec:
        source_identifier = "OBJ-99"
        sbids = None

    err, sid, sbids = parse_execution_source_spec(Spec())
    assert err is None
    assert sid == "OBJ-99"


def test_parse_source_spec_missing_identifier_returns_error():
    err, sid, sbids = parse_execution_source_spec({})
    assert err is not None
    assert sid is None


def test_parse_source_spec_empty_identifier_returns_error():
    err, sid, sbids = parse_execution_source_spec({"source_identifier": ""})
    assert err is not None


# ---- filter_archive_rows_by_sbids ----


def test_filter_rows_returns_all_when_no_sbids():
    rows = [{"sbid": "1"}, {"sbid": "2"}]
    result = filter_archive_rows_by_sbids(rows, None)
    assert result == rows


def test_filter_rows_filters_by_sbids():
    rows = [{"sbid": "1"}, {"sbid": "2"}, {"sbid": "3"}]
    result = filter_archive_rows_by_sbids(rows, ["1", "3"])
    assert len(result) == 2
    assert all(r["sbid"] in ("1", "3") for r in result)


def test_filter_rows_empty_sbids_list_returns_all():
    rows = [{"sbid": "1"}, {"sbid": "2"}]
    result = filter_archive_rows_by_sbids(rows, [])
    assert result == rows


def test_filter_rows_no_match_returns_empty():
    rows = [{"sbid": "1"}, {"sbid": "2"}]
    result = filter_archive_rows_by_sbids(rows, ["99"])
    assert result == []


# ---- parsed_source_readiness_error (full gate) ----


def _ready_registered() -> dict:
    return {
        "enabled": True,
        "last_checked_at": "2024-01-01",
        "discovery_signature": "sig",
        "discovery_claim_token": None,
    }


def _ready_rows() -> list:
    return [{"sbid": "1", "metadata_json": {"discovery_flags": {"casda": True}}}]


def test_parsed_source_readiness_error_none_when_all_ok():
    err = parsed_source_readiness_error("S1", None, _ready_registered(), _ready_rows())
    assert err is None


def test_parsed_source_readiness_error_not_registered():
    err = parsed_source_readiness_error("S1", None, None, _ready_rows())
    assert err is not None
    assert "not registered" in err


def test_parsed_source_readiness_error_disabled():
    reg = {**_ready_registered(), "enabled": False}
    err = parsed_source_readiness_error("S1", None, reg, _ready_rows())
    assert err is not None
    assert "disabled" in err


def test_parsed_source_readiness_error_no_metadata():
    err = parsed_source_readiness_error("S1", None, _ready_registered(), [])
    assert err is not None
    assert "metadata" in err.lower() or "discovery" in err.lower()


def test_parsed_source_readiness_error_bad_flags():
    rows = [{"sbid": "1", "metadata_json": {"discovery_flags": {"casda": False}}}]
    err = parsed_source_readiness_error("S1", None, _ready_registered(), rows)
    assert err is not None
    assert "casda" in err


def test_parsed_source_readiness_error_sbid_filter_no_match():
    err = parsed_source_readiness_error("S1", ["99"], _ready_registered(), _ready_rows())
    assert err is not None


def test_parsed_source_readiness_error_sbid_filter_match():
    err = parsed_source_readiness_error("S1", ["1"], _ready_registered(), _ready_rows())
    assert err is None


def test_parsed_source_readiness_error_active_lease():
    reg = {**_ready_registered(), "discovery_claim_token": "active"}
    err = parsed_source_readiness_error("S1", None, reg, _ready_rows())
    assert err is not None
