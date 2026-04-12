"""Tests for workflow_exceptions module."""
from uuid import UUID

import pytest

from app.core.exceptions.workflow_exceptions import (
    WorkflowErrorCode,
    WorkflowFailure,
    wf_execution_not_found,
    wf_no_deployment_profile,
    wf_staging_requires_casda,
    wf_unexpected,
)

_MAX_TERMINAL_LEN = 900


# ---- WorkflowErrorCode ----


def test_workflow_error_code_values_are_strings():
    for code in WorkflowErrorCode:
        assert isinstance(code.value, str)
        assert code.value == str(code)


def test_workflow_error_code_discovery_codes_exist():
    assert WorkflowErrorCode.DISCOVERY_INVALID_PAYLOAD
    assert WorkflowErrorCode.DISCOVERY_UNKNOWN_PROJECT_MODULE
    assert WorkflowErrorCode.DISCOVERY_REQUEST_MISSING_FIELD
    assert WorkflowErrorCode.DISCOVERY_EMPTY_SOURCE_LIST
    assert WorkflowErrorCode.DISCOVERY_ADAPTER_NOT_REGISTERED


def test_workflow_error_code_execution_codes_exist():
    assert WorkflowErrorCode.EXECUTION_INVALID_PAYLOAD
    assert WorkflowErrorCode.EXECUTION_INVALID_WORKFLOW_KEY
    assert WorkflowErrorCode.EXECUTION_NOT_FOUND
    assert WorkflowErrorCode.EXECUTION_STAGING_PRECONDITION
    assert WorkflowErrorCode.EXECUTION_NO_DEPLOYMENT_PROFILE


# ---- WorkflowFailure construction ----


def test_workflow_failure_stores_code_and_detail():
    wf = WorkflowFailure(WorkflowErrorCode.EXECUTION_NOT_FOUND, "not found")
    assert wf.code is WorkflowErrorCode.EXECUTION_NOT_FOUND
    assert wf.detail == "not found"
    assert wf.cause is None


def test_workflow_failure_stores_cause():
    cause = ValueError("original")
    wf = WorkflowFailure(WorkflowErrorCode.EXECUTION_UNEXPECTED, "boom", cause=cause)
    assert wf.cause is cause


def test_workflow_failure_is_exception():
    wf = WorkflowFailure(WorkflowErrorCode.DISCOVERY_INVALID_PAYLOAD, "bad payload")
    assert isinstance(wf, Exception)


def test_workflow_failure_message_matches_detail():
    wf = WorkflowFailure(WorkflowErrorCode.EXECUTION_NOT_FOUND, "  stripped  ")
    assert str(wf) == "stripped"
    assert wf.detail == "stripped"


# ---- format_for_terminal ----


def test_format_for_terminal_includes_code_and_detail():
    wf = WorkflowFailure(WorkflowErrorCode.EXECUTION_NOT_FOUND, "missing exec")
    result = wf.format_for_terminal()
    assert "EXECUTION_NOT_FOUND" in result
    assert "missing exec" in result


def test_format_for_terminal_truncates_long_messages():
    long_detail = "x" * 1000
    wf = WorkflowFailure(WorkflowErrorCode.EXECUTION_UNEXPECTED, long_detail)
    result = wf.format_for_terminal()
    assert len(result) <= _MAX_TERMINAL_LEN
    assert result.endswith("...")


def test_format_for_terminal_does_not_truncate_short_messages():
    wf = WorkflowFailure(WorkflowErrorCode.EXECUTION_NOT_FOUND, "short")
    result = wf.format_for_terminal()
    assert not result.endswith("...")
    assert len(result) <= _MAX_TERMINAL_LEN


# ---- format_for_ledger ----


def test_format_for_ledger_includes_code_and_detail():
    wf = WorkflowFailure(WorkflowErrorCode.DISCOVERY_INVALID_PAYLOAD, "bad json")
    result = wf.format_for_ledger()
    assert "DISCOVERY_INVALID_PAYLOAD" in result
    assert "bad json" in result


def test_format_for_ledger_allows_longer_than_terminal():
    # ledger has no hard truncation
    long_detail = "y" * 950
    wf = WorkflowFailure(WorkflowErrorCode.EXECUTION_UNEXPECTED, long_detail)
    ledger = wf.format_for_ledger()
    terminal = wf.format_for_terminal()
    assert len(ledger) >= len(terminal)


# ---- factory helpers ----


def test_wf_execution_not_found_contains_uuid():
    uid = UUID("12345678-1234-5678-1234-567812345678")
    wf = wf_execution_not_found(uid)
    assert wf.code is WorkflowErrorCode.EXECUTION_NOT_FOUND
    assert str(uid) in wf.detail


def test_wf_staging_requires_casda_code():
    wf = wf_staging_requires_casda()
    assert wf.code is WorkflowErrorCode.EXECUTION_STAGING_PRECONDITION
    assert "CASDA_USERNAME" in wf.detail


def test_wf_no_deployment_profile_code():
    wf = wf_no_deployment_profile()
    assert wf.code is WorkflowErrorCode.EXECUTION_NO_DEPLOYMENT_PROFILE
    assert "deployment" in wf.detail.lower()


def test_wf_unexpected_wraps_exception():
    original = RuntimeError("disk full")
    wf = wf_unexpected(original)
    assert wf.code is WorkflowErrorCode.EXECUTION_UNEXPECTED
    assert "RuntimeError" in wf.detail
    assert "disk full" in wf.detail
    assert wf.cause is original


def test_wf_unexpected_with_none_cause():
    # should not raise; cause defaults to None gracefully
    wf = wf_unexpected(Exception("plain"))
    assert wf.code is WorkflowErrorCode.EXECUTION_UNEXPECTED
