"""Tests for log_context module."""
import logging
from contextvars import ContextVar

import pytest

from app.core.log_context import (
    ExecutionLogContextFilter,
    bind_execution_log_context,
    bind_execution_log_context_from_arq,
    parse_arq_job_context,
)


# ---- parse_arq_job_context ----


def test_parse_arq_job_context_with_dict():
    ctx = {"job_id": "abc123", "job_try": 2}
    job_id, job_try = parse_arq_job_context(ctx)
    assert job_id == "abc123"
    assert job_try == 2


def test_parse_arq_job_context_with_object():
    class Ctx:
        job_id = "xyz"
        job_try = 5

    job_id, job_try = parse_arq_job_context(Ctx())
    assert job_id == "xyz"
    assert job_try == 5


def test_parse_arq_job_context_missing_fields():
    job_id, job_try = parse_arq_job_context({})
    assert job_id is None
    assert job_try is None


def test_parse_arq_job_context_converts_job_try_string():
    ctx = {"job_id": "j1", "job_try": "3"}
    _, job_try = parse_arq_job_context(ctx)
    assert job_try == 3


def test_parse_arq_job_context_invalid_job_try_returns_none():
    ctx = {"job_id": "j1", "job_try": "not-a-number"}
    _, job_try = parse_arq_job_context(ctx)
    assert job_try is None


def test_parse_arq_job_context_job_id_coerced_to_str():
    ctx = {"job_id": 42, "job_try": 1}
    job_id, _ = parse_arq_job_context(ctx)
    assert isinstance(job_id, str)
    assert job_id == "42"


# ---- ExecutionLogContextFilter ----


def _make_record() -> logging.LogRecord:
    return logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="hi", args=(), exc_info=None,
    )


def test_log_filter_defaults_to_absent():
    f = ExecutionLogContextFilter()
    record = _make_record()
    f.filter(record)
    assert record.execution_id == "-"
    assert record.arq_job_id == "-"
    assert record.job_try == "-"


def test_log_filter_reflects_context():
    f = ExecutionLogContextFilter()
    with bind_execution_log_context(execution_id="exec-1", arq_job_id="job-2", job_try=3):
        record = _make_record()
        f.filter(record)
        assert record.execution_id == "exec-1"
        assert record.arq_job_id == "job-2"
        assert record.job_try == "3"


def test_log_filter_absent_after_context_exits():
    f = ExecutionLogContextFilter()
    with bind_execution_log_context(execution_id="exec-x"):
        pass  # exited immediately
    record = _make_record()
    f.filter(record)
    assert record.execution_id == "-"


# ---- bind_execution_log_context ----


def test_bind_execution_log_context_is_reentrant():
    """Nested binds should each restore to the outer value."""
    f = ExecutionLogContextFilter()

    with bind_execution_log_context(execution_id="outer"):
        r1 = _make_record()
        f.filter(r1)
        assert r1.execution_id == "outer"

        with bind_execution_log_context(execution_id="inner"):
            r2 = _make_record()
            f.filter(r2)
            assert r2.execution_id == "inner"

        r3 = _make_record()
        f.filter(r3)
        assert r3.execution_id == "outer"

    r4 = _make_record()
    f.filter(r4)
    assert r4.execution_id == "-"


def test_bind_execution_log_context_partial_fields():
    f = ExecutionLogContextFilter()
    with bind_execution_log_context(execution_id="eid"):
        record = _make_record()
        f.filter(record)
        assert record.execution_id == "eid"
        assert record.arq_job_id == "-"
        assert record.job_try == "-"


def test_bind_execution_log_context_empty_string_becomes_absent():
    f = ExecutionLogContextFilter()
    with bind_execution_log_context(execution_id="", arq_job_id=""):
        record = _make_record()
        f.filter(record)
        assert record.execution_id == "-"
        assert record.arq_job_id == "-"


def test_bind_execution_log_context_restores_after_exception():
    f = ExecutionLogContextFilter()
    try:
        with bind_execution_log_context(execution_id="boom-exec"):
            raise RuntimeError("oops")
    except RuntimeError:
        pass
    record = _make_record()
    f.filter(record)
    assert record.execution_id == "-"


# ---- bind_execution_log_context_from_arq ----


def test_bind_execution_log_context_from_arq_yields_job_info():
    ctx = {"job_id": "arq-j1", "job_try": 7}
    with bind_execution_log_context_from_arq(ctx=ctx, execution_id="eid-9") as (job_id, job_try):
        assert job_id == "arq-j1"
        assert job_try == 7
        f = ExecutionLogContextFilter()
        record = _make_record()
        f.filter(record)
        assert record.execution_id == "eid-9"
        assert record.arq_job_id == "arq-j1"
        assert record.job_try == "7"


def test_bind_execution_log_context_from_arq_cleans_up():
    ctx = {"job_id": "arq-j2", "job_try": 1}
    with bind_execution_log_context_from_arq(ctx=ctx):
        pass
    f = ExecutionLogContextFilter()
    record = _make_record()
    f.filter(record)
    assert record.execution_id == "-"
    assert record.arq_job_id == "-"
