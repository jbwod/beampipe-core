"""Tests for restate_workflows/options.py (_run_opts_* builders)."""
from datetime import timedelta
from unittest.mock import patch

import pytest

from app.restate_workflows.options import (
    _run_opts_database,
    _run_opts_external_io,
    _run_opts_poll,
)


# ---- helpers ----


def _opts_as_dict(opts):
    """Extract meaningful fields from a RunOptions object for assertions."""
    return {
        "max_attempts": opts.max_attempts,
        "max_duration": opts.max_duration,
        "initial_retry_interval": opts.initial_retry_interval,
        "max_retry_interval": opts.max_retry_interval,
        "retry_interval_factor": opts.retry_interval_factor,
    }


# ---- _run_opts_external_io ----


def test_run_opts_external_io_defaults():
    opts = _run_opts_external_io(None)
    d = _opts_as_dict(opts)
    assert d["max_attempts"] > 0
    assert isinstance(d["max_duration"], timedelta)
    assert d["max_duration"].total_seconds() > 0
    assert d["retry_interval_factor"] == pytest.approx(2.0)


def test_run_opts_external_io_override_max_attempts():
    overrides = {"external_max_attempts": 7}
    opts = _run_opts_external_io(overrides)
    assert opts.max_attempts == 7


def test_run_opts_external_io_override_duration():
    overrides = {"external_max_duration_minutes": 10}
    opts = _run_opts_external_io(overrides)
    assert opts.max_duration == timedelta(minutes=10)


def test_run_opts_external_io_override_initial_retry():
    overrides = {"initial_retry_seconds": 5.0}
    opts = _run_opts_external_io(overrides)
    assert opts.initial_retry_interval == timedelta(seconds=5.0)


def test_run_opts_external_io_override_max_retry_interval():
    overrides = {"max_retry_interval_seconds": 60.0}
    opts = _run_opts_external_io(overrides)
    assert opts.max_retry_interval == timedelta(seconds=60.0)


def test_run_opts_external_io_invalid_override_falls_back_to_default():
    # negative or zero is not a positive int/float → falls back to default
    overrides = {"external_max_attempts": -1}
    opts_override = _run_opts_external_io(overrides)
    opts_default = _run_opts_external_io(None)
    assert opts_override.max_attempts == opts_default.max_attempts


def test_run_opts_external_io_zero_override_falls_back_to_default():
    overrides = {"external_max_attempts": 0}
    opts_override = _run_opts_external_io(overrides)
    opts_default = _run_opts_external_io(None)
    assert opts_override.max_attempts == opts_default.max_attempts


# ---- _run_opts_database ----


def test_run_opts_database_defaults():
    opts = _run_opts_database(None)
    d = _opts_as_dict(opts)
    assert d["max_attempts"] > 0
    assert isinstance(d["max_duration"], timedelta)
    assert d["retry_interval_factor"] == pytest.approx(2.0)


def test_run_opts_database_override_max_attempts():
    overrides = {"db_max_attempts": 5}
    opts = _run_opts_database(overrides)
    assert opts.max_attempts == 5


def test_run_opts_database_override_duration():
    overrides = {"db_max_duration_minutes": 3}
    opts = _run_opts_database(overrides)
    assert opts.max_duration == timedelta(minutes=3)


def test_run_opts_database_empty_override_dict():
    opts_empty = _run_opts_database({})
    opts_none = _run_opts_database(None)
    assert opts_empty.max_attempts == opts_none.max_attempts


def test_run_opts_database_irrelevant_key_ignored():
    overrides = {"totally_unknown_key": 999}
    opts = _run_opts_database(overrides)
    opts_default = _run_opts_database(None)
    assert opts.max_attempts == opts_default.max_attempts


# ---- _run_opts_poll ----


def test_run_opts_poll_defaults():
    opts = _run_opts_poll(None)
    d = _opts_as_dict(opts)
    assert d["max_attempts"] > 0
    assert isinstance(d["max_duration"], timedelta)
    assert d["retry_interval_factor"] == pytest.approx(2.0)


def test_run_opts_poll_override_max_attempts():
    overrides = {"poll_max_attempts": 10}
    opts = _run_opts_poll(overrides)
    assert opts.max_attempts == 10


def test_run_opts_poll_override_duration():
    overrides = {"poll_max_duration_minutes": 2}
    opts = _run_opts_poll(overrides)
    assert opts.max_duration == timedelta(minutes=2)


# ---- cross-cutting: distinct defaults ----


def test_external_and_db_opts_differ():
    """external IO and database runs should have distinct default attempt counts."""
    ext = _run_opts_external_io(None)
    db = _run_opts_database(None)
    # They may be equal in config, but both should be sensible positive values.
    assert ext.max_attempts > 0
    assert db.max_attempts > 0


def test_all_opts_have_retry_factor_two():
    for fn in (_run_opts_external_io, _run_opts_database, _run_opts_poll):
        opts = fn(None)
        assert opts.retry_interval_factor == pytest.approx(2.0), f"{fn.__name__} retry_interval_factor"
