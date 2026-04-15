from datetime import timedelta
from typing import Any

import restate

from ..core.config import settings
from ..core.positive_policy import positive_float, positive_int


def _run_opts_external_io(
    overrides: dict[str, Any] | None = None,
) -> restate.RunOptions[Any]:
    max_attempts = positive_int(
        overrides,
        "external_max_attempts",
        settings.RESTATE_STEP_EXTERNAL_MAX_ATTEMPTS,
    )
    max_duration_minutes = positive_int(
        overrides,
        "external_max_duration_minutes",
        settings.RESTATE_STEP_EXTERNAL_MAX_DURATION_MINUTES,
    )
    initial_retry_seconds = positive_float(
        overrides,
        "initial_retry_seconds",
        settings.RESTATE_STEP_INITIAL_RETRY_SECONDS,
    )
    max_retry_interval_seconds = positive_float(
        overrides,
        "max_retry_interval_seconds",
        settings.RESTATE_STEP_MAX_RETRY_INTERVAL_SECONDS,
    )
    return restate.RunOptions(
        max_attempts=max_attempts,
        max_duration=timedelta(minutes=max_duration_minutes),
        initial_retry_interval=timedelta(seconds=initial_retry_seconds),
        max_retry_interval=timedelta(seconds=max_retry_interval_seconds),
        retry_interval_factor=2.0,
    )


def _run_opts_database(
    overrides: dict[str, Any] | None = None,
) -> restate.RunOptions[Any]:
    max_attempts = positive_int(
        overrides,
        "db_max_attempts",
        settings.RESTATE_STEP_DB_MAX_ATTEMPTS,
    )
    max_duration_minutes = positive_int(
        overrides,
        "db_max_duration_minutes",
        settings.RESTATE_STEP_DB_MAX_DURATION_MINUTES,
    )
    initial_retry_seconds = positive_float(
        overrides,
        "initial_retry_seconds",
        settings.RESTATE_STEP_INITIAL_RETRY_SECONDS,
    )
    max_retry_interval_seconds = positive_float(
        overrides,
        "max_retry_interval_seconds",
        settings.RESTATE_STEP_MAX_RETRY_INTERVAL_SECONDS,
    )
    return restate.RunOptions(
        max_attempts=max_attempts,
        max_duration=timedelta(minutes=max_duration_minutes),
        initial_retry_interval=timedelta(seconds=initial_retry_seconds),
        max_retry_interval=timedelta(seconds=max_retry_interval_seconds),
        retry_interval_factor=2.0,
    )


def _run_opts_poll(
    overrides: dict[str, Any] | None = None,
) -> restate.RunOptions[Any]:
    max_attempts = positive_int(
        overrides,
        "poll_max_attempts",
        settings.RESTATE_STEP_POLL_MAX_ATTEMPTS,
    )
    max_duration_minutes = positive_int(
        overrides,
        "poll_max_duration_minutes",
        settings.RESTATE_STEP_POLL_MAX_DURATION_MINUTES,
    )
    initial_retry_seconds = positive_float(
        overrides,
        "initial_retry_seconds",
        settings.RESTATE_STEP_INITIAL_RETRY_SECONDS,
    )
    max_retry_interval_seconds = positive_float(
        overrides,
        "max_retry_interval_seconds",
        settings.RESTATE_STEP_MAX_RETRY_INTERVAL_SECONDS,
    )
    return restate.RunOptions(
        max_attempts=max_attempts,
        max_duration=timedelta(minutes=max_duration_minutes),
        initial_retry_interval=timedelta(seconds=initial_retry_seconds),
        max_retry_interval=timedelta(seconds=max_retry_interval_seconds),
        retry_interval_factor=2.0,
    )
