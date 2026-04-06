from datetime import timedelta
from typing import Any

import restate

from ..core.config import settings


def _run_opts_external_io() -> restate.RunOptions[Any]:
    return restate.RunOptions(
        max_attempts=settings.RESTATE_STEP_EXTERNAL_MAX_ATTEMPTS,
        max_duration=timedelta(minutes=settings.RESTATE_STEP_EXTERNAL_MAX_DURATION_MINUTES),
        initial_retry_interval=timedelta(seconds=settings.RESTATE_STEP_INITIAL_RETRY_SECONDS),
        max_retry_interval=timedelta(seconds=settings.RESTATE_STEP_MAX_RETRY_INTERVAL_SECONDS),
        retry_interval_factor=2.0,
    )


def _run_opts_database() -> restate.RunOptions[Any]:
    return restate.RunOptions(
        max_attempts=settings.RESTATE_STEP_DB_MAX_ATTEMPTS,
        max_duration=timedelta(minutes=settings.RESTATE_STEP_DB_MAX_DURATION_MINUTES),
        initial_retry_interval=timedelta(seconds=settings.RESTATE_STEP_INITIAL_RETRY_SECONDS),
        max_retry_interval=timedelta(seconds=settings.RESTATE_STEP_MAX_RETRY_INTERVAL_SECONDS),
        retry_interval_factor=2.0,
    )


def _run_opts_poll() -> restate.RunOptions[Any]:
    return restate.RunOptions(
        max_attempts=settings.RESTATE_STEP_POLL_MAX_ATTEMPTS,
        max_duration=timedelta(minutes=settings.RESTATE_STEP_POLL_MAX_DURATION_MINUTES),
        initial_retry_interval=timedelta(seconds=settings.RESTATE_STEP_INITIAL_RETRY_SECONDS),
        max_retry_interval=timedelta(seconds=settings.RESTATE_STEP_MAX_RETRY_INTERVAL_SECONDS),
        retry_interval_factor=2.0,
    )
