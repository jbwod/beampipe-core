"""Correlation IDs for logging (run_id, ARQ job id, job try) via contextvars.
"""
import logging
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any

_ABSENT = "-"

_run_id_var: ContextVar[str | None] = ContextVar("execution_run_id", default=None)
_arq_job_id_var: ContextVar[str | None] = ContextVar("execution_arq_job_id", default=None)
_job_try_var: ContextVar[int | None] = ContextVar("execution_job_try", default=None)


def _fmt_str(value: str | None) -> str:
    if value is None or value == "":
        return _ABSENT
    return value


def _fmt_try(value: int | None) -> str:
    if value is None:
        return _ABSENT
    return str(value)


class ExecutionLogContextFilter(logging.Filter):
    """Inject run_id, arq_job_id, job_try on each LogRecord from contextvars."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _fmt_str(_run_id_var.get())
        record.arq_job_id = _fmt_str(_arq_job_id_var.get())
        record.job_try = _fmt_try(_job_try_var.get())
        return True


def parse_arq_job_context(ctx: Any) -> tuple[str | None, int | None]:
    if isinstance(ctx, dict):
        job_id = ctx.get("job_id")
        job_try = ctx.get("job_try")
    else:
        job_id = getattr(ctx, "job_id", None)
        job_try = getattr(ctx, "job_try", None)
    if job_id is not None:
        job_id = str(job_id)
    if job_try is not None and not isinstance(job_try, int):
        try:
            job_try = int(job_try)
        except (TypeError, ValueError):
            job_try = None
    return job_id, job_try


@contextmanager
def bind_execution_log_context(
    *,
    run_id: str | None = None,
    arq_job_id: str | None = None,
    job_try: int | None = None,
) -> Generator[None, None, None]:
    """Bind correlation fields for the current sync/async call stack (one Task)."""
    t1: Token[str | None] = _run_id_var.set(run_id)
    t2: Token[str | None] = _arq_job_id_var.set(arq_job_id)
    t3: Token[int | None] = _job_try_var.set(job_try)
    try:
        yield
    finally:
        _run_id_var.reset(t1)
        _arq_job_id_var.reset(t2)
        _job_try_var.reset(t3)


@contextmanager
def bind_execution_log_context_from_arq(
    *,
    ctx: Any,
    run_id: str | None = None,
) -> Generator[tuple[str | None, int | None], None, None]:
    arq_job_id, job_try = parse_arq_job_context(ctx)
    with bind_execution_log_context(
        run_id=run_id,
        arq_job_id=arq_job_id,
        job_try=job_try,
    ):
        yield arq_job_id, job_try
