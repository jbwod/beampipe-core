"""Tests for discover_schedule_task (discovery scheduler) in worker/tasks/scheduler.py."""
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.worker.tasks.scheduler import discover_schedule_task


def _make_ctx(redis=None):
    return {"redis": redis or AsyncMock()}


def _ok_module_result(**overrides) -> dict:
    defaults = {
        "ok": True,
        "total_sources": 5,
        "total_jobs": 1,
        "job_ids": ["job-1"],
        "enqueue_failures": 0,
        "failed_batches": [],
        "max_sources_per_run": 5,
        "queue_depth": None,
        "skipped_due_to_queue_full": False,
        "skipped_due_to_tap_unreachable": False,
        "skipped_due_to_tick_discovery_batch_limit": False,
        "admitted_by_rate": 5,
        "blocked_by_rate": False,
        "blocked_by_in_flight": False,
        "tap_unreachable": [],
    }
    defaults.update(overrides)
    return defaults


# ---- discover_schedule_task: redis check ----


@pytest.mark.asyncio
async def test_discover_schedule_task_raises_if_no_redis():
    ctx = {"redis": None}
    with patch("app.core.worker.tasks.scheduler.local_session") as mock_session:
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm
        result = await discover_schedule_task(ctx, project_module="wallaby")
    assert result["ok"] is False
    assert "Redis" in result["error"]


# ---- discover_schedule_task: single module success ----


@pytest.mark.asyncio
async def test_discover_schedule_task_single_module_ok():
    ctx = _make_ctx()
    module_result = _ok_module_result()

    with (
        patch("app.core.worker.tasks.scheduler.local_session") as mock_session,
        patch("app.core.worker.tasks.scheduler.list_project_modules", return_value=["wallaby"]),
        patch("app.core.worker.tasks.scheduler.discover_schedule", AsyncMock(return_value=module_result)),
    ):
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await discover_schedule_task(ctx, project_module="wallaby")

    assert result["ok"] is True
    assert result["total_sources"] == 5
    assert result["total_jobs"] == 1
    assert "job-1" in result["job_ids"]


# ---- discover_schedule_task: multi-module aggregation ----


@pytest.mark.asyncio
async def test_discover_schedule_task_aggregates_multiple_modules():
    ctx = _make_ctx()
    r1 = _ok_module_result(total_sources=3, total_jobs=1, admitted_by_rate=3)
    r2 = _ok_module_result(total_sources=7, total_jobs=2, admitted_by_rate=7, job_ids=["j2", "j3"])

    call_count = 0

    async def _fake_schedule(db, redis, project_module):
        nonlocal call_count
        call_count += 1
        return r1 if project_module == "m1" else r2

    with (
        patch("app.core.worker.tasks.scheduler.local_session") as mock_session,
        patch("app.core.worker.tasks.scheduler.list_project_modules", return_value=["m1", "m2"]),
        patch("app.core.worker.tasks.scheduler.discover_schedule", side_effect=_fake_schedule),
    ):
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await discover_schedule_task(ctx)  # no project_module → all

    assert call_count == 2
    assert result["total_sources"] == 10
    assert result["total_jobs"] == 3
    assert set(result["job_ids"]) == {"job-1", "j2", "j3"}
    assert result["admitted_by_rate"] == 10


# ---- discover_schedule_task: propagates flag states ----


@pytest.mark.asyncio
async def test_discover_schedule_task_propagates_queue_full_flag():
    ctx = _make_ctx()
    module_result = _ok_module_result(skipped_due_to_queue_full=True, total_sources=0, total_jobs=0)

    with (
        patch("app.core.worker.tasks.scheduler.local_session") as mock_session,
        patch("app.core.worker.tasks.scheduler.list_project_modules", return_value=["wallaby"]),
        patch("app.core.worker.tasks.scheduler.discover_schedule", AsyncMock(return_value=module_result)),
    ):
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await discover_schedule_task(ctx, project_module="wallaby")

    assert result["skipped_due_to_queue_full"] is True


@pytest.mark.asyncio
async def test_discover_schedule_task_propagates_blocked_by_rate():
    ctx = _make_ctx()
    module_result = _ok_module_result(blocked_by_rate=True, total_sources=0, total_jobs=0)

    with (
        patch("app.core.worker.tasks.scheduler.local_session") as mock_session,
        patch("app.core.worker.tasks.scheduler.list_project_modules", return_value=["wallaby"]),
        patch("app.core.worker.tasks.scheduler.discover_schedule", AsyncMock(return_value=module_result)),
    ):
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await discover_schedule_task(ctx, project_module="wallaby")

    assert result["blocked_by_rate"] is True


# ---- discover_schedule_task: exception path ----


@pytest.mark.asyncio
async def test_discover_schedule_task_returns_error_on_exception():
    ctx = _make_ctx()

    with patch("app.core.worker.tasks.scheduler.local_session") as mock_session:
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=RuntimeError("db down"))
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await discover_schedule_task(ctx, project_module="wallaby")

    assert result["ok"] is False
    assert "db down" in result["error"]
    assert "scheduled_at" in result


# ---- discover_schedule_task: tap_unreachable aggregation ----


@pytest.mark.asyncio
async def test_discover_schedule_task_aggregates_tap_unreachable():
    ctx = _make_ctx()
    r1 = _ok_module_result(tap_unreachable=["casda"], skipped_due_to_tap_unreachable=True)
    r2 = _ok_module_result(tap_unreachable=["vizier"])

    results_iter = iter([r1, r2])

    async def _fake_schedule(db, redis, project_module):
        return next(results_iter)

    with (
        patch("app.core.worker.tasks.scheduler.local_session") as mock_session,
        patch("app.core.worker.tasks.scheduler.list_project_modules", return_value=["m1", "m2"]),
        patch("app.core.worker.tasks.scheduler.discover_schedule", side_effect=_fake_schedule),
    ):
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await discover_schedule_task(ctx)

    assert "casda" in result["tap_unreachable"]
    assert "vizier" in result["tap_unreachable"]
    assert result["skipped_due_to_tap_unreachable"] is True
