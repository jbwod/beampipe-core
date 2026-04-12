"""Tests for workflow_execution_schedule_task (run scheduler) in worker/tasks/scheduler.py."""
from unittest.mock import AsyncMock, patch

import pytest

from app.core.worker.tasks.scheduler import workflow_execution_schedule_task


def _make_ctx(redis=None):
    return {"redis": redis or AsyncMock()}


def _ok_schedule_result(**overrides) -> dict:
    defaults = {
        "ok": True,
        "scheduled_at": "2024-01-01T00:00:00+00:00",
        "project_module": "wallaby",
        "execution_count": 2,
        "total_sources": 4,
        "execution_ids": ["e1", "e2"],
        "job_ids": ["j1", "j2"],
        "skipped_modules": [],
        "reason_counts": {},
    }
    defaults.update(overrides)
    return defaults


# ---- redis guard ----


@pytest.mark.asyncio
async def test_workflow_execution_schedule_task_raises_if_no_redis():
    ctx = {"redis": None}
    with patch("app.core.worker.tasks.scheduler.local_session") as mock_session:
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await workflow_execution_schedule_task(ctx, project_module="wallaby")

    assert result["ok"] is False
    assert "Redis" in result["error"]


# ---- success path ----


@pytest.mark.asyncio
async def test_workflow_execution_schedule_task_ok():
    ctx = _make_ctx()
    schedule_result = _ok_schedule_result()

    with (
        patch("app.core.worker.tasks.scheduler.local_session") as mock_session,
        patch(
            "app.core.worker.tasks.scheduler.workflow_execution_schedule",
            AsyncMock(return_value=schedule_result),
        ),
    ):
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await workflow_execution_schedule_task(ctx, project_module="wallaby")

    assert result["ok"] is True
    assert result["execution_count"] == 2
    assert result["total_sources"] == 4


@pytest.mark.asyncio
async def test_workflow_execution_schedule_task_no_module_passes_none():
    """When project_module is not given, None is passed to workflow_execution_schedule."""
    ctx = _make_ctx()
    captured = {}

    async def _fake_schedule(db, redis, project_module=None):
        captured["project_module"] = project_module
        return _ok_schedule_result(project_module=project_module or "all")

    with (
        patch("app.core.worker.tasks.scheduler.local_session") as mock_session,
        patch("app.core.worker.tasks.scheduler.workflow_execution_schedule", side_effect=_fake_schedule),
    ):
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        await workflow_execution_schedule_task(ctx)

    assert captured["project_module"] is None


# ---- skipped (zero executions) does not error ----


@pytest.mark.asyncio
async def test_workflow_execution_schedule_task_zero_executions_ok():
    ctx = _make_ctx()
    schedule_result = _ok_schedule_result(execution_count=0, total_sources=0, reason_counts={"no_sources": 1})

    with (
        patch("app.core.worker.tasks.scheduler.local_session") as mock_session,
        patch(
            "app.core.worker.tasks.scheduler.workflow_execution_schedule",
            AsyncMock(return_value=schedule_result),
        ),
    ):
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await workflow_execution_schedule_task(ctx, project_module="wallaby")

    assert result["ok"] is True
    assert result["execution_count"] == 0


# ---- exception handling ----


@pytest.mark.asyncio
async def test_workflow_execution_schedule_task_returns_error_on_exception():
    ctx = _make_ctx()

    with patch("app.core.worker.tasks.scheduler.local_session") as mock_session:
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=RuntimeError("db exploded"))
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await workflow_execution_schedule_task(ctx, project_module="wallaby")

    assert result["ok"] is False
    assert "db exploded" in result["error"]
    assert "scheduled_at" in result
    assert result["project_module"] == "wallaby"


# ---- result always contains ok key ----


@pytest.mark.asyncio
async def test_workflow_execution_schedule_task_ensures_ok_key():
    """result from workflow_execution_schedule missing 'ok' should get ok=True added."""
    ctx = _make_ctx()
    # result without 'ok' key
    schedule_result = {
        "scheduled_at": "2024-01-01T00:00:00+00:00",
        "execution_count": 1,
        "total_sources": 2,
    }

    with (
        patch("app.core.worker.tasks.scheduler.local_session") as mock_session,
        patch(
            "app.core.worker.tasks.scheduler.workflow_execution_schedule",
            AsyncMock(return_value=schedule_result),
        ),
    ):
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session.return_value = mock_cm

        result = await workflow_execution_schedule_task(ctx, project_module="wallaby")

    assert "ok" in result
    assert result["ok"] is True
