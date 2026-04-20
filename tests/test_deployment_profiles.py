"""Tests for deployment profile resolution and _profile_to_dict in orchestration/service.py."""
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from app.core.orchestration.service import _profile_to_dict, _resolve_deployment_profile

# ---- _profile_to_dict ----


def test_profile_to_dict_rest_remote_defaults():
    profile = {"translation": {}, "deployment": {"kind": "rest_remote"}}
    result = _profile_to_dict(profile)
    assert result["deployment_backend"] == "rest_remote"
    assert result["algo"] == "metis"
    assert result["num_par"] == 1
    assert result["num_islands"] == 0
    assert result["verify_ssl"] is False


def test_profile_to_dict_custom_translation():
    profile = {
        "translation": {"algo": "mysarkar", "num_par": 4, "num_islands": 2, "tm_url": "http://tm:8080"},
        "deployment": {"kind": "rest_remote", "deploy_host": "dim.local", "deploy_port": 8001},
    }
    result = _profile_to_dict(profile)
    assert result["algo"] == "mysarkar"
    assert result["num_par"] == 4
    assert result["num_islands"] == 2
    assert result["tm_url"] == "http://tm:8080"
    assert result["deploy_host"] == "dim.local"
    assert result["deploy_port"] == 8001


def test_profile_to_dict_empty_profile():
    result = _profile_to_dict({})
    assert result["algo"] == "metis"
    assert result["num_par"] == 1
    assert result["deployment_backend"] == "rest_remote"


def test_profile_to_dict_verify_ssl_true():
    profile = {"translation": {}, "deployment": {"verify_ssl": True}}
    result = _profile_to_dict(profile)
    assert result["verify_ssl"] is True


def test_profile_to_dict_deployment_config_included():
    deployment = {"kind": "rest_remote", "deploy_host": "h", "custom_field": "value"}
    profile = {"translation": {}, "deployment": deployment}
    result = _profile_to_dict(profile)
    assert result["deployment_config"] == deployment


def test_profile_to_dict_slurm_remote_backend():
    deployment = {
        "kind": "slurm_remote",
        "login_node": "setonix.pawsey.org.au",
        "dlg_root": "/scratch/pawsey0411/me/dlg",
        "account": "pawsey0411",
        "job_duration_minutes": 30,
        "num_nodes": 2,
        "num_islands": 1,
        "sbatch_mem": "32G",
        "verbose_level": 5,
        "max_threads": 0,
        "all_nics": True,
        "slurm_template_name": "setonix",
    }
    profile = {"translation": {"algo": "metis"}, "deployment": deployment}
    result = _profile_to_dict(profile)
    assert result["deployment_backend"] == "slurm_remote"
    assert result["deployment_config"] == deployment


def test_slurm_remote_schema_create_validates_new_fields():
    """Round-trip the canonical setonix payload through the create schema."""
    from app.schemas.daliuge import DaliugeDeploymentProfileCreate

    body = {
        "name": "setonix-default",
        "translation": {"algo": "metis", "num_par": 1, "num_islands": 1},
        "deployment": {
            "kind": "slurm_remote",
            "login_node": "setonix.pawsey.org.au",
            "dlg_root": "/scratch/pawsey0411/me/dlg",
            "account": "pawsey0411",
            "job_duration_minutes": 30,
        },
    }
    parsed = DaliugeDeploymentProfileCreate.model_validate(body)
    dep = parsed.deployment
    assert dep.kind == "slurm_remote"
    assert dep.num_nodes == 1
    assert dep.num_islands == 1
    assert dep.sbatch_mem == "16G"
    assert dep.verbose_level == 1
    assert dep.max_threads == 0
    assert dep.all_nics is False
    assert dep.slurm_template_name == "setonix"


# ---- _resolve_deployment_profile ----


@pytest.mark.asyncio
async def test_resolve_deployment_profile_uses_profile_id():
    profile_id = uuid4()
    run = {"deployment_profile_id": str(profile_id), "project_module": "m1"}
    stored_profile = {
        "translation": {"algo": "metis"},
        "deployment": {"kind": "rest_remote"},
    }
    with patch(
        "app.core.orchestration.service.crud_daliuge_deployment_profile.get",
        AsyncMock(return_value=stored_profile),
    ):
        result = await _resolve_deployment_profile(db=AsyncMock(), run=run)
    assert result["algo"] == "metis"


@pytest.mark.asyncio
async def test_resolve_deployment_profile_falls_back_to_project_default(monkeypatch):
    """When the run's profile_id doesn't resolve, fall back to project default."""
    run = {"deployment_profile_id": None, "project_module": "m1"}
    stored_profile = {
        "translation": {"algo": "mysarkar"},
        "deployment": {"kind": "rest_remote"},
    }
    # Simulate DB query returning a profile UUID and then loading it
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = str(uuid4())
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(return_value=mock_result)

    with patch(
        "app.core.orchestration.service.crud_daliuge_deployment_profile.get",
        AsyncMock(return_value=stored_profile),
    ):
        result = await _resolve_deployment_profile(db=mock_db, run=run)
    assert result["algo"] == "mysarkar"


@pytest.mark.asyncio
async def test_resolve_deployment_profile_raises_when_none_found():
    from app.core.exceptions.workflow_exceptions import WorkflowErrorCode, WorkflowFailure
    run = {"deployment_profile_id": None, "project_module": None}
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(return_value=mock_result)

    with patch(
        "app.core.orchestration.service.crud_daliuge_deployment_profile.get",
        AsyncMock(return_value=None),
    ):
        with pytest.raises(WorkflowFailure) as exc_info:
            await _resolve_deployment_profile(db=mock_db, run=run)
    assert exc_info.value.code is WorkflowErrorCode.EXECUTION_NO_DEPLOYMENT_PROFILE


# ---- workflow_execution_policy_for_module ----


from app.core.projects.service import resolve_workflow_execute_step_overrides
from app.core.worker.tasks.execution_process import workflow_execution_policy_for_module


def test_execution_policy_defaults_when_no_automation():
    with patch("app.core.worker.tasks.execution_process.get_workflow_execution_automation_policy", return_value={}):
        policy = workflow_execution_policy_for_module("any_module")
    assert policy["enabled"] is False
    assert policy["archive_name"] == "casda"
    assert policy["max_sources_per_execution"] == 20


def test_execution_policy_enabled_by_automation():
    raw = {"enabled": True, "max_sources_per_execution": 10}
    with patch("app.core.worker.tasks.execution_process.get_workflow_execution_automation_policy", return_value=raw):
        policy = workflow_execution_policy_for_module("wallaby")
    assert policy["enabled"] is True
    assert policy["max_sources_per_execution"] == 10


def test_execution_policy_deployment_profile_name():
    raw = {"enabled": True, "deployment_profile_name": "  my-profile  "}
    with patch("app.core.worker.tasks.execution_process.get_workflow_execution_automation_policy", return_value=raw):
        policy = workflow_execution_policy_for_module("wallaby")
    assert policy["deployment_profile_name"] == "my-profile"


def test_execution_policy_deployment_profile_name_empty_ignored():
    raw = {"enabled": True, "deployment_profile_name": "   "}
    with patch("app.core.worker.tasks.execution_process.get_workflow_execution_automation_policy", return_value=raw):
        policy = workflow_execution_policy_for_module("wallaby")
    assert "deployment_profile_name" not in policy


def test_execution_policy_positive_int_fields():
    raw = {
        "concurrent_execution_run_limit": 5,
        "execution_max_attempts_external": 4,
        "execution_poll_step_max_attempts": 12,
        "execution_poll_step_max_duration_minutes": 30,
        "execution_rest_remote_poll_max_rounds": 240,
        "execution_slurm_remote_poll_max_rounds": 480,
    }
    with patch("app.core.worker.tasks.execution_process.get_workflow_execution_automation_policy", return_value=raw):
        policy = workflow_execution_policy_for_module("wallaby")
    assert policy["concurrent_execution_run_limit"] == 5
    assert policy["execution_max_attempts_external"] == 4
    assert policy["execution_poll_step_max_attempts"] == 12
    assert policy["execution_poll_step_max_duration_minutes"] == 30
    assert policy["execution_rest_remote_poll_max_rounds"] == 240
    assert policy["execution_slurm_remote_poll_max_rounds"] == 480


def test_execution_policy_negative_positive_int_fields_ignored():
    raw = {"concurrent_execution_run_limit": -1}
    with patch("app.core.worker.tasks.execution_process.get_workflow_execution_automation_policy", return_value=raw):
        policy = workflow_execution_policy_for_module("wallaby")
    assert "concurrent_execution_run_limit" not in policy


def test_resolve_workflow_execute_step_overrides_uses_new_poll_step_keys():
    raw = {
        "execution_poll_step_max_attempts": 11,
        "execution_poll_step_max_duration_minutes": 22,
        "execution_rest_remote_poll_max_rounds": 123,
        "execution_slurm_remote_poll_max_rounds": 456,
    }
    with patch("app.core.projects.service.get_workflow_execution_automation_policy", return_value=raw):
        out = resolve_workflow_execute_step_overrides("wallaby")
    assert out["poll_max_attempts"] == 11
    assert out["poll_max_duration_minutes"] == 22
    assert out["rest_remote_poll_max_rounds"] == 123
    assert out["slurm_remote_poll_max_rounds"] == 456
