"""Tests for projects API handlers."""
from unittest.mock import patch

import pytest

from src.app.api.v1.projects import (
    get_project_module_contract,
    list_project_contracts,
    list_projects,
)
from fastapi import HTTPException


@pytest.mark.asyncio
async def test_list_projects():
    with patch("src.app.api.v1.projects.project_module_service.list_project_names", return_value=["a", "b"]):
        r = await list_projects(request=None)
    assert r["projects"] == ["a", "b"]


@pytest.mark.asyncio
async def test_list_project_contracts():
    statuses = [
        {"project_module": "a", "valid": True, "required_adapters": ["casda"], "error": None, "exports": []},
        {"project_module": "b", "valid": False, "required_adapters": [], "error": "err", "exports": []},
    ]
    with patch("src.app.api.v1.projects.project_module_service.list_contract_statuses", return_value=statuses):
        r = await list_project_contracts(request=None)
    assert r["count"] == 2
    assert r["modules"][0]["valid"] is True
    assert r["modules"][1]["valid"] is False
    assert "err" in (r["modules"][1]["error"] or "")


@pytest.mark.asyncio
async def test_get_project_module_contract_404():
    with patch("src.app.api.v1.projects.project_module_service.project_exists", return_value=False):
        with pytest.raises(HTTPException) as exc:
            await get_project_module_contract(request=None, project_module="missing")
    assert exc.value.status_code == 404
