"""Service layer for project module discovery/contracts."""

import json
import logging
from pathlib import Path
from typing import Any

import httpx

from .plugins import list_project_modules, load_project_module

logger = logging.getLogger(__name__)

_GRAPH_FETCH_TIMEOUT = 30.0
_GRAPH_FETCH_RETRIES = 3


def get_graph_path(project_module: str) -> str | None:
    """Get GRAPH_PATH."""
    module = load_project_module(project_module)
    return getattr(module, "GRAPH_PATH", None) or None


def get_graph_github_url(project_module: str) -> str | None:
    """Get GRAPH_GITHUB_URL"""
    module = load_project_module(project_module)
    return getattr(module, "GRAPH_GITHUB_URL", None) or None


def resolve_graph_content(project_module: str) -> str:
    path = get_graph_path(project_module)
    if path:
        p = Path(path)
        if p.exists():
            logger.info("Resolved graph from local path: %s", path)
            return p.read_text()
        raise FileNotFoundError(f"Graph path not found: {path}")

    url = get_graph_github_url(project_module)
    if not url:
        raise ValueError(
            f"Project module '{project_module}' has no GRAPH_PATH or GRAPH_GITHUB_URL"
        )

    last_error: Exception | None = None
    for attempt in range(1, _GRAPH_FETCH_RETRIES + 1):
        try:
            logger.info("Fetching graph from GitHub (attempt %d/%d): %s", attempt, _GRAPH_FETCH_RETRIES, url)
            resp = httpx.get(url, timeout=_GRAPH_FETCH_TIMEOUT)
            resp.raise_for_status()
            content = resp.text
            json.loads(content)
            logger.info("Resolved graph from GitHub: %s", url)
            return content
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            last_error = e
            logger.warning("Graph fetch attempt %d/%d failed: %s", attempt, _GRAPH_FETCH_RETRIES, e)
            if attempt == _GRAPH_FETCH_RETRIES:
                raise
    raise last_error or RuntimeError("Graph fetch failed")


class ProjectModuleService:
    @staticmethod
    def get_contract_status(project_module: str) -> dict[str, Any]:
        """Return discovery contract status for one project module."""
        try:
            module = load_project_module(project_module)
            required_adapters = getattr(module, "REQUIRED_ADAPTERS", [])
            enrichment_keys_raw = getattr(module, "DISCOVERY_ENRICHMENT_KEYS", None)
            enrichment_keys: list[str] = []
            if isinstance(enrichment_keys_raw, list):
                enrichment_keys = [k for k in enrichment_keys_raw if isinstance(k, str)]
            graph_path = getattr(module, "GRAPH_PATH", None)
            graph_github_url = getattr(module, "GRAPH_GITHUB_URL", None)
            wf_auto = getattr(module, "WORKFLOW_RUN_AUTOMATION", None)
            workflow_run_automation: dict[str, Any] | None = (
                dict(wf_auto) if isinstance(wf_auto, dict) else None
            )
            return {
                "project_module": project_module,
                "valid": True,
                "required_adapters": required_adapters if isinstance(required_adapters, list) else [],
                "error": None,
                "exports": [
                    symbol
                    for symbol in [
                        "discover",
                        "prepare_metadata",
                        "stage",
                        "build_manifest_sources",
                        "REQUIRED_ADAPTERS",
                    ]
                    if hasattr(module, symbol)
                ],
                "enrichment_keys": enrichment_keys,
                "graph_path": graph_path,
                "graph_github_url": graph_github_url,
                "workflow_run_automation": workflow_run_automation,
            }
        except Exception as exc:
            return {
                "project_module": project_module,
                "valid": False,
                "required_adapters": [],
                "error": str(exc),
                "exports": [],
                "enrichment_keys": [],
                "graph_path": None,
                "graph_github_url": None,
                "workflow_run_automation": None,
            }

    @staticmethod
    def list_project_names() -> list[str]:
        """Return registered project module names."""
        return list_project_modules()

    @staticmethod
    def list_contract_statuses() -> list[dict[str, Any]]:
        """Return discovery contract status for all installed project modules."""
        module_names = list_project_modules()
        return [ProjectModuleService.get_contract_status(name) for name in module_names]

    @staticmethod
    def project_exists(project_module: str) -> bool:
        """Check whether a project module entry point exists."""
        return project_module in list_project_modules()


project_module_service = ProjectModuleService()
