"""Service layer for project module discovery/contracts."""

from pathlib import Path
from typing import Any

import httpx

from . import list_project_modules, load_project_module


def get_graph_path(project_module: str) -> str | None:
    """Get GRAPH_PATH."""
    module = load_project_module(project_module)
    return getattr(module, "GRAPH_PATH", None) or None


def get_graph_github_url(project_module: str) -> str | None:
    """Get GRAPH_GITHUB_URL"""
    module = load_project_module(project_module)
    return getattr(module, "GRAPH_GITHUB_URL", None) or None


def resolve_graph_content(project_module: str) -> str:
    """Resolve graph content from GRAPH_PATH or GRAPH_GITHUB_URL."""
    path = get_graph_path(project_module)
    if path:
        p = Path(path)
        if p.exists():
            return p.read_text()
        raise FileNotFoundError(f"Graph path not found: {path}")
    url = get_graph_github_url(project_module)
    if url:
        resp = httpx.get(url, timeout=30.0)
        resp.raise_for_status()
        return resp.text
    raise ValueError(
        f"Project module '{project_module}' has no GRAPH_PATH or GRAPH_GITHUB_URL"
    )


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
            return {
                "project_module": project_module,
                "valid": True,
                "required_adapters": required_adapters if isinstance(required_adapters, list) else [],
                "error": None,
                "exports": [
                    symbol
                    for symbol in ["discover", "prepare_metadata", "stage", "build_manifest_sources", "REQUIRED_ADAPTERS"]
                    if hasattr(module, symbol)
                ],
                "enrichment_keys": enrichment_keys,
                "graph_path": graph_path,
                "graph_github_url": graph_github_url,
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
