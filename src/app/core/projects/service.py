"""Service layer for project module discovery/contracts."""

from typing import Any

from . import list_project_modules, load_project_module


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
            return {
                "project_module": project_module,
                "valid": True,
                "required_adapters": required_adapters if isinstance(required_adapters, list) else [],
                "error": None,
                "exports": [
                    symbol
                    for symbol in ["discover", "prepare_metadata", "stage", "REQUIRED_ADAPTERS"]
                    if hasattr(module, symbol)
                ],
                "enrichment_keys": enrichment_keys,
            }
        except Exception as exc:
            return {
                "project_module": project_module,
                "valid": False,
                "required_adapters": [],
                "error": str(exc),
                "exports": [],
                "enrichment_keys": [],
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
