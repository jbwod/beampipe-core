from types import ModuleType
from typing import Any, Protocol, TypedDict, cast


class DiscoverBundle(TypedDict, total=False):
    query_results: Any
    enrichments: dict[str, Any]

# MANIFEST_SCHEMA
# GRAPH_PATH
# GRAPH_GITHUB_URL
# WORKFLOW_EXECUTION_AUTOMATION [Optional]: may include ``deployment_profile_name`` (str) for the
# daliuge deployment profile row name.

class ProjectDiscoveryModule(Protocol):
    REQUIRED_ADAPTERS: list[str]
    def discover(self, source_identifier: str, adapters: dict[str, Any] | None = None) -> DiscoverBundle: ...
    def prepare_metadata(
        self,
        source_identifier: str,
        query_results: DiscoverBundle,
        data_url_by_scan_id: dict[str, str] | None = None,
        checksum_url_by_scan_id: dict[str, str] | None = None,
        adapters: dict[str, Any] | None = None,
    ) -> Any: ...

def validate_project_module_interface(module: ModuleType, module_name: str) -> None:
    discover_fn = getattr(module, "discover", None)
    prepare_fn = getattr(module, "prepare_metadata", None)

    if not callable(discover_fn):
        raise ValueError(
            f"module '{module_name}' must implement discover(source_identifier, adapters=...)"
        )
    if not callable(prepare_fn):
        raise ValueError(
            f"module '{module_name}' must implement prepare_metadata(source_identifier, query_results, ...)"
        )


def extract_discover_bundle(discover_output: Any, module_name: str) -> DiscoverBundle:
    """Validate and return expectted discover output."""
    if not isinstance(discover_output, dict):
        raise ValueError(
            "Project module "
            f"'{module_name}' discover() must return dict bundle with required key: query_results"
        )
    required_keys = {"query_results"}
    missing = sorted(required_keys.difference(discover_output.keys()))
    if missing:
        raise ValueError(
            f"Project module '{module_name}' discover() missing bundle keys: {missing}"
        )
    enrichments = discover_output.get("enrichments")
    if enrichments is not None and not isinstance(enrichments, dict):
        raise ValueError(
            f"Project module '{module_name}' discover() key 'enrichments' must be a dict when provided"
        )
    return cast(DiscoverBundle, discover_output)


# """
# {
#     "enrichments": {
#         "key": "value",
#         "sbid_to_eval_file": "wallaby_eval_file.txt",
#     }
# }
# sit it in the module when validating the discovery outputs
# """
def get_discover_enrichment(
    bundle: DiscoverBundle,
    key: str,
    *,
    default: Any = None,
    expected_type: type[Any] | tuple[type[Any], ...] | None = None,
    module_name: str | None = None,
) -> Any:
    """Read discover enrichments"""
    enrichments = bundle.get("enrichments")
    if enrichments is None:
        return default
    value = enrichments.get(key, default)
    if expected_type is not None and value is not None and not isinstance(value, expected_type):
        module_label = module_name or "unknown"
        expected_label = (
            "|".join(t.__name__ for t in expected_type)
            if isinstance(expected_type, tuple)
            else expected_type.__name__
        )
        raise ValueError(
            f"module '{module_label}' discover() enrichment '{key}' must be {expected_label}"
        )
    return value