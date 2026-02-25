from typing import Any, Protocol, TypedDict
from types import ModuleType

class DiscoverBundle(TypedDict, total=False):
    query_results: Any
    enrichments: dict[str, Any]

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
    pass

def extract_discover_bundle(discover_output: Any, module_name: str) -> DiscoverBundle:
    pass

def get_discover_enrichment(
    bundle: DiscoverBundle,
    key: str,
    *,
    default: Any = None,
    expected_type: type[Any] | tuple[type[Any], ...] | None = None,
    module_name: str | None = None,
) -> Any:
    pass
