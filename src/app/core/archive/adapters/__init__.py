"""Archive adapter implementations."""

import logging
from importlib.metadata import entry_points

from .base import AdapterRegistry, DiscoverAdapter, adapter_registry

logger = logging.getLogger(__name__)


def _discover_and_register_adapters() -> None:
    """ beampipe.adapters entry points and register them."""
    try:
        eps = entry_points()
        if hasattr(eps, "select"):
            adapter_eps = eps.select(group="beampipe.adapters")
        else:
            adapter_eps = eps.get("beampipe.adapters", [])  # type: ignore[arg-type]
    except Exception as e:
        logger.warning("event=adapter_discovery_error error=%s", e)
        return
    for ep in adapter_eps:
        try:
            mod = ep.load()
            adapter = getattr(mod, "adapter", None)
            if adapter is None:
                logger.warning("event=adapter_missing_attr name=%s", ep.name)
                continue
            if callable(adapter):
                adapter = adapter()
            adapter_registry.register(ep.name, adapter)
            logger.debug("event=adapter_registered name=%s", ep.name)
        except Exception as e:
            logger.warning("event=adapter_load_error name=%s error=%s", ep.name, e)


_discover_and_register_adapters()


def get_adapter(name: str) -> DiscoverAdapter | None:
    return adapter_registry.get(name)


def list_adapter_names() -> list[str]:
    """Return names of all registered adapters (for discovery/validation)."""
    return adapter_registry.names()


def get_health_endpoints() -> list[tuple[str, str]]:
    return adapter_registry.get_health_endpoints()


def query_adapter(name: str, adql: str, tap_url: str | None = None):
    adapter = get_adapter(name)
    if adapter is None:
        raise ValueError(f"Adapter '{name}' not found. Available: {list_adapter_names()}")
    return adapter.query(adql, tap_url=tap_url)


# Stage helpers from casda (no adapter protocol)
from .casda import stage_data as casda_stage_data
from .casda import stage_data_pawsey as casda_stage_data_pawsey

__all__ = [
    "AdapterRegistry",
    "DiscoverAdapter",
    "adapter_registry",
    "get_adapter",
    "get_health_endpoints",
    "list_adapter_names",
    "query_adapter",
    "casda_stage_data",
    "casda_stage_data_pawsey",
]
