"""Vizier adapter exports."""
from .service import VIZIER_TAP_HEALTH_URL, VIZIER_TAP_URL, VizierDiscoverAdapter, query

# Default instance for registry; can be overridden when registering.
vizier_adapter = VizierDiscoverAdapter()
# Entry-point contract: modules under beampipe.adapters must expose `adapter`.
adapter = vizier_adapter

__all__ = [
    "VIZIER_TAP_HEALTH_URL",
    "VIZIER_TAP_URL",
    "VizierDiscoverAdapter",
    "adapter",
    "vizier_adapter",
    "query",
]
