"""Adapter protocol and registry for discovery backends."""

from typing import Protocol

from astropy.table import Table


class DiscoverAdapter(Protocol):
    """Protocol for TAP/archive adapters used by discovery."""

    @property
    def health_url(self) -> str:
        """Health-check URL for this adapter."""
        ...

    @property
    def tap_url(self) -> str:
        """Default TAP base URL for queries."""
        ...

    def query(self, query: str, tap_url: str | None = None) -> Table:
        """Execute a discovery query."""
        ...


class AdapterRegistry:
    """Runtime registry of named adapters."""

    def __init__(self) -> None:
        self._adapters: dict[str, DiscoverAdapter] = {}

    def register(self, name: str, adapter: DiscoverAdapter) -> None:
        self._adapters[name] = adapter

    def get(self, name: str) -> DiscoverAdapter | None:
        return self._adapters.get(name)

    def names(self) -> list[str]:
        return list(self._adapters.keys())

    def get_health_endpoints(self) -> list[tuple[str, str]]:
        return [(name, adapter.health_url) for name, adapter in self._adapters.items()]


adapter_registry = AdapterRegistry()
