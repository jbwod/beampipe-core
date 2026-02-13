"""CASDA adapter exports."""
from .service import (
    CASDA_TAP_HEALTH_URL,
    CASDA_TAP_URL,
    CasdaDiscoverAdapter,
    _extract_scan_id,
    query,
    stage_data,
    stage_data_pawsey,
)

casda_adapter = CasdaDiscoverAdapter()
adapter = casda_adapter

__all__ = [
    "CASDA_TAP_HEALTH_URL",
    "CASDA_TAP_URL",
    "CasdaDiscoverAdapter",
    "adapter",
    "casda_adapter",
    "_extract_scan_id",
    "query",
    "stage_data",
    "stage_data_pawsey",
]
