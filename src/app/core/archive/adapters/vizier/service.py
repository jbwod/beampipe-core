"""
Vizier adapter services.
"""

import logging

from astropy.table import Table
from astroquery.utils.tap.core import TapPlus

logger = logging.getLogger(__name__)

VIZIER_TAP_URL = "http://tapvizier.cds.unistra.fr/TAPVizieR/tap"
# UWS async endpoint for health check (returns 200 + XML when up)
VIZIER_TAP_HEALTH_URL = "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/async"


class VizierDiscoverAdapter:
    """Vizier adapter implementing the DiscoverAdapter protocol for registry and health checks."""

    def __init__(
        self, tap_url: str = VIZIER_TAP_URL, health_url: str = VIZIER_TAP_HEALTH_URL
    ) -> None:
        self._tap_url = tap_url
        self._health_url = health_url

    @property
    def health_url(self) -> str:
        return self._health_url

    @property
    def tap_url(self) -> str:
        return self._tap_url

    def query(self, query_str: str, tap_url: str | None = None) -> Table:
        return query(query_str, tap_url=tap_url or self._tap_url)


def query(query: str, tap_url: str | None = None) -> Table:
    """Run a TAP query against Vizier (or an overridden TAP URL)."""
    tap_endpoint = tap_url or VIZIER_TAP_URL
    logger.debug("event=vizier_tap_query query=%s", query[:200] + "..." if len(query) > 200 else query)
    try:
        viziertap = TapPlus(url=tap_endpoint, verbose=False)
        job = viziertap.launch_job_async(query)
        results = job.get_results()
        logger.debug("event=vizier_tap_query_result row_count=%s", len(results))
        return results
    except Exception as e:
        logger.error("event=vizier_tap_query_error error=%s query_snippet=%s", e, query[:150])
        raise RuntimeError(f"Vizier TAP query failed: {e}") from e
