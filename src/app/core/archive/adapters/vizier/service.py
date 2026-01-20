"""
Vizier adapter services.
"""
from __future__ import annotations

from typing import Optional

from astropy.table import Table
from astroquery.utils.tap.core import TapPlus

VIZIER_TAP_URL = "http://tapvizier.cds.unistra.fr/TAPVizieR/tap"


def query(query: str, tap_url: Optional[str] = None) -> Table:
    """Run a TAP query against Vizier (or an overridden TAP URL)."""
    tap_endpoint = tap_url or VIZIER_TAP_URL
    try:
        viziertap = TapPlus(url=tap_endpoint, verbose=False)
        job = viziertap.launch_job_async(query)
        return job.get_results()
    except Exception as e:
        raise RuntimeError(f"Vizier TAP query failed: {e}") from e
