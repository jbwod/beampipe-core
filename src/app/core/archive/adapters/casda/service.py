"""
CASDA adapter services.
"""
import logging
import re
import xml.etree.ElementTree as ET
from typing import Optional
from urllib.parse import unquote

import requests
from astropy.table import Table
from astroquery.utils.tap.core import TapPlus

logger = logging.getLogger(__name__)

# CASDA TAP endpoint and health-check URL (TAP base is used for health)
CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"
CASDA_TAP_HEALTH_URL = CASDA_TAP_URL


class CasdaDiscoverAdapter:
    """CASDA adapter."""

    def __init__(self, tap_url: str = CASDA_TAP_URL, health_url: str = CASDA_TAP_HEALTH_URL) -> None:
        self._tap_url = tap_url
        self._health_url = health_url

    @property
    def health_url(self) -> str:
        return self._health_url

    @property
    def tap_url(self) -> str:
        return self._tap_url

    def query(self, query_str: str, tap_url: Optional[str] = None) -> Table:
        return query(query_str, tap_url=tap_url or self._tap_url)


def query(query: str, tap_url: Optional[str] = None) -> Table:
    """Run a TAP query against CASDA (or an overridden TAP URL)."""
    tap_endpoint = tap_url or CASDA_TAP_URL
    logger.debug("event=casda_tap_query query=%s", query)
    try:
        casdatap = TapPlus(url=tap_endpoint, verbose=False)
        job = casdatap.launch_job_async(query)
        results = job.get_results()
        logger.debug("event=casda_tap_query_result result_count=%s", len(results))
        return results
    except Exception as e:
        logger.error("event=casda_tap_query_error error=%s", e)
        raise


def stage_data(
    casda,
    query_results: Table,
    verbose: bool = True,
    service_name: str = "async_service",
) -> tuple[dict[str, str], dict[str, str]]:
    if len(query_results) == 0:
        logger.warning("event=casda_staging_no_results")
        return {}, {}

    logger.debug("event=casda_staging_start result_count=%s", len(query_results))
    logger.debug("event=casda_staging_note message=Staging may take time and will poll for completion")
    try:
        data_url_by_scan_id: dict[str, str] = {}
        checksum_url_by_scan_id: dict[str, str] = {}
        # casda.stage_data
        # Try to get a scan-id keyed mapping from CASDA job results (if available).
        # https://data.csiro.au/casda_vo_proxy/vo/datalink/links?ID=scan-105366-255133
        # https://astroquery.readthedocs.io/en/latest/_modules/astroquery/casda/core.html#CasdaClass.stage_data
        # TLDR; casda.stage_data returns a list of URLs, which is fine if we're using all of them, 
        # but we need to get the scan-id keyed mapping from the CASDA job results.
        # otherwise we don't know which url corresponds to which scan-id (like with ingest
        #  we can infer from the path, but not for the checksums)
        # ie; what happens when duplicate filename, but different obs_publisher_did?
        if hasattr(casda, "_create_job") and hasattr(casda, "_complete_job"):
            try:
                job_url = casda._create_job(query_results, service_name, verbose)
                casda._complete_job(job_url, verbose)  # type: ignore[attr-defined]
                results_url = f"{job_url}/results"
                session = getattr(casda, "_session", None)
                # change to httpx at some point
                response = session.get(results_url) if session else requests.get(results_url)
                response.raise_for_status()
                data_url_by_scan_id, checksum_url_by_scan_id = _parse_job_results(response.text)
            except Exception as e:
                logger.error("event=casda_staging_job_error error=%s", e)
                raise
        else:
            logger.error(
                "event=casda_staging_unsupported "
                "message=CASDA object does not support _create_job/_complete_job"
            )
            raise RuntimeError("CASDA does not have required job methods for staging.")

        logger.debug(
            "event=casda_staging_complete data_url_count=%s checksum_url_count=%s",
            len(data_url_by_scan_id),
            len(checksum_url_by_scan_id),
        )

        return data_url_by_scan_id, checksum_url_by_scan_id
    except Exception as e:
        logger.error("event=casda_staging_error error=%s", e)
        raise


def stage_data_pawsey(
    casda, query_results: Table, verbose: bool = True
) -> tuple[dict[str, str], dict[str, str]]:
    """Stage data via the pawsey async service."""
    return stage_data(casda, query_results, verbose=verbose, service_name="pawsey_async_service")


def _extract_scan_id(obs_publisher_did: str) -> Optional[str]:
    match = re.search(r"scan-(\d+)-", obs_publisher_did)
    if match:
        return match.group(1)
    return None


def _parse_job_results(xml_text: str) -> tuple[dict[str, str], dict[str, str]]:
    data_url_by_scan_id: dict[str, str] = {}
    checksum_url_by_scan_id: dict[str, str] = {}

    uws_ns = "http://www.ivoa.net/xml/UWS/v1.0"
    xlink_ns = "http://www.w3.org/1999/xlink"

    root = ET.fromstring(xml_text)
    for result in root.findall(f".//{{{uws_ns}}}result"):
        result_id = result.attrib.get("id", "")
        href = result.attrib.get(f"{{{xlink_ns}}}href", "")
        if not result_id or not href:
            continue
        url = unquote(href)
        match = re.search(r"visibility-(\d+)", result_id)
        if not match:
            continue
        scan_id = match.group(1)
        if ".checksum" in result_id:
            checksum_url_by_scan_id[scan_id] = url
        else:
            data_url_by_scan_id[scan_id] = url

    return data_url_by_scan_id, checksum_url_by_scan_id
