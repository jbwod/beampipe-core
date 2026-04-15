"""
CASDA adapter services.
"""
import logging
import re
from collections.abc import Iterator
from typing import Any

import requests
from astropy.table import Table
from astroquery.utils.tap.core import TapPlus

from ....utils import extract_filename_from_url, iter_uws_results

logger = logging.getLogger(__name__)

# CASDA TAP endpoint and health-check URL (TAP base is used for health)
CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"
CASDA_TAP_HEALTH_URL = CASDA_TAP_URL

# CASDA datalink base for evaluation/access URLs (e.g. ?ID=evaluation-N)
# CASDA_DATALINK_BASE = "https://data.csiro.au/casda_vo_proxy/vo/datalink/links"


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

    def query(self, query_str: str, tap_url: str | None = None) -> Table:
        return query(query_str, tap_url=tap_url or self._tap_url)


def query(query: str, tap_url: str | None = None) -> Table:
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


def _iter_datasets(records: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    for rec in records:
        yield from (rec.get("metadata_json") or {}).get("datasets") or []


def _safe_table_column(table: Table, colname: str) -> list:
    return list(table[colname]) if colname in table.colnames else []


def _run_staging_job(casda, table: Table, service_name: str, verbose: bool) -> str:
    job_url = casda._create_job(table, service_name, verbose)
    casda._complete_job(job_url, verbose)
    results_url = f"{job_url}/results"
    session = getattr(casda, "_session", None)
    response = session.get(results_url) if session else requests.get(results_url)
    response.raise_for_status()
    return response.text


def stage_data(
    casda,
    query_results: Table,
    verbose: bool = True,
    service_name: str = "async_service",
) -> tuple[dict[str, str], dict[str, str]]:
    """Stage visibility files.

    CASDA returns URLs keyed by result id (e.g. visibility-123); we parse job results
    to map scan_id -> staged URL for manifest.
    """
    if len(query_results) == 0:
        logger.warning("event=casda_staging_no_results")
        return {}, {}

    logger.debug("event=casda_staging_start result_count=%s", len(query_results))
    try:
        xml_text = _run_staging_job(casda, query_results, service_name, verbose)
        data_url_by_scan_id, checksum_url_by_scan_id = _parse_job_results(xml_text)
        # casda.stage_data
        # Try to get a scan-id keyed mapping from CASDA job results (if available).
        # https://data.csiro.au/casda_vo_proxy/vo/datalink/links?ID=scan-105366-255133
        # https://astroquery.readthedocs.io/en/latest/_modules/astroquery/casda/core.html#CasdaClass.stage_data
        # TLDR; casda.stage_data returns a list of URLs, which is fine if we're using all of them,
        # but we need to get the scan-id keyed mapping from the CASDA job results.
        # otherwise we don't know which url corresponds to which scan-id (like with ingest
        #  we can infer from the path, but not for the checksums)
        # ie; what happens when duplicate filename, but different obs_publisher_did?
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


def metadata_records_to_staging_table(records: list[dict]) -> Table:
    """Need to turn this back to an Astropy Table."""
    rows = [
        {
            "access_url": ds["access_url"],
            "obs_publisher_did": ds.get("obs_publisher_did", ""),
            "filename": ds.get("dataset_id") or ds.get("visibility_filename", ""),
        }
        for ds in _iter_datasets(records)
        if ds.get("access_url")
    ]
    return Table(rows) if rows else Table()


def metadata_records_to_eval_staging_table(records: list[dict]) -> Table:
    """Need to turn this back to an Astropy Table"""
    seen: set[tuple[str, str]] = set()
    rows = []
    for ds in _iter_datasets(records):
        eval_file = ds.get("evaluation_file")
        sbid = ds.get("sbid")
        if not eval_file or sbid is None:
            continue
        key = (str(sbid), str(eval_file))
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "access_url": ds.get("evaluation_file_access_url"),
            "filename": str(eval_file),
            "sbid": str(sbid),
        })
    return Table(rows) if rows else Table()


def stage_eval_data(
    casda,
    eval_table: Table,
    verbose: bool = True,
    service_name: str = "async_service",
) -> tuple[dict[str, str], dict[str, str]]:
    """Stage evaluation files; returns (eval_urls_by_sbid, eval_checksum_urls_by_sbid)."""
    if len(eval_table) == 0:
        return {}, {}

    logger.debug("event=casda_eval_staging_start result_count=%s", len(eval_table))
    try:
        xml_text = _run_staging_job(casda, eval_table, service_name, verbose)
        eval_url_by_filename, eval_checksum_url_by_filename = _parse_eval_job_results(xml_text)
        sbid_col = _safe_table_column(eval_table, "sbid")
        filename_col = _safe_table_column(eval_table, "filename")
        eval_urls_by_sbid: dict[str, str] = {}
        eval_checksum_urls_by_sbid: dict[str, str] = {}
        for i, fn in enumerate(filename_col):
            if i < len(sbid_col):
                sbid = str(sbid_col[i])
                fn_str = str(fn)
                url = eval_url_by_filename.get(fn_str)
                checksum_url = eval_checksum_url_by_filename.get(fn_str)
                if url and sbid:
                    eval_urls_by_sbid[sbid] = url
                if checksum_url and sbid:
                    eval_checksum_urls_by_sbid[sbid] = checksum_url
        logger.debug(
            "event=casda_eval_staging_complete count=%s checksum_count=%s",
            len(eval_urls_by_sbid),
            len(eval_checksum_urls_by_sbid),
        )
        return eval_urls_by_sbid, eval_checksum_urls_by_sbid
    except RuntimeError as e:
        logger.error("event=casda_eval_staging_unsupported error=%s", e)
        return {}, {}
    except Exception as e:
        logger.warning("event=casda_eval_staging_error error=%s", e)
        return {}, {}


def _parse_eval_job_results(xml_text: str) -> tuple[dict[str, str], dict[str, str]]:
    """Parse CASDA job results for evaluation files.

    Returns (eval_url_by_filename, eval_checksum_url_by_filename).
    CASDA uses result_id like evaluation-10584 and evaluation-10584.checksum.
    """
    eval_url_by_filename: dict[str, str] = {}
    eval_checksum_url_by_filename: dict[str, str] = {}
    for result_id, url in iter_uws_results(xml_text):
        if ".checksum" in result_id:
            fn = extract_filename_from_url(url)
            if fn and fn.endswith(".checksum"):
                base = fn.removesuffix(".checksum")
                eval_checksum_url_by_filename[base] = url
        else:
            fn = extract_filename_from_url(url)
            if fn:
                eval_url_by_filename[fn] = url
    return eval_url_by_filename, eval_checksum_url_by_filename


def _extract_scan_id(obs_publisher_did: str) -> str | None:
    match = re.search(r"scan-(\d+)-", obs_publisher_did)
    if match:
        return match.group(1)
    return None


def _parse_job_results(xml_text: str) -> tuple[dict[str, str], dict[str, str]]:
    """Parse CASDA visibility staging job results"""
    data_url_by_scan_id: dict[str, str] = {}
    checksum_url_by_scan_id: dict[str, str] = {}
    for result_id, url in iter_uws_results(xml_text):
        if not result_id:
            continue
        match = re.search(r"visibility-(\d+)", result_id)
        if not match:
            continue
        scan_id = match.group(1)
        if ".checksum" in result_id:
            checksum_url_by_scan_id[scan_id] = url
        else:
            data_url_by_scan_id[scan_id] = url
    return data_url_by_scan_id, checksum_url_by_scan_id
