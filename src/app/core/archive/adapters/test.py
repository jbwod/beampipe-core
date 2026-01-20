"""
Test script for CASDA TAP queries and data staging.
Monitoring loop will eventually do automatically.
Basically going to convert each of these into an API endpoint or service function at some point.
https://github.com/ICRAR/wallaby-hires/blob/main/dlg-testdata/test_catalogue_processed.csv
https://astroquery.readthedocs.io/en/latest/casda/casda.html
https://github.com/ICRAR/wallaby-hires/blob/main/wallaby_hires/funcs.py

References to og:
- CASDA initialization: 68 [NOTE existing is with the old API (Casda(username, password))]
- Visibility query: 92, 797-819 (tap_query_filename_visibility)
- SBID evaluation query: 831-853 (tap_query_sbid_evaluation)
- RA/DEC/VSys query: 599-633 (tap_query_RA_DEC_VSYS)
- Staging: 1124, 1642, 1953 (casda.stage_data)
- Coordinate conversion: 541-585 (degrees_to_hms, degrees_to_dms)
"""
import argparse
import json
import logging
import os
import re
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

from astropy.table import Table
from astroquery.casda import Casda


from casda import _extract_scan_id, query as casda_query, stage_data as casda_stage_data, CASDA_TAP_URL
from vizier import VIZIER_TAP_URL, query as vizier_query
from app.core.utils.astro import degrees_to_dms, degrees_to_hms

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Query template for vis files
VISIBILITY_QUERY_TEMPLATE = "SELECT * FROM ivoa.obscore WHERE filename LIKE '{source_identifier}%'"

# Query template for finding eval files by SBID
SBID_EVALUATION_QUERY_TEMPLATE = "SELECT * FROM casda.observation_evaluation_file WHERE sbid = '{sbid}'"

# Query template for RA, DEC, VSys from Vizier HIPASS catalog
RA_DEC_VSYS_QUERY_TEMPLATE = (
    'SELECT RAJ2000, DEJ2000, VSys FROM "J/AJ/128/16/table2" WHERE HIPASS LIKE \'{source_name}\''
)


def query_casda_visibility_files(source_identifier: str) -> Table:
    # Query CASDA visibility files by source identifier
    # https://astroquery.readthedocs.io/en/stable/api/astroquery.utils.tap.Tap.html
    query = VISIBILITY_QUERY_TEMPLATE.format(source_identifier=source_identifier)
    return casda_query(query, tap_url=CASDA_TAP_URL)


def query_sbid_evaluation(sbid: int) -> Table:
    query = SBID_EVALUATION_QUERY_TEMPLATE.format(sbid=str(sbid))
    return casda_query(query, tap_url=CASDA_TAP_URL)


def query_ra_dec_vsys(source_identifier: str) -> Optional[dict[str, Any]]:
    """Query Vizier TAP for RA, DEC, and VSys from HIPASS catalog.
    
    (tap_query_RA_DEC_VSYS)
    (usage in process_SOURCE_str)
    This is probably done in workflow rather than here, but just testing the TAP query.
    """
    # Extract the part after "HIPASS" if present
    if "HIPASS" in source_identifier:
        extracted_name = source_identifier[source_identifier.index("HIPASS") + len("HIPASS") :].strip()
    else:
        extracted_name = source_identifier.strip()

    query = RA_DEC_VSYS_QUERY_TEMPLATE.format(source_name=extracted_name)
    try:
        results = vizier_query(query, tap_url=VIZIER_TAP_URL)
        logger.info(f"RA/DEC/VSys query returned {len(results)} results")

        if len(results) == 0:
            logger.warning(f"No RA/DEC/VSys data found for {source_identifier}")
            return None

        ra_deg = results["RAJ2000"][0]
        dec_deg = results["DEJ2000"][0]
        vsys = results["VSys"][0]

        logger.info(f"Retrieved RA={ra_deg}, DEC={dec_deg}, VSys={vsys} for {source_identifier}")
        ra_h, ra_m, ra_s = degrees_to_hms(ra_deg)
        dec_d, dec_m, dec_s = degrees_to_dms(dec_deg)
        ra_s = round(ra_s, 2)
        dec_s = round(dec_s, 2)
        ra_string = f"{ra_h}h{ra_m}m{ra_s}s"
        dec_string = f"{dec_d}.{dec_m}.{dec_s}"

        logger.info(
            f"Converted RA={ra_h}h {ra_m}m {ra_s:.2f}s, "
            f"DEC={dec_d}° {dec_m} {dec_s:.2f}″ for {source_identifier}"
        )

        return {
            "ra_degrees": float(ra_deg),
            "dec_degrees": float(dec_deg),
            "vsys": float(vsys),
            "ra_string": ra_string,
            "dec_string": dec_string,
            "ra_hms": (ra_h, ra_m, ra_s),
            "dec_dms": (dec_d, dec_m, dec_s),
        }
    except Exception as e:
        logger.error(f"Error executing RA/DEC/VSys query: {e}")
        return None


def _extract_filename_from_url(url: str) -> Optional[str]:
    decoded_url = unquote(url)
    parsed = urlparse(decoded_url)
    query_params = parse_qs(parsed.query)
    response_disposition = query_params.get("response-content-disposition", [])
    for value in response_disposition:
        match = re.search(r'filename="?([^";]+)"?', value)
        if match:
            return match.group(1)
    filename = os.path.basename(parsed.path)
    return filename or None


def get_evaluation_file_for_sbid(sbid: int) -> Optional[str]:
    """Get the evaluation file for a given SBID (largest file by size).
    evaluation file selection logic in process_data)
    (tap_query_sbid_evaluation call)
    126-129 (finding largest file by filesize is what was done originally - check this)?
    """
    try:
        eval_results = query_sbid_evaluation(sbid)
        if len(eval_results) == 0:
            return None
        if "filename" in eval_results.colnames and "filesize" in eval_results.colnames:
            largest_file_row = eval_results[eval_results["filesize"].argmax()]
            return str(largest_file_row["filename"])
        elif "filename" in eval_results.colnames:
            return str(eval_results["filename"][0])
        else:
            return None
    except Exception as e:
        logger.warning(f"Error querying evaluation file for SBID {sbid}: {e}")
        return None


def prepare_metadata(
    source_identifier: str,
    query_results: Table,
    data_url_by_scan_id: Optional[dict[str, str]] = None,
    checksum_url_by_scan_id: Optional[dict[str, str]] = None,
    include_evaluation_files: bool = True,
    include_ra_dec_vsys: bool = True,
) -> list[dict[str, Any]]:
    metadata_list = []

    # Query RA/DEC/VSys from Vizier (once per source, not per dataset)
    ra_dec_vsys_data = None
    if include_ra_dec_vsys:
        logger.info(f"Querying RA/DEC/VSys for source: {source_identifier}")
        ra_dec_vsys_data = query_ra_dec_vsys(source_identifier)
        if ra_dec_vsys_data:
            logger.info(
                f"Source coordinates: RA={ra_dec_vsys_data['ra_string']}, "
                f"DEC={ra_dec_vsys_data['dec_string']}, VSys={ra_dec_vsys_data['vsys']}"
            )

    # Extract filenames from query results
    filenames = query_results["filename"] if "filename" in query_results.colnames else []

    # Extract other useful metadata if available
    obs_ids = query_results["obs_id"] if "obs_id" in query_results.colnames else []
    obs_publisher_dids = (
        query_results["obs_publisher_did"] if "obs_publisher_did" in query_results.colnames else []
    )
    scan_ids = [
        _extract_scan_id(str(obs_publisher_did)) if obs_publisher_did is not None else None
        for obs_publisher_did in obs_publisher_dids
    ]
    sbid_list = []
    for obs_id in obs_ids:
        # Extract SBID from obs_id (format: "ASKAP-12345")
        if isinstance(obs_id, str) and "ASKAP-" in obs_id:
            sbid = obs_id.replace("ASKAP-", "")
            sbid_list.append(int(sbid) if sbid.isdigit() else None)
        else:
            sbid_list.append(None)

    # Get evaluation files for each unique SBID (if requested)
    sbid_to_eval_file = {}
    if include_evaluation_files:
        unique_sbids = {sbid for sbid in sbid_list if sbid is not None}
        logger.info(f"Querying evaluation files for {len(unique_sbids)} unique SBIDs...")
        for sbid in unique_sbids:
            eval_file = get_evaluation_file_for_sbid(sbid)
            if eval_file:
                sbid_to_eval_file[sbid] = eval_file
                logger.info(f"SBID {sbid}: evaluation file = {eval_file}")
            else:
                logger.warning(f"No evaluation file found for SBID {sbid}")

    # Scan-id keyed matching from CASDA job results (preferred and deterministic).
    # if data_url_by_scan_id is not None, then we have a scan-id keyed mapping from the CASDA job results.
    if data_url_by_scan_id:
        missing_scan_ids = [sid for sid in scan_ids if sid and sid not in data_url_by_scan_id]
        if missing_scan_ids:
            logger.warning(f"Missing staged URLs for {len(missing_scan_ids)} scan ids")
    if checksum_url_by_scan_id:
        missing_scan_ids = [sid for sid in scan_ids if sid and sid not in checksum_url_by_scan_id]
        if missing_scan_ids:
            logger.warning(f"Missing checksum URLs for {len(missing_scan_ids)} scan ids")

    # Create metadata for each dataset
    for i, filename in enumerate(filenames):
        sbid = sbid_list[i] if i < len(sbid_list) else None
        obs_publisher_did = str(obs_publisher_dids[i]) if i < len(obs_publisher_dids) else None
        scan_id = _extract_scan_id(obs_publisher_did) if obs_publisher_did else None

        staged_url = data_url_by_scan_id.get(scan_id) if scan_id and data_url_by_scan_id else None
        checksum_url = (
            checksum_url_by_scan_id.get(scan_id) if scan_id and checksum_url_by_scan_id else None
        )
        if staged_url:
            staged_filename = _extract_filename_from_url(staged_url)
            if staged_filename and staged_filename != filename:
                logger.warning(
                    "Staged URL filename mismatch for scan id %s: expected %s, got %s",
                    scan_id,
                    filename,
                    staged_filename,
                )
        if checksum_url:
            checksum_filename = _extract_filename_from_url(checksum_url)
            if checksum_filename:
                base_checksum = checksum_filename.removesuffix(".checksum")
                if base_checksum != filename:
                    logger.warning(
                        "Checksum URL filename mismatch for scan id %s: expected %s, got %s",
                        scan_id,
                        filename,
                        checksum_filename,
                    )

        dataset_metadata = {
            "source_identifier": source_identifier,
            "dataset_id": filename,
            "visibility_filename": filename,
            "sbid": sbid,
            "obs_id": str(obs_ids[i]) if i < len(obs_ids) else None,
            "staged_url": staged_url,
            "checksum_url": checksum_url,
            "evaluation_file": sbid_to_eval_file.get(sbid) if sbid and include_evaluation_files else None,
        }

        # Add RA/DEC/VSys data if available (same for all datasets from same source)
        if ra_dec_vsys_data:
            dataset_metadata.update(
                {
                    "ra_degrees": ra_dec_vsys_data["ra_degrees"],
                    "dec_degrees": ra_dec_vsys_data["dec_degrees"],
                    "vsys": ra_dec_vsys_data["vsys"],
                    "ra_string": ra_dec_vsys_data["ra_string"],
                    "dec_string": ra_dec_vsys_data["dec_string"],
                }
            )

        # Add any additional columns from query results
        for colname in query_results.colnames:
            if colname not in ["filename", "obs_id"]:
                try:
                    dataset_metadata[colname.lower()] = query_results[colname][i]
                except (IndexError, KeyError):
                    pass

        metadata_list.append(dataset_metadata)

    return metadata_list


def main():
    """Test function to demonstrate CASDA TAP queries and staging."""
    import sys

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Test CASDA TAP queries and data staging for a source",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python test.py HIPASSJ1318-21 --username myuser --verbose
        """,
    )
    parser.add_argument(
        "source_identifier",
        type=str,
        help="Source identifier (e.g., HIPASSJ1318-21)",
    )
    parser.add_argument(
        "--username",
        type=str,
        required=False,
        help="CASDA username (required only if --stage is used)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--stage",
        action="store_true",
        help="Stage files for download (default: False, just query TAP and prepare metadata)",
    )
    parser.add_argument(
        "--no-eval",
        action="store_true",
        help="Skip evaluation file queries (default: False, queries evaluation files by SBID)",
    )
    parser.add_argument(
        "--no-coords",
        action="store_true",
        help="Skip RA/DEC/VSys queries from Vizier (default: False, queries coordinates)",
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize CASDA only if staging is requested
    casda = None
    if args.stage:
        if not args.username:
            logger.error("--username required when using --stage")
            sys.exit(1)
        try:
            username = args.username

            # Try the new API first (for astroquery >= 0.4+)
            try:
                casda = Casda()
                casda.USERNAME = username
                # New API login doesn't accept password directly as a constructor argument
                # It may use keychain file or prompt interactively
                try:
                    casda.login(username=username)
                    logger.info("CASDA client initialized)")
                except Exception as login_error:
                    logger.warning(f"API login failed ({login_error}), trying old API...")
                    raise login_error
            except Exception as new_api_error:
                # Fallback to the old API (for compatibility with og-funcs version and old astroquery)
                logger.warning(f"API login failed ({new_api_error}), make sure v4 astroquery is installed")
                raise new_api_error
            logger.info("CASDA client initialized for staging")
        except Exception as e:
            logger.error(f"Failed to initialize CASDA client: {e}")
            sys.exit(1)
    else:
        logger.info("Skipping CASDA initialization (staging disabled)")

    logger.info(f"\n{'='*60}")
    logger.info(f"Testing TAP query for source: {args.source_identifier}")
    if args.stage:
        logger.info("File staging: ENABLED (will poll for completion)")
    else:
        logger.info("File staging: DISABLED (just TAP query and metadata)")
    logger.info(f"{'='*60}\n")

    try:
        # Query CASDA for visibility files (TAP query - no authentication needed)
        query_results = query_casda_visibility_files(args.source_identifier)

        if len(query_results) == 0:
            logger.info(f"No datasets found for {args.source_identifier}")
            sys.exit(0)

        # Extract filenames (convert to list to avoid numpy array issues)
        filenames = list(query_results["filename"]) if "filename" in query_results.colnames else []
        logger.info(f"Found {len(filenames)} datasets in CASDA")

        new_filenames = filenames
        
        if len(new_filenames) == 0:
            logger.info(f"All datasets for {args.source_identifier} have already been processed")
            sys.exit(0)

        logger.info(f"Found {len(new_filenames)} new datasets")

        new_indices = [i for i, f in enumerate(filenames) if f in new_filenames]
        new_query_results = query_results[new_indices]

        # Stage the new datasets (if requested and casda is provided)
        data_url_by_scan_id: dict[str, str] = {}
        checksum_url_by_scan_id: dict[str, str] = {}
        if args.stage and casda:
            logger.info("Staging files (this will poll for completion)...")
            data_url_by_scan_id, checksum_url_by_scan_id = casda_stage_data(
                casda, new_query_results, verbose=True
            )
        else:
            logger.info("Skipping file staging (use --stage to enable)")

        # Prepare metadata from TAP query results
        metadata_list = prepare_metadata(
            args.source_identifier,
            new_query_results,
            data_url_by_scan_id=data_url_by_scan_id,
            checksum_url_by_scan_id=checksum_url_by_scan_id,
            include_evaluation_files=not args.no_eval,
            include_ra_dec_vsys=not args.no_coords,
        )

        if metadata_list:
            logger.info(f"\nFound {len(metadata_list)} new datasets:")
            for i, dataset in enumerate(metadata_list, 1):
                logger.info(f"\nDataset {i}:")
                logger.info(json.dumps(dataset, indent=2, default=str))
        else:
            logger.info("No new datasets found")

    except Exception as e:
        logger.error(f"Error during test: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

