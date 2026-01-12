"""
Test script for CASDA TAP queries and data staging.
Monitoring loop will eventually do automatically.
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
from typing import Any, Optional

from astropy.table import Table
from astroquery.casda import Casda
from astroquery.utils.tap.core import TapPlus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CASDA TAP endpoint
CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"

# Vizier TAP endpoint
VIZIER_TAP_URL = "http://tapvizier.cds.unistra.fr/TAPVizieR/tap"

# Query template for vis files
VISIBILITY_QUERY_TEMPLATE = "SELECT * FROM ivoa.obscore WHERE filename LIKE '{source_identifier}%'"

# Query template for finding eval files by SBID
SBID_EVALUATION_QUERY_TEMPLATE = "SELECT * FROM casda.observation_evaluation_file WHERE sbid = '{sbid}'"

# Query template for RA, DEC, VSys from Vizier HIPASS catalog
RA_DEC_VSYS_QUERY_TEMPLATE = 'SELECT RAJ2000, DEJ2000, VSys FROM "J/AJ/128/16/table2" WHERE HIPASS LIKE \'{source_name}\''

def query_casda_visibility_files(source_identifier: str):
    # Query CASDA visibility files by source identifier
    # https://astroquery.readthedocs.io/en/stable/api/astroquery.utils.tap.Tap.html
    pass

def query_sbid_evaluation(sbid: int):
    # Query CASDA for evaluation files associated with an SBID
    pass

def degrees_to_hms(degrees: float):
    """Convert RA given in degrees to hours-minutes-seconds.
    """
    pass

def degrees_to_dms(degrees: float):
    """Convert DEC given in degrees to degrees-minutes-seconds.
    """
    pass

def query_ra_dec_vsys(source_identifier: str):
    """Query Vizier TAP for RA, DEC, and VSys from HIPASS catalog.
    
    (tap_query_RA_DEC_VSYS)
    (usage in process_SOURCE_str)
    This is probably done in workflow rather than here, but just testing the TAP query.
    """
    # Extract the part after "HIPASS" if present
    pass

def stage_data(casda, query_results, verbose=True):

    pass

def get_evaluation_file_for_sbid(sbid: int):
    """Get the evaluation file for a given SBID (largest file by size).
    evaluation file selection logic in process_data)
    (tap_query_sbid_evaluation call)
    126-129 (finding largest file by filesize)?
    """
    pass

def prepare_metadata(
    source_identifier: str,
    query_results,
    staged_urls=None,
    checksum_urls=None,
    include_evaluation_files=True,
    include_ra_dec_vsys=True,
):

    pass

def main():
    # Parse command-line arguments
    # Set logging level
    # Initialize CASDA only if staging is requested
    # Query CASDA for visibility files
    # Handle staging, metadata, and logging
    pass

if __name__ == "__main__":
    main()

