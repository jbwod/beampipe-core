from astropy.table import Table
from astroquery.utils.tap.core import TapPlus
# https://astroquery.readthedocs.io/en/latest/casda/casda.html
# https://github.com/ICRAR/wallaby-hires/blob/main/wallaby_hires/funcs.py

CASDA_TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"
VIZIER_TAP_URL = "http://tapvizier.cds.unistra.fr/TAPVizieR/tap"

source = "HIPASSJ1303+07"

casdatap = TapPlus(url=CASDA_TAP_URL, verbose=False)
query = f"SELECT * FROM ivoa.obscore WHERE filename LIKE '{source}%'"
job = casdatap.launch_job_async(query)
results = job.get_results()
print(f"Visibility files: {len(results)}")
print(results)

sbid = 33681
query_eval = f"SELECT * FROM casda.observation_evaluation_file WHERE sbid = '{sbid}'"
job_eval = casdatap.launch_job_async(query_eval)
results_eval = job_eval.get_results()
print(f"\nEvaluation files for SBID {sbid}: {len(results_eval)}")
print(results_eval)

source_name = source.replace("HIPASS", "").strip()
viziertap = TapPlus(url=VIZIER_TAP_URL, verbose=False)
query_coords = f'SELECT RAJ2000, DEJ2000, VSys FROM "J/AJ/128/16/table2" WHERE HIPASS LIKE \'{source_name}\''
job_coords = viziertap.launch_job_async(query_coords)
results_coords = job_coords.get_results()
print(f"\nCoordinates: {len(results_coords)}")
print(results_coords)