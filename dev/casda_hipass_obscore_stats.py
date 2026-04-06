import sys
from pathlib import Path

from astroquery.utils.tap.core import TapPlus

CASDA_TAP_SYNC = "https://casda.csiro.au/casda_vo_tools/tap/sync"
SID_EXPR = "SUBSTR(filename, 1, STRPOS(filename, '_')-1)"

def main() -> int:

    out_csv = Path("hipass_casda_obscore_sources.csv")
    try:
        tap = TapPlus(url=CASDA_TAP_SYNC)
        query = f"""
            SELECT DISTINCT {SID_EXPR} AS source_identifier
            FROM ivoa.obscore
            WHERE filename LIKE 'HIPASSJ%'
            ORDER BY 1
        """
        job = tap.launch_job(query, output_format="votable", verbose=False)
        table = job.get_results()
        # Accept both string and bytes, and filter out any None entries — robust extraction
        sids = []
        for sid in table["source_identifier"]:
            if isinstance(sid, str):
                val = sid.strip()
                if val:
                    sids.append(val)
            elif isinstance(sid, bytes):  # Just in case
                val = sid.decode("utf-8").strip()
                if val:
                    sids.append(val)
        content = "source_identifier\n" + "\n".join(sids) + ("\n" if sids else "")
        out_csv.write_text(content, encoding="utf-8")
        print(f"Wrote {out_csv} with {len(sids)} source identifiers")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
