import json
import sys

from _paths import setup_sys_path

setup_sys_path()

from app.core.projects import load_project_module

metadata_by_source = {
    "HIPASSJ1318-21": [
        {
            "metadata_json": {
                "datasets": [
                    {
                        "sbid": 12345,
                        "dataset_id": "vis_12345_1",
                        "visibility_filename": "vis_12345_1.ms",
                        "ra_string": "11h11m11s",
                        "dec_string": "-11.11.11",
                        "vsys": 1234.5,
                        "evaluation_file": "SB11111_eval.tar",
                    },
                    {
                        "sbid": 12345,
                        "dataset_id": "vis_12345_1",
                        "visibility_filename": "vis_12345_1.ms",
                        "ra_string": "11h11m11s",
                        "dec_string": "-11.11.11",
                        "vsys": 1234.5,
                        "evaluation_file": "SB11111_eval.tar",
                    }
                ],
                "discovery_flags": {},
            }
        }
    ],
    "HIPASSJ1318-22": [
        {
            "metadata_json": {
                "datasets": [
                    {
                        "sbid": 12345,
                        "dataset_id": "vis_12345_1",
                        "visibility_filename": "vis_12345_1.ms",
                        "ra_string": "11h11m11s",
                        "dec_string": "-11.11.11",
                        "vsys": 1234.5,
                        "evaluation_file": "SB11111_eval.tar",
                    }
                ],
                "discovery_flags": {},
            }
        }
    ]
}

# Simulated staged URLs (CASDA returns keys like "105172" from scan-105172-254939)
staged_urls_by_scan_id = {
    "12345": "https://data.csiro.au/casda/.../HIPASSJ1318-21_A_beam25_10arc_split.ms.tar",
    "12345": "https://data.csiro.au/casda/.../HIPASSJ1318-21_A_beam26_10arc_split.ms.tar",
}
eval_urls_by_sbid = {
    "12345": "https://data.csiro.au/casda/.../calibration-metadata-processing-logs-SB34166_2021-12-31-011733.tar",
    "12345": "https://data.csiro.au/casda/.../calibration-metadata-processing-logs-SB34275_2022-01-06-132301.tar",
}

mod = load_project_module("wallaby_hires")
fn = getattr(mod, "manifest", None) or getattr(mod, "build_manifest_sources", None)
if not fn:
    print("ERROR: wallaby_hires has no manifest or build_manifest_sources")
    sys.exit(1)

sources = fn(
    metadata_by_source,
    staged_urls_by_scan_id=staged_urls_by_scan_id,
    eval_urls_by_sbid=eval_urls_by_sbid,
)

print("Manifest sources output:")
print(json.dumps(sources, indent=2))
