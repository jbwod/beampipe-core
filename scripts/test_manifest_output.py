import json

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

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
    ]
}

staged_urls = {"12345": "https://data.example.com/vis_12345_1.ms.tar"}
eval_urls = {"12345": "https://data.example.com/SB32736_eval.tar"}

mod = load_project_module("wallaby_hires")
fn = getattr(mod, "manifest", None) or getattr(mod, "build_manifest_sources", None)
if not fn:
    print("ERROR: wallaby_hires has no manifest or build_manifest_sources")
    sys.exit(1)

sources = fn(
    metadata_by_source,
    staged_urls_by_scan_id=staged_urls,
    eval_urls_by_sbid=eval_urls,
)

print("Manifest sources output:")
print(json.dumps(sources, indent=2))
