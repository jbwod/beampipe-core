"""
  Translator:  POST /gen_pgt (form: lg) = GET /gen_pg?pgt_id&dlg_mgr_host&dlg_mgr_port  (TM calls DIM /api/nodes)
  DIM:         POST /api/sessions (body: sessionId) = POST /api/sessions/{id}/graph/append (body: PG) = POST /api/sessions/{id}/deploy (form: completed=roots)
  Poll:        GET /api/sessions/{id}/status  then  GET /api/sessions/{id}/graph/status

  Test with embedded manifest (beampipe-ingest + test-manifest.graph):
  cd src/app/core/orchestration && PYTHONPATH=../../.. python3 submit_hello_universe-no-dg.py --proxied --insecure

"""
import argparse
import json
import os
import sys
import time
from typing import Any
from urllib.parse import quote

import requests

#  (PYTHONPATH=../../.. from src/app/core/orchestration)
_ORCH_DIR = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.abspath(os.path.join(_ORCH_DIR, "..", "..", ".."))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from app.core.utils.daliuge import get_roots
from app.core.orchestration.translator_client import DaliugeTranslatorClient

GRAPH_FILE = "test_graphs/test-manifest.graph"
TM_URL = "http://dlg-tm.desk"
DIM_HOST = "dlg-dim.desk"
DIM_PORT = 8001



def main() -> int:
    if requests is None:
        print("pip install requests", file=sys.stderr)
        return 1

    p = argparse.ArgumentParser()
    p.add_argument("--tm-url", default=TM_URL)
    p.add_argument("--dim-host", default=DIM_HOST)
    p.add_argument("--dim-port", type=int, default=DIM_PORT)
    p.add_argument("--proxied", action="store_true", help="port 80, no :port in URL")
    p.add_argument("--dim-host-for-tm", default=None, help="DIM host as seen by translator (e.g. dlg-dim)")
    p.add_argument("--no-wait", action="store_true")
    p.add_argument("--insecure", action="store_true", help="skip TLS verify (for self-signed)")
    p.add_argument("--dim-port-for-tm", type=int, default=None, help="port for translator=DIM (default: 8001 when --proxied)")
    p.add_argument("--validate", action="store_true", help="check graph + config only")
    p.add_argument("--diagnose", action="store_true", help="probe TM + DIM")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    verify = not args.insecure
    if args.proxied:
        args.dim_port = 80
    dim_base = f"http://{args.dim_host}" if args.dim_port == 80 else f"http://{args.dim_host}:{args.dim_port}"
    # Translator (in Docker) needs DIM host as it sees it: default "dlg-dim" when proxied so container can resolve
    dim_host_tm = args.dim_host_for_tm or ("dlg-dim" if args.proxied else args.dim_host)
    # When proxied, script hits DIM via proxy:80; translator hits DIM container on 8001
    dim_port_tm = args.dim_port_for_tm if args.dim_port_for_tm is not None else (8001 if args.proxied else args.dim_port)

    if args.validate:
        try:
            with open(GRAPH_FILE) as f:
                g = json.load(f)
            print(f"Graph OK: {len(g.get('nodeDataArray', []))} nodes, {len(g.get('linkDataArray', []))} links")
        except FileNotFoundError:
            print(f"Not found: {GRAPH_FILE} (run from orchestration dir)", file=sys.stderr)
            return 1
        print(f"TM: {args.tm_url}  DIM: {dim_base}")
        return 0

    if args.diagnose:
        print("Diagnostics:")
        # REST: GET {tm_url}/  (Translator root)
        # REST: GET {dim_base}/api/nodes  = JSON list of node manager names
        # REST: GET {dim_base}/api/sessions  = session list / info
        for label, url in [
            ("Translator", f"{args.tm_url.rstrip('/')}/"),
            ("DIM nodes", f"{dim_base}/api/nodes"),
            ("DIM sessions", f"{dim_base}/api/sessions"),
        ]:
            try:
                r = requests.get(url, timeout=5, verify=verify)
                print(f"  {label}: {r.status_code}")
            except Exception as e:
                print(f"  {label}: FAIL {e}")
        print(f"  (gen_pg would use DIM at {dim_host_tm}:{dim_port_tm})")
        return 0

    try:
        with open(GRAPH_FILE) as f:
            graph_json = json.load(f)
    except FileNotFoundError:
        print(f"Not found: {GRAPH_FILE} (run from orchestration dir)", file=sys.stderr)
        return 1

    # Embed manifest in graph (beampipe-ingest receives inline JSON; no second file)
    # from app.core.orchestration.manifest import apply_manifest_to_lg, prepare_manifest_embed
    from app.core.orchestration.manifest import inject_manifest_config_into_graph

    test_manifest = {
        "run_context": {"source_identifier": "test-source", "run_uuid": "test-run-123-injection-test"},
    }
    # manifest_with_embed = prepare_manifest_embed(test_manifest)
    # apply_manifest_to_lg(graph_json, manifest_with_embed)
    # print("Applied embedded manifest to beampipe-ingest")
    inject_manifest_config_into_graph(graph_json, test_manifest)
    print("Injected manifest config into graph (beampipe-ingest)")

    run_suffix = time.strftime("%Y-%m-%dT%H-%M-%S")
    # Fixed lg_name so gen_pg finds the PGT (translator stores by base + "1_pgt.graph"; unique lg_name 404s).
    lg_name = os.path.basename(GRAPH_FILE)

    # Translator: LG = PGT, then gen_pg
    client = DaliugeTranslatorClient(base_url=args.tm_url, verify=verify, timeout=60.0)
    try:
        pgt_id = client.translate_lg_to_pgt(lg_name, graph_json, algo="metis", num_par=1, num_islands=0)
        pg_spec = client.translate_pgt_to_pg(
            pgt_id, dim_host_for_tm=dim_host_tm, dim_port_for_tm=dim_port_tm
        )
    finally:
        client.close()
    print(f"PGT: {pgt_id}")

    # Translator: PGT = PG (translator_rest.py gen_pg L554)
    # REST: GET {tm_url}/gen_pg?pgt_id=...&dlg_mgr_host=...&dlg_mgr_port=...
    #   Query: pgt_id (PGT id), dlg_mgr_host (DIM host for TM to call), dlg_mgr_port (DIM port)
    #   TM uses host:port to GET /api/nodes from DIM, then maps PGT to PG. Response: JSON PG spec (list).
    client = DaliugeTranslatorClient(base_url=args.tm_url, verify=verify, timeout=60.0)
    try:
        pg_spec = client.translate_pgt_to_pg(pgt_id, dim_host_for_tm=dim_host_tm, dim_port_for_tm=dim_port_tm)
    finally:
        client.close()

    if not isinstance(pg_spec, list) or len(pg_spec) == 0:
        print("Empty PG", file=sys.stderr)
        return 1
    drops = pg_spec[1:] if isinstance(pg_spec[0], str) else pg_spec
    specs = [x for x in drops if isinstance(x, dict) and x.get("oid")]
    roots = list(get_roots(specs))
    session_id = f"TestManifest_{run_suffix}"
    print(f"PG: {len(specs)} nodes, {len(roots)} roots  session: {session_id}")

    # DIM: create session, append PG, deploy (dlg/clients.py create_session L59, append_graph L81, deploy_session L68)
    # REST: POST {dim_base}/api/sessions
    #   Body: JSON {"sessionId": "<id>"}
    # REST: POST {dim_base}/api/sessions/{sessionId}/graph/append
    #   Body: JSON PG spec (list of DROP specs)
    # REST: POST {dim_base}/api/sessions/{sessionId}/deploy
    #   Body: application/x-www-form-urlencoded  completed=<comma-separated root OIDs>
    try:
        requests.post(f"{dim_base}/api/sessions", json={"sessionId": session_id}, timeout=30, verify=verify).raise_for_status()
        requests.post(f"{dim_base}/api/sessions/{quote(session_id)}/graph/append", json=pg_spec, timeout=60, verify=verify).raise_for_status()
        requests.post(f"{dim_base}/api/sessions/{quote(session_id)}/deploy", data={"completed": ",".join(roots)} if roots else None, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=30, verify=verify).raise_for_status()
    except Exception as e:
        print(f"DIM failed: {e}", file=sys.stderr)
        return 1
    print("Deployed.")

    if args.no_wait:
        print(f"{dim_base}/session?sessionId={session_id}")
        return 0

    # DIM: poll session status
    # REST: GET {dim_base}/api/sessions/{sessionId}/status  = JSON session status (e.g. 4 = FINISHED, 3 = ERROR)
    while True:
        try:
            r = requests.get(f"{dim_base}/api/sessions/{quote(session_id)}/status", timeout=10, verify=verify)
            r.raise_for_status()
            status = r.json()
        except Exception as e:
            print(f"status: {e}", file=sys.stderr)
            time.sleep(5)
            continue
        s = str(status)
        if "4" in s or "FINISHED" in s or "Finished" in s:
            print("Session finished.")
            # DIM: per-DROP status (dlg/clients.py graph_status)
            # REST: GET {dim_base}/api/sessions/{sessionId}/graph/status  = JSON dict { drop_uid: status, ... }
            try:
                r = requests.get(f"{dim_base}/api/sessions/{quote(session_id)}/graph/status", timeout=10, verify=verify)
                r.raise_for_status()
                graph_status = r.json()
                print(json.dumps(graph_status, indent=2))
            except Exception as e:
                print(f"(graph/status: {e})")
            return 0
        if "3" in s or "ERROR" in s or "Error" in s:
            print("Session error.", file=sys.stderr)
            print(json.dumps(status, indent=2), file=sys.stderr)
            return 1
        time.sleep(3)


if __name__ == "__main__":
    sys.exit(main())
