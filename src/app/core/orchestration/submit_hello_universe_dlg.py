"""
  - dlg.dropmake.pg_generator: unroll(), partition(), resource_map()  (LG  PGT  PG)
  - dlg.common.reproducibility: init_pgt_unroll_repro_data(), init_pgt_partition_repro_data()
  - dlg.common: get_roots()
  - dlg.clients.DataIslandManagerClient: create_session(), append_graph(), deploy_session(),
    session_status(), graph_status(), nodes()

pip install daliuge-common daliuge-translator
See: https://daliuge.readthedocs.io/en/v5.6.1/installing.html#pip-install
     https://daliuge.readthedocs.io/en/v5.6.1/api-index.html
"""
import argparse
import json
import sys
import time

GRAPH_FILE = "test_graphs/HelloWorld-Universe.graph"
DIM_HOST = "dlg-dim.desk"
DIM_PORT = 8001


def main() -> int:
    p = argparse.ArgumentParser(description="Submit HelloUniverse via dlg Python API.")
    p.add_argument("--dim-host", default=DIM_HOST)
    p.add_argument("--dim-port", type=int, default=DIM_PORT)
    p.add_argument("--proxied", action="store_true", help="Use port 80 for DIM (behind proxy, like REST script)")
    p.add_argument("--algo", default="mysarkar", help="Partition algorithm: mysarkar (default, no native lib), metis, none, ...")
    p.add_argument("--no-wait", action="store_true")
    p.add_argument("--validate", action="store_true", help="Check graph file only")
    p.add_argument("--diagnose", action="store_true", help="Probe DIM (nodes, sessions)")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    if args.proxied:
        args.dim_port = 80

    if args.validate:
        try:
            with open(GRAPH_FILE) as f:
                g = json.load(f)
            print(f"Graph OK: {len(g.get('nodeDataArray', []))} nodes, {len(g.get('linkDataArray', []))} links")
        except FileNotFoundError:
            print(f"Not found: {GRAPH_FILE} (run from orchestration dir)", file=sys.stderr)
            return 1
        print(f"DIM: {args.dim_host}:{args.dim_port}")
        return 0

    # Import dlg modules (fail fast with a clear message if not installed)
    try:
        from dlg.dropmake import pg_generator
        from dlg.common.reproducibility.reproducibility import (
            init_pgt_unroll_repro_data,
            init_pgt_partition_repro_data,
        )
        from dlg.common import get_roots
        from dlg.clients import DataIslandManagerClient
    except ImportError as e:
        print(
            "DALiuGE Python packages required: pip install daliuge-common daliuge-translator",
            file=sys.stderr,
        )
        print(f"ImportError: {e}", file=sys.stderr)
        return 1

    if args.diagnose:
        try:
            client = DataIslandManagerClient(host=args.dim_host, port=args.dim_port)
            nodes = client.nodes()
            sessions = client.sessions()
            print(f"DIM nodes: {nodes}")
            print(f"DIM sessions: {sessions}")
        except Exception as e:
            print(f"DIM diagnose failed: {e}", file=sys.stderr)
            return 1
        return 0

    # Load logical graph (EAGLE/GoJS format; LG() and pg_generator accept it)
    try:
        with open(GRAPH_FILE) as f:
            lg_graph = json.load(f)
    except FileNotFoundError:
        print(f"Not found: {GRAPH_FILE} (run from orchestration dir)", file=sys.stderr)
        return 1

    # LG to PGT: unroll then partition (dlg.dropmake.pg_generator, dlg.common.reproducibility)
    try:
        pgt = pg_generator.unroll(lg_graph, zerorun=False)
        pgt = init_pgt_unroll_repro_data(pgt)
        reprodata = pgt.pop()
        pgt = pg_generator.partition(
            pgt,
            algo=args.algo,
            num_partitions=1,
            num_islands=0,
        )
        pgt.append(reprodata)
        pgt = init_pgt_partition_repro_data(pgt)
    except Exception as e:
        print(f"Translation (unroll/partition) failed: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    # Get node list from DIM; first element is DIM host for island, rest are NMs (resource_map convention)
    try:
        client = DataIslandManagerClient(host=args.dim_host, port=args.dim_port)
        nm_list = client.nodes()
        nodes = [args.dim_host] + list(nm_list)
    except Exception as e:
        print(f"DIM nodes() failed: {e}", file=sys.stderr)
        return 1

    if not nodes:
        print("DIM returned no nodes", file=sys.stderr)
        return 1

    # PGT to PG: map onto nodes (dlg.dropmake.pg_generator.resource_map)
    try:
        pg = pg_generator.resource_map(pgt, nodes, num_islands=1)
    except Exception as e:
        print(f"resource_map failed: {e}", file=sys.stderr)
        return 1

    # Roots for deploy (dlg.common.get_roots)
    drop_specs = [d for d in pg if isinstance(d, dict) and d.get("oid")]
    roots = list(get_roots(drop_specs))
    session_id = f"HelloUniverse_{time.strftime('%Y-%m-%dT%H-%M-%S')}"
    print(f"PG: {len(drop_specs)} nodes, {len(roots)} roots  session: {session_id}")

    # DIM: create_session, append_graph, deploy_session (dlg.clients.DataIslandManagerClient)
    try:
        client.create_session(session_id)
        client.append_graph(session_id, pg)
        client.deploy_session(session_id, completed_uids=roots)
    except Exception as e:
        print(f"DIM submit failed: {e}", file=sys.stderr)
        return 1
    print("Deployed.")

    if args.no_wait:
        print(f"Session: {args.dim_host}:{args.dim_port}  sessionId={session_id}")
        return 0

    # Poll session_status until finished (dlg.clients.session_status)
    while True:
        try:
            status = client.session_status(session_id)
        except Exception as e:
            print(f"session_status: {e}", file=sys.stderr)
            time.sleep(5)
            continue
        s = str(status)
        if "4" in s or "FINISHED" in s or "Finished" in s:
            print("Session finished.")
            try:
                gstatus = client.graph_status(session_id)
                if isinstance(gstatus, dict):
                    by_state = {}
                    for uid, st in gstatus.items():
                        key = st if isinstance(st, (str, int)) else (st.get("status", st.get("state", str(st))) if isinstance(st, dict) else str(st))
                        by_state[key] = by_state.get(key, 0) + 1
                    print(f"Graph status: {len(gstatus)} DROPs - > {by_state}")
                if args.verbose:
                    print(json.dumps(gstatus, indent=2))
            except Exception as e:
                print(f"(graph_status: {e})")
            return 0
        if "3" in s or "ERROR" in s or "Error" in s:
            print("Session error.", file=sys.stderr)
            print(json.dumps(status, indent=2), file=sys.stderr)
            return 1
        time.sleep(3)


if __name__ == "__main__":
    sys.exit(main())
