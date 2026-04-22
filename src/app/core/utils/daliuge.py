from typing import Any


def dim_rest_http_base(deploy_host: str, deploy_port: int) -> str:
    if deploy_port != 80:
        return f"http://{deploy_host}:{deploy_port}"
    return f"http://{deploy_host}"


def dim_graph_status_error_uids(drop_statuses: dict[str, Any]) -> list[str]:
    return [
        uid
        for uid, st in drop_statuses.items()
        if (isinstance(st, int) and st == 3) or (isinstance(st, str) and st.upper() == "ERROR")
    ]


def _links(links: Any) -> list[str]:
    """list of oids."""
    if isinstance(links, list):
        out: list[str] = []
        for x in links:
            out.extend(x.keys() if isinstance(x, dict) else (x if isinstance(x, list) else [x]))
        return out
    return list(links.keys()) if isinstance(links, dict) else []


def get_roots(pg_spec: list[dict]) -> set[str]:
    """(pulled from dlg/daliuge-common/dlg/common/__init__.py get_roots L219-266)."""
    all_oids: set[str] = set()
    nonroots: set[str] = set()
    for d in pg_spec:
        if not isinstance(d, dict) or "oid" not in d:
            continue
        oid = d["oid"]
        all_oids.add(oid)
        ct = d.get("categoryType") or d.get("type") or ""
        if ct in ("Application", "app", "Socket", "socket"):
            if d.get("inputs") or d.get("streamingInputs"):
                nonroots.add(oid)
            if d.get("outputs"):
                nonroots |= set(_links(d["outputs"]))
        elif ct in ("Data", "data"):
            if d.get("producers"):
                nonroots.add(oid)
            for k in ("consumers", "streamingConsumers"):
                if d.get(k):
                    nonroots |= set(_links(d[k]))
    return all_oids - nonroots


def classify_dim_session_status(status_payload: Any) -> str:
    if isinstance(status_payload, int):
        # DALiuGE SessionStates (dlg/manager/session.py):
        # PRISTINE=0, BUILDING=1, DEPLOYING=2, RUNNING=3, FINISHED=4, CANCELLED=5, FAILED=6
        if status_payload == 4:
            return "finished"
        if status_payload == 5:
            return "cancelled"
        if status_payload == 6:
            return "failed"
        return "running"

    if isinstance(status_payload, str):
        upper = status_payload.upper()
        if upper == "FINISHED":
            return "finished"
        if upper == "CANCELLED":
            return "cancelled"
        if upper in ("FAILED", "FAIL"):
            return "failed"
        if upper == "ERROR":
            return "failed"
        return "running"

    if isinstance(status_payload, dict):
        # Some DIM deployments return either:
        # - {"status": 4}
        # - {"status": "FINISHED"}
        # - {"dlg-nm1:...": 4, "dlg-nm2:...": 4}  (per-node status map)
        if "status" in status_payload:
            val = status_payload.get("status")
            if isinstance(val, (int, str)):
                return classify_dim_session_status(val)

        # Per-node status map: if any node reports failed => failed; if all finished => finished;
        # if all cancelled => cancelled; otherwise running.
        states: list[str] = []
        for v in status_payload.values():
            if isinstance(v, (int, str)):
                states.append(classify_dim_session_status(v))
        if states:
            if any(s == "failed" for s in states):
                return "failed"
            if all(s == "finished" for s in states):
                return "finished"
            if all(s == "cancelled" for s in states):
                return "cancelled"
            return "running"

    return "running"


__all__ = [
    "classify_dim_session_status",
    "dim_graph_status_error_uids",
    "dim_rest_http_base",
    "get_roots",
]

