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
        if status_payload == 4:
            return "finished"
        if status_payload == 3:
            return "error"
        return "running"

    if isinstance(status_payload, str):
        upper = status_payload.upper()
        if upper == "FINISHED":
            return "finished"
        if upper == "ERROR":
            return "error"
        return "running"

    if isinstance(status_payload, dict):
        val = status_payload.get("status", status_payload)
        if isinstance(val, (int, str)) and val != status_payload:
            return classify_dim_session_status(val)

    return "running"


__all__ = [
    "classify_dim_session_status",
    "dim_graph_status_error_uids",
    "dim_rest_http_base",
    "get_roots",
]

