from typing import Any


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


__all__ = ["get_roots"]

