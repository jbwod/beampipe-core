# from __future__ import annotations # really should use the proper runtime...

import json
import tempfile
from typing import Any, Dict


ManifestDict = Dict[str, Any]

# use beampipe-ingest from the beampipe.palette
def prepare_manifest_embed(manifest: ManifestDict) -> ManifestDict:
    """Return a copy of `manifest` with graph_overrides set for beampipe-ingest.

    The returned dict contains a `graph_overrides` mapping such that the
    `beampipe-ingest` node receives the entire manifest (minus any existing
    graph_overrides) as an inline JSON string via its `manifest_path` field.

    This might be slightly 'hacky', but should be the same as the dlg fill behaviour"""
    data: ManifestDict = dict(manifest)
    overrides: Dict[str, Dict[str, Any]] = dict(data.get("graph_overrides") or {})

    # Embed the manifest content
    embed_content = {k: v for k, v in data.items() if k != "graph_overrides"}
    overrides["beampipe-ingest"] = {
        "manifest_path": json.dumps(embed_content, separators=(",", ":")),
    }

    data["graph_overrides"] = overrides
    return data


def _set_node_field_value(field: Dict[str, Any], value: Any) -> None:
    if isinstance(value, (list, dict)) and field.get("type") == "Array":
        field["value"] = value
    else:
        field["value"] = value


def apply_manifest_to_lg(
    lg_dict: ManifestDict,
    manifest_or_overrides: ManifestDict,
) -> ManifestDict:
    """
    lg_dict:
        logical graph
    manifest_or_overrides:
        Either:
        - a dict containing a \"graph_overrides\" key, or
        - a dict directly in the shape {node_key: {field_name: value}}.
    Returns the same lg_dict (modified in place).
    """
    nodes = lg_dict.get("nodeDataArray")
    if not isinstance(nodes, list):
        return lg_dict

    if "graph_overrides" in manifest_or_overrides:
        overrides = manifest_or_overrides.get("graph_overrides") or {}
    else:
        overrides = manifest_or_overrides

    if not isinstance(overrides, dict):
        return lg_dict

    for node in nodes:
        if not isinstance(node, dict):
            continue
        nid = node.get("id")
        name = node.get("name")
        node_key = None
        if nid is not None and str(nid) in overrides:
            node_key = str(nid)
        elif name and str(name) in overrides:
            node_key = str(name)
        if not node_key:
            continue

        for field in node.get("fields") or []:
            if not isinstance(field, dict):
                continue
            fname = field.get("name")
            if fname in overrides[node_key]:
                _set_node_field_value(field, overrides[node_key][fname])

    return lg_dict


def write_manifest_to_temp(
    manifest: ManifestDict,
    prefix: str = "beampipe_manifest_",
    suffix: str = ".json",
) -> str:
    """Write a manifest dict to a temp JSON file and return its path."""
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix)
    with open(fd, "w") as f:
        json.dump(manifest, f, indent=2)
    return path


def write_filled_lg_to_temp(lg_dict: ManifestDict, suffix: str = ".graph") -> str:
    """Write logical graph dict to a temp file. Returns path."""
    fd, path = tempfile.mkstemp(suffix=suffix, prefix="dlg_lg_")
    with open(fd, "w") as f:
        json.dump(lg_dict, f, indent=2)
    return path


def resolve_lg_with_manifest(
    lg_path: str,
    *,
    manifest_path: str | None = None,
    manifest_dict: ManifestDict | None = None,
) -> str:
    """Load LG from path; if manifest provided, apply it and return path to filled LG.

    Otherwise returns `lg_path` unchanged. Caller can use returned path with
    DALiuGE translator/submit commands.
    """
    with open(lg_path) as f:
        lg_dict = json.load(f)
    if manifest_path:
        with open(manifest_path) as mf:
            manifest_dict = json.load(mf)
    if manifest_dict is not None:
        apply_manifest_to_lg(lg_dict, manifest_dict)
        return write_filled_lg_to_temp(lg_dict)
    return lg_path