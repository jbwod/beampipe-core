import hashlib
import json
from typing import Any

def _to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(x) for x in value]
    return value

def _stable_json_dumps(value: Any) -> str:
    return json.dumps(_to_jsonable(value), sort_keys=True, separators=(",", ":"))

def discovery_signature(payload_by_sbid: dict[str, Any]) -> str:
    raw = _stable_json_dumps(payload_by_sbid)
    return hashlib.sha256(raw.encode()).hexdigest()

# Same content, different key order and one tuple
p1 = {"34166": {"datasets": [{"sbid": "34166", "dataset_id": "d1", "visibility_filename": "v.fits"}]}}
p2 = {"34166": {"datasets": ({"visibility_filename": "v.fits", "sbid": "34166", "dataset_id": "d1"},)}}

canonical_1 = _stable_json_dumps(p1)
canonical_2 = _stable_json_dumps(p2)

print("p1:", p1)
print("p2:", p2)
print("Canonical JSON (p1):", canonical_1)
print("Canonical JSON (p2):", canonical_2)
print("Same canonical string:", canonical_1 == canonical_2)
print("discovery_signature(p1):", discovery_signature(p1))
print("discovery_signature(p2):", discovery_signature(p2))
print("Same signature:", discovery_signature(p1) == discovery_signature(p2))