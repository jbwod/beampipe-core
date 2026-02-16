from app.core.archive.adapters import (
    get_adapter,
    get_health_endpoints,
    list_adapter_names,
    query_adapter,
)


def main() -> None:
    errors = []

    # list_adapter_names
    names = list_adapter_names()
    if not isinstance(names, list):
        errors.append(f"list_adapter_names() did not return a list: {type(names)}")
    if "casda" not in names or "vizier" not in names:
        errors.append(f"expected casda and vizier in {names}")

    # get_adapter
    casda = get_adapter("casda")
    if casda is None:
        errors.append("get_adapter('casda') returned None")
    elif not (hasattr(casda, "query") and hasattr(casda, "tap_url") and hasattr(casda, "health_url")):
        errors.append("casda adapter missing query/tap_url/health_url")

    vizier = get_adapter("vizier")
    if vizier is None:
        errors.append("get_adapter('vizier') returned None")
    elif not (hasattr(vizier, "query") and hasattr(vizier, "tap_url")):
        errors.append("vizier adapter missing query/tap_url")

    if get_adapter("nonexistent") is not None:
        errors.append("get_adapter('nonexistent') should return None")

    # get_health_endpoints
    endpoints = get_health_endpoints()
    if len(endpoints) != len(names):
        errors.append(f"get_health_endpoints length {len(endpoints)} != list_adapter_names {len(names)}")
    for (name, url) in endpoints:
        if not isinstance(name, str) or not isinstance(url, str) or name not in names:
            errors.append(f"bad endpoint tuple: {(name, url)}")

    # query_adapter
    try:
        query_adapter("nonexistent", "SELECT 1")
        errors.append("query_adapter('nonexistent', ...) should raise ValueError")
    except ValueError as e:
        if "nonexistent" not in str(e) and "not found" not in str(e).lower():
            errors.append(f"unexpected ValueError message: {e}")

    if errors:
        for e in errors:
            print("FAIL:", e)
        raise SystemExit(1)
    print("OK: adapter registry and API checks passed")


if __name__ == "__main__":
    main()
