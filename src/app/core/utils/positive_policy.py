"""Positive numeric reads from optional policy dicts (module automation, Restate overrides)."""

from typing import Any


def is_positive_int(value: Any) -> int | None:
    try:
        v = int(value)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def is_positive_float(value: Any) -> float | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def positive_int_optional(policy: dict[str, Any] | None, key: str) -> int | None:
    if not policy or key not in policy:
        return None
    return is_positive_int(policy[key])


def positive_int(policy: dict[str, Any] | None, key: str, default: int) -> int:
    v = positive_int_optional(policy, key)
    return int(default) if v is None else v


def positive_float_optional(policy: dict[str, Any] | None, key: str) -> float | None:
    if not policy or key not in policy:
        return None
    return is_positive_float(policy[key])


def positive_float(policy: dict[str, Any] | None, key: str, default: float) -> float:
    v = positive_float_optional(policy, key)
    d = float(default)
    return d if v is None else v
