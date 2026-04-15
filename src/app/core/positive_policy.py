from typing import Annotated, Any, cast

from pydantic import Field, TypeAdapter, ValidationError

_PositiveInt: TypeAdapter[int] = TypeAdapter(Annotated[int, Field(gt=0)])
_PositiveFloat: TypeAdapter[float] = TypeAdapter(Annotated[float, Field(gt=0)])


def is_positive_int(value: Any) -> int | None:
    try:
        return cast(int, _PositiveInt.validate_python(value))
    except ValidationError:
        return None


def is_positive_float(value: Any) -> float | None:
    try:
        return cast(float, _PositiveFloat.validate_python(value))
    except ValidationError:
        return None


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
