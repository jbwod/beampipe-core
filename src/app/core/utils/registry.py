"""Registry-related utilities: source validation, etc."""

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..registry.service import source_registry_service


async def validate_source_spec(
    db: AsyncSession,
    project_module: str,
    spec: dict | Any,
) -> tuple[str | None, dict | None, str | None]:

    sid = spec.get("source_identifier") if isinstance(spec, dict) else getattr(spec, "source_identifier", None)
    if not sid:
        return None, None, "Source spec missing source_identifier"

    registered = await source_registry_service.check_existing_source(
        db, project_module, sid
    )
    if not registered:
        return None, None, f"Source {sid} is not registered"
    if not registered.get("enabled", False):
        return None, None, f"Source {sid} is disabled"

    return sid, registered, None
