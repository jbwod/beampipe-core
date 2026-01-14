"""Source registry service.

Provides source registration and cataloging functionality.
"""
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_source_registry import crud_source_registry
from ...models.registry import SourceRegistry
from ...schemas.registry import SourceRegistryCreateInternal, SourceRegistryRead
from ..exceptions.http_exceptions import NotFoundException

logger = logging.getLogger(__name__)


class SourceRegistryService:
    @staticmethod
    async def check_existing_source(
        db: AsyncSession,
        project_module: str,
        source_identifier: str,
    ) -> SourceRegistry | None:
        """Check if a source already exists for the given key."""
        try:
            source = await crud_source_registry.get(
                db=db,
                project_module=project_module,
                source_identifier=source_identifier,
                schema_to_select=SourceRegistryRead,
            )
            return source
        except Exception as e:
            logger.exception(f"Error checking existing source for {project_module}/{source_identifier}: {e}")
            return None

    @staticmethod
    async def register_source(
        db: AsyncSession,
        project_module: str,
        source_identifier: str,
        enabled: bool = False,
    ) -> SourceRegistry:
        """Register a new source or return existing (idempotent)."""
        existing = await crud_source_registry.get(
            db=db,
            project_module=project_module,
            source_identifier=source_identifier,
            schema_to_select=SourceRegistryRead,
        )
        if existing:
            return existing
        source_data = SourceRegistryCreateInternal(
            project_module=project_module,
            source_identifier=source_identifier,
            enabled=enabled,
        )
        source = await crud_source_registry.create(
            db=db,
            object=source_data,
            schema_to_select=SourceRegistryRead,
        )
        return source

    @staticmethod
    async def get_source(
        db: AsyncSession,
        source_id: UUID,
    ) -> SourceRegistry:
        """Get a single source by UUID."""
        source = await crud_source_registry.get(
            db=db,
            uuid=source_id,
            schema_to_select=SourceRegistryRead,
        )
        if not source:
            raise NotFoundException(f"Source with id {source_id} not found")
        return source

    @staticmethod
    async def update_source(
        db: AsyncSession,
        source_id: UUID,
        enabled: bool | None = None,
    ) -> SourceRegistry:
        """Update source metadata."""
        source = await crud_source_registry.get(
            db=db,
            uuid=source_id,
            schema_to_select=SourceRegistryRead,
        )
        if not source:
            raise NotFoundException(f"Source with id {source_id} not found")
        
        update_data: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        if enabled is not None:
            update_data["enabled"] = enabled
        
        await crud_source_registry.update(
            db=db,
            object=update_data,
            uuid=source_id,
        )
        
        updated_source = await crud_source_registry.get(
            db=db,
            uuid=source_id,
            schema_to_select=SourceRegistryRead,
        )
        if not updated_source:
            raise NotFoundException(f"Source with id {source_id} not found after update")
        return updated_source

    @staticmethod
    async def get_enabled_sources(
        db: AsyncSession,
        project_module: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get all enabled sources, optionally filtered by project module.
        
        Returns:
            List of source dictionaries (SourceRegistryRead schema)
        """
        filters: dict[str, Any] = {"enabled": True}
        if project_module:
            filters["project_module"] = project_module
        
        sources_data = await crud_source_registry.get_multi(
            db=db,
            schema_to_select=SourceRegistryRead,
            **filters,
        )
        return sources_data.get("items", [])


# instance
source_registry_service = SourceRegistryService()
