"""Source registry service.

Provides source registration and cataloging functionality.
"""
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

from sqlalchemy import and_, exists, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_source_registry import crud_source_registry
from ...models.archive import ArchiveMetadata
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
    ) -> dict[str, Any] | None:
        """Check if a source already exists for the given key.

        Args:
            db: Database session
            project_module: Project module identifier
            source_identifier: Source identifier

        Returns:
            Source registry entry if found, None otherwise
        """
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
    ) -> dict[str, Any]:
        """Register a new source or return existing (idempotent).

        If a source with the same project_module and source_identifier
        already exists, returns the existing source. Otherwise, creates
        a new source registry entry.

        Args:
            db: Database session
            project_module: Project module identifier
            source_identifier: Source identifier (e.g., "HIPASSJ1318-21")
            enabled: Whether the source is enabled for monitoring (default: False)

        Returns:
            Source registry entry (existing or newly created)
        """
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
        # fire off a source discovery job
        return source

    @staticmethod
    async def get_source(
        db: AsyncSession,
        source_id: UUID,
    ) -> dict[str, Any]:
        """Get a single source by UUID.

        Args:
            db: Database session
            source_id: Source UUID

        Returns:
            Source registry entry

        Raises:
            NotFoundException: If source not found
        """
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
    ) -> dict[str, Any]:
        """Update source metadata.

        Currently supports updating the enabled status. Updates the
        updated_at timestamp automatically.

        Args:
            db: Database session
            source_id: Source UUID
            enabled: New enabled status (None to leave unchanged)

        Returns:
            Updated source registry entry

        Raises:
            NotFoundException: If source not found
        """
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

        Args:
            db: Database session
            project_module: Optional project module filter (e.g., "wallaby")

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
        return cast(list[dict[str, Any]], sources_data.get("items", []))

    @staticmethod
    async def get_sources_for_discovery(
        db: AsyncSession,
        project_module: str | None = None,
        stale_after_hours: int | None = None,
        limit: int | None = None,
    ) -> list[SourceRegistry]:
        metadata_exists = exists(
            select(1).where(
                and_(
                    ArchiveMetadata.project_module == SourceRegistry.project_module,
                    ArchiveMetadata.source_identifier == SourceRegistry.source_identifier,
                )
            )
        )
        query = select(SourceRegistry).where(SourceRegistry.enabled.is_(True))
        if project_module:
            query = query.where(SourceRegistry.project_module == project_module)

        conditions = [SourceRegistry.last_checked_at.is_(None), ~metadata_exists]
        if stale_after_hours is not None:
            cutoff = datetime.now(UTC) - timedelta(hours=stale_after_hours)
            conditions.append(SourceRegistry.last_checked_at < cutoff)

        query = query.where(or_(*conditions)).order_by(SourceRegistry.created_at.asc())
        if limit:
            query = query.limit(limit)

        result = await db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    async def mark_sources_checked(
        db: AsyncSession, source_ids: list[UUID], checked_at: datetime | None = None
    ) -> None:
        """Update last_checked_at for a batch of sources."""
        if not source_ids:
            return
        await db.execute(
            update(SourceRegistry)
            .where(SourceRegistry.uuid.in_(source_ids))
            .values(last_checked_at=checked_at or datetime.now(UTC))
        )
        await db.commit()


# instance
source_registry_service = SourceRegistryService()
