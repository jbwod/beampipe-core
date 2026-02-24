"""Source registry service.

Provides source registration and cataloging functionality.
"""
import logging
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import and_, exists, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_source_registry import crud_source_registry
from ...models.archive import ArchiveMetadata
from ...models.registry import SourceRegistry
from ...schemas.registry import SourceRegistryCreateInternal, SourceRegistryRead
from ..config import settings
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
            logger.exception(
                "event=registry_check_source_error "
                "project_module=%s source_identifier=%s error=%s",
                project_module,
                source_identifier,
                e,
            )
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
        stale_after_hours: int | None = None,
        *,
        update_stale_after_hours: bool = False,
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
        if update_stale_after_hours:
            update_data["stale_after_hours"] = stale_after_hours

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
        default_stale = stale_after_hours if stale_after_hours is not None else settings.DISCOVERY_STALE_HOURS
        #  something like last_checked_at < (now - COALESCE(stale_after_hours, default) hours)
        stale_by_time = and_(
            SourceRegistry.last_checked_at.isnot(None),
            text(
                "source_registry.last_checked_at < (now() AT TIME ZONE 'UTC') "
                "- ((CASE WHEN source_registry.stale_after_hours IS NOT NULL THEN source_registry.stale_after_hours ELSE :default_stale END) * interval '1 hour')"
            ).bindparams(default_stale=default_stale),
        )
        conditions = [SourceRegistry.last_checked_at.is_(None), ~metadata_exists, stale_by_time]
        query = select(SourceRegistry).where(SourceRegistry.enabled.is_(True))
        if project_module:
            query = query.where(SourceRegistry.project_module == project_module)
        retry_cooldown_minutes = max(0, settings.DISCOVERY_RETRY_COOLDOWN_MINUTES)
        if retry_cooldown_minutes > 0:
            cooldown_ok = or_(
                SourceRegistry.last_attempted_at.is_(None),
                text(
                    "source_registry.last_attempted_at < (now() AT TIME ZONE 'UTC') "
                    "- (:cooldown_minutes * interval '1 minute')"
                ).bindparams(cooldown_minutes=retry_cooldown_minutes),
            )
            query = query.where(cooldown_ok)
        query = query.where(or_(*conditions)).order_by(SourceRegistry.created_at.asc())
        if limit:
            query = query.limit(limit)

        result = await db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    async def mark_sources_checked(
        db: AsyncSession,
        source_ids: list[UUID],
        checked_at: datetime | None = None,
        *,
        commit: bool = True,
    ) -> None:
        """Update last_checked_at for a batch of sources."""
        if not source_ids:
            return
        logger.debug(
            "event=registry_mark_checked count=%s",
            len(source_ids),
        )
        await db.execute(
            update(SourceRegistry)
            .where(SourceRegistry.uuid.in_(source_ids))
            .values(last_checked_at=checked_at or datetime.now(UTC))
        )
        if commit:
            await db.commit()

    @staticmethod
    async def mark_sources_attempted(
        db: AsyncSession,
        project_module: str,
        source_identifiers: list[str],
        attempted_at: datetime | None = None,
        *,
        commit: bool = True,
    ) -> None:
        """Update last_attempted_at for failed source attempts."""
        if not source_identifiers:
            return
        logger.debug(
            "event=registry_mark_attempted project_module=%s count=%s",
            project_module,
            len(source_identifiers),
        )
        await db.execute(
            update(SourceRegistry)
            .where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.source_identifier.in_(source_identifiers),
            )
            .values(last_attempted_at=attempted_at or datetime.now(UTC))
        )
        if commit:
            await db.commit()

    @staticmethod
    async def mark_sources_for_rediscovery(
        db: AsyncSession,
        project_module: str,
        source_identifiers: list[str] | None = None,
    ) -> list[str]:
        conditions = [
            SourceRegistry.project_module == project_module,
            SourceRegistry.enabled.is_(True),
        ]
        if source_identifiers:
            conditions.append(SourceRegistry.source_identifier.in_(source_identifiers))
        stmt = (
            update(SourceRegistry)
            .where(and_(*conditions))
            .values(last_checked_at=None, last_attempted_at=None)
            .returning(SourceRegistry.source_identifier)
        )
        result = await db.execute(stmt)
        identifiers = [row[0] for row in result.all()]
        await db.commit()
        logger.debug(
            "event=registry_mark_for_rediscovery project_module=%s count=%s",
            project_module,
            len(identifiers),
        )
        return identifiers

    @staticmethod
    async def update_source_discovery_state(
        db: AsyncSession,
        source_id: UUID,
        *,
        checked_at: datetime | None = None,
        attempted_at: datetime | None = None,
        discovery_signature: str | None = None,
    ) -> None:
        """Update discovery-specific source state without committing."""
        update_data: dict[str, Any] = {}
        if checked_at is not None:
            update_data["last_checked_at"] = checked_at
        if attempted_at is not None:
            update_data["last_attempted_at"] = attempted_at
        if discovery_signature is not None:
            update_data["discovery_signature"] = discovery_signature
            logger.debug(
                "event=registry_discovery_signature_update source_id=%s discovery_signature=%s",
                source_id,
                discovery_signature,
            )
        if not update_data:
            return
        await db.execute(
            update(SourceRegistry)
            .where(SourceRegistry.uuid == source_id)
            .values(**update_data)
        )


# instance
source_registry_service = SourceRegistryService()
