"""Source registry service.

Provides source registration and cataloging functionality.
"""
import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import and_, exists, func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_source_registry import crud_source_registry
from ...models.archive import ArchiveMetadata
from ...models.registry import SourceRegistry
from ...schemas.registry import SourceRegistryCreateInternal, SourceRegistryRead
from ..config import settings
from ..exceptions.http_exceptions import NotFoundException
from ..projects import list_project_modules

logger = logging.getLogger(__name__)


def invalid_project_module_message(
    project_module: str, available_modules: Sequence[str] | None = None
) -> str:
    if available_modules is None:
        available_modules = list_project_modules()
    return f"Project module '{project_module}' not found. Available: {available_modules}"


def _validate_project_module(project_module: str) -> None:
    available_modules = list_project_modules()
    if project_module not in available_modules:
        raise ValueError(invalid_project_module_message(project_module, available_modules))


def _metadata_exists_clause() -> Any:
    return exists(
        select(1).where(
            and_(
                ArchiveMetadata.project_module == SourceRegistry.project_module,
                ArchiveMetadata.source_identifier == SourceRegistry.source_identifier,
            )
        )
    )


def _discovery_eligibility_conditions(
    *,
    project_module: str | None = None,
    stale_after_hours: int | None = None,
) -> list[Any]:
    metadata_exists = _metadata_exists_clause()
    default_stale = (
        stale_after_hours if stale_after_hours is not None else settings.DISCOVERY_STALE_HOURS
    )
    stale_by_time = and_(
        SourceRegistry.last_checked_at.isnot(None),
        text(
            "source_registry.last_checked_at < (now() AT TIME ZONE 'UTC') "
            "- ((CASE WHEN source_registry.stale_after_hours IS NOT NULL "
            "THEN source_registry.stale_after_hours ELSE :default_stale END) * interval '1 hour')"
        ).bindparams(default_stale=default_stale),
    )
    claim_available = or_(
        SourceRegistry.discovery_claim_expires_at.is_(None),
        SourceRegistry.discovery_claim_expires_at
        < text("(now() AT TIME ZONE 'UTC')"),
    )
    conditions: list[Any] = [
        SourceRegistry.enabled.is_(True),
        claim_available,
        or_(
            SourceRegistry.last_checked_at.is_(None),
            ~metadata_exists,
            stale_by_time,
        ),
    ]
    if project_module:
        conditions.append(SourceRegistry.project_module == project_module)

    retry_cooldown_minutes = max(0, settings.DISCOVERY_RETRY_COOLDOWN_MINUTES)
    if retry_cooldown_minutes > 0:
        cooldown_ok = or_(
            SourceRegistry.last_attempted_at.is_(None),
            text(
                "source_registry.last_attempted_at < (now() AT TIME ZONE 'UTC') "
                "- (:cooldown_minutes * interval '1 minute')"
            ).bindparams(cooldown_minutes=retry_cooldown_minutes),
        )
        conditions.append(cooldown_ok)
    return conditions


def _claim_timestamp_values(lease_ttl_minutes: int | None) -> dict[str, Any]:
    claimed_at = datetime.now(UTC)
    claim_expires_at = claimed_at + timedelta(
        minutes=max(1, lease_ttl_minutes or settings.DISCOVERY_CLAIM_TTL_MINUTES)
    )
    return {
        "discovery_claim_token": uuid4().hex,
        "discovery_claim_expires_at": claim_expires_at,
    }


def _workflow_claim_timestamp_values(lease_ttl_minutes: int | None = None) -> dict[str, Any]:
    claimed_at = datetime.now(UTC)
    ttl_minutes = max(1, lease_ttl_minutes or max(30, settings.DISCOVERY_CLAIM_TTL_MINUTES))
    return {
        "workflow_claim_token": uuid4().hex,
        "workflow_claimed_at": claimed_at,
        "workflow_claim_expires_at": claimed_at + timedelta(minutes=ttl_minutes),
    }


def _discovery_state_values(
    *,
    checked_at: datetime | None = None,
    attempted_at: datetime | None = None,
    discovery_signature: str | None = None,
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    if checked_at is not None:
        values["last_checked_at"] = checked_at
    if attempted_at is not None:
        values["last_attempted_at"] = attempted_at
    if discovery_signature is not None:
        values["discovery_signature"] = discovery_signature
    return values


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
        return await crud_source_registry.get(
            db=db,
            project_module=project_module,
            source_identifier=source_identifier,
            schema_to_select=SourceRegistryRead,
        )

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
        _validate_project_module(project_module)
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

        Currently supports updating enabled and stale_after_hours.

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

        update_data: dict[str, Any] = {}
        if enabled is not None:
            update_data["enabled"] = enabled
        if update_stale_after_hours:
            update_data["stale_after_hours"] = stale_after_hours
        if not update_data:
            return source

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
    async def get_sources_by_identifiers(
        db: AsyncSession,
        project_module: str,
        source_identifiers: Sequence[str],
    ) -> list[dict[str, Any]]:
        if not source_identifiers:
            return []
        result = await db.execute(
            select(SourceRegistry.uuid, SourceRegistry.source_identifier).where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.source_identifier.in_(list(source_identifiers)),
            )
        )
        return [
            {"uuid": row.uuid, "source_identifier": row.source_identifier}
            for row in result.all()
        ]

    @staticmethod
    async def get_sources_for_discovery(
        db: AsyncSession,
        project_module: str | None = None,
        stale_after_hours: int | None = None,
        limit: int | None = None,
    ) -> list[SourceRegistry]:
        query = (
            select(SourceRegistry)
            .where(
                *_discovery_eligibility_conditions(
                    project_module=project_module,
                    stale_after_hours=stale_after_hours,
                )
            )
            .order_by(SourceRegistry.created_at.asc())
        )
        if limit:
            query = query.limit(limit)

        result = await db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    async def get_pending_sources_for_workflow_run(
        db: AsyncSession,
        *,
        project_module: str,
        limit: int,
    ) -> list[str]:
        if limit <= 0:
            return []
        result = await db.execute(
            select(SourceRegistry.source_identifier)
            .where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.enabled.is_(True),
                SourceRegistry.workflow_run_pending.is_(True),
            )
            .order_by(
                SourceRegistry.workflow_run_pending_at.asc().nulls_last(),
                SourceRegistry.created_at.asc(),
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [row.source_identifier for row in result.all()]

    @staticmethod
    async def get_workflow_pending_stats(
        db: AsyncSession,
        *,
        project_module: str,
    ) -> dict[str, Any]:
        count_result = await db.execute(
            select(func.count()).select_from(SourceRegistry).where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.enabled.is_(True),
                SourceRegistry.workflow_run_pending.is_(True),
            )
        )
        oldest_result = await db.execute(
            select(SourceRegistry.workflow_run_pending_at)
            .where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.enabled.is_(True),
                SourceRegistry.workflow_run_pending.is_(True),
            )
            .order_by(SourceRegistry.workflow_run_pending_at.asc().nulls_last())
            .limit(1)
        )
        return {
            "count": int(count_result.scalar() or 0),
            "oldest_pending_at": oldest_result.scalar_one_or_none(),
        }

    @staticmethod
    async def claim_pending_sources_for_workflow_run(
        db: AsyncSession,
        *,
        project_module: str,
        limit: int,
        lease_ttl_minutes: int | None = None,
        commit: bool = True,
    ) -> tuple[str | None, list[str]]:
        if limit <= 0:
            return None, []
        claim_values = _workflow_claim_timestamp_values(lease_ttl_minutes)
        claimable_result = await db.execute(
            select(SourceRegistry.uuid, SourceRegistry.source_identifier)
            .where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.enabled.is_(True),
                SourceRegistry.workflow_run_pending.is_(True),
                or_(
                    SourceRegistry.workflow_claim_expires_at.is_(None),
                    SourceRegistry.workflow_claim_expires_at < text("(now() AT TIME ZONE 'UTC')"),
                ),
            )
            .order_by(
                SourceRegistry.workflow_run_pending_at.asc().nulls_last(),
                SourceRegistry.created_at.asc(),
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = list(claimable_result.all())
        if not rows:
            return None, []
        uuids = [row.uuid for row in rows]
        await db.execute(update(SourceRegistry).where(SourceRegistry.uuid.in_(uuids)).values(**claim_values))
        if commit:
            await db.commit()
        return cast(str, claim_values["workflow_claim_token"]), [row.source_identifier for row in rows]

    @staticmethod
    async def release_workflow_claim(
        db: AsyncSession,
        *,
        project_module: str,
        source_identifiers: Sequence[str],
        claim_token: str | None,
        commit: bool = True,
    ) -> int:
        if not source_identifiers or not claim_token:
            return 0
        result = await db.execute(
            update(SourceRegistry)
            .where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.source_identifier.in_(list(source_identifiers)),
                SourceRegistry.workflow_claim_token == claim_token,
            )
            .values(
                workflow_claim_token=None,
                workflow_claimed_at=None,
                workflow_claim_expires_at=None,
            )
            .returning(SourceRegistry.source_identifier)
        )
        count = len(result.all())
        if commit:
            await db.commit()
        return count

    @staticmethod
    async def get_enabled_project_modules(
        db: AsyncSession,
        project_module: str | None = None,
    ) -> list[str]:
        query = select(SourceRegistry.project_module).where(SourceRegistry.enabled.is_(True)).distinct()
        if project_module:
            query = query.where(SourceRegistry.project_module == project_module)
        query = query.order_by(SourceRegistry.project_module.asc())
        result = await db.execute(query)
        return [row[0] for row in result.all()]

    @staticmethod
    async def claim_source_rows_for_discovery(
        db: AsyncSession,
        *,
        project_module: str | None = None,
        stale_after_hours: int | None = None,
        limit: int,
        lease_ttl_minutes: int | None = None,
        commit: bool = True,
    ) -> tuple[str | None, list[dict[str, str]]]:
        if limit <= 0:
            return None, []

        claim_values = _claim_timestamp_values(lease_ttl_minutes)

        claimable_result = await db.execute(
            select(
                SourceRegistry.uuid,
                SourceRegistry.project_module,
                SourceRegistry.source_identifier,
            )
            .where(
                *_discovery_eligibility_conditions(
                    project_module=project_module,
                    stale_after_hours=stale_after_hours,
                )
            )
            .order_by(SourceRegistry.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        claimable_rows = list(claimable_result.all())
        if not claimable_rows:
            return None, []

        uuids = [row.uuid for row in claimable_rows]
        await db.execute(
            update(SourceRegistry)
            .where(SourceRegistry.uuid.in_(uuids))
            .values(**claim_values)
        )

        claimed_rows = [
            {
                "project_module": row.project_module,
                "source_identifier": row.source_identifier,
            }
            for row in claimable_rows
        ]
        logger.debug(
            "event=registry_claim_source_rows project_module=%s count=%s claim_token=%s claim_expires_at=%s",
            project_module or "all",
            len(claimed_rows),
            claim_values["discovery_claim_token"],
            claim_values["discovery_claim_expires_at"].isoformat(),
        )
        if commit:
            await db.commit()
        return cast(str, claim_values["discovery_claim_token"]), claimed_rows

    @staticmethod
    async def _update_claimed_sources(
        db: AsyncSession,
        project_module: str,
        source_identifiers: Sequence[str],
        *,
        claim_token: str | None,
        values: dict[str, Any],
        commit: bool = True,
    ) -> int:
        if not source_identifiers or not claim_token or not values:
            return 0
        result = await db.execute(
            update(SourceRegistry)
            .where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.source_identifier.in_(list(source_identifiers)),
                SourceRegistry.discovery_claim_token == claim_token,
            )
            .values(**values)
            .returning(SourceRegistry.source_identifier)
        )
        updated_count = len(result.all())
        if commit:
            await db.commit()
        return updated_count

    @staticmethod
    async def release_discovery_claim(
        db: AsyncSession,
        project_module: str,
        source_identifiers: list[str],
        *,
        claim_token: str | None,
        commit: bool = True,
    ) -> int:
        released_count = await SourceRegistryService._update_claimed_sources(
            db=db,
            project_module=project_module,
            source_identifiers=source_identifiers,
            claim_token=claim_token,
            values={
                "discovery_claim_token": None,
                "discovery_claim_expires_at": None,
            },
            commit=commit,
        )
        logger.debug(
            "event=registry_release_claim project_module=%s count=%s claim_token=%s",
            project_module,
            released_count,
            claim_token,
        )
        return released_count

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
    async def mark_sources_checked_if_claimed(
        db: AsyncSession,
        project_module: str,
        source_identifiers: list[str],
        *,
        claim_token: str | None,
        checked_at: datetime | None = None,
        commit: bool = True,
    ) -> int:
        """Update last_checked_at only for rows still owned by this claim."""
        return await SourceRegistryService._update_claimed_sources(
            db=db,
            project_module=project_module,
            source_identifiers=source_identifiers,
            claim_token=claim_token,
            values={"last_checked_at": checked_at or datetime.now(UTC)},
            commit=commit,
        )

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
    async def mark_sources_attempted_if_claimed(
        db: AsyncSession,
        project_module: str,
        source_identifiers: list[str],
        *,
        claim_token: str | None,
        attempted_at: datetime | None = None,
        commit: bool = True,
    ) -> int:
        """Update last_attempted_at only for rows still owned by this claim."""
        return await SourceRegistryService._update_claimed_sources(
            db=db,
            project_module=project_module,
            source_identifiers=source_identifiers,
            claim_token=claim_token,
            values={"last_attempted_at": attempted_at or datetime.now(UTC)},
            commit=commit,
        )

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
            .values(
                last_checked_at=None,
                last_attempted_at=None,
                discovery_claim_token=None,
                discovery_claim_expires_at=None,
                workflow_claim_token=None,
                workflow_claimed_at=None,
                workflow_claim_expires_at=None,
            )
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
    async def mark_source_pending_workflow_run(
        db: AsyncSession,
        source_id: UUID,
        *,
        pending_at: datetime | None = None,
    ) -> None:
        await db.execute(
            update(SourceRegistry)
            .where(SourceRegistry.uuid == source_id)
            .values(
                workflow_run_pending=True,
                workflow_run_pending_at=pending_at or datetime.now(UTC),
            )
        )

    @staticmethod
    async def clear_workflow_pending_for_sources(
        db: AsyncSession,
        *,
        project_module: str,
        source_identifiers: list[str],
        commit: bool = True,
    ) -> int:
        if not source_identifiers:
            return 0
        result = await db.execute(
            update(SourceRegistry)
            .where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.source_identifier.in_(source_identifiers),
            )
            .values(
                workflow_run_pending=False,
                workflow_run_pending_at=None,
                workflow_claim_token=None,
                workflow_claimed_at=None,
                workflow_claim_expires_at=None,
            )
            .returning(SourceRegistry.source_identifier)
        )
        count = len(result.all())
        if commit:
            await db.commit()
        return count

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
        update_data = _discovery_state_values(
            checked_at=checked_at,
            attempted_at=attempted_at,
            discovery_signature=discovery_signature,
        )
        if discovery_signature is not None:
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

    @staticmethod
    async def get_claimed_source_for_update(
        db: AsyncSession,
        project_module: str,
        source_identifier: str,
        *,
        claim_token: str | None,
    ) -> dict[str, Any] | None:
        """Lock and return a source row only if it is still owned by this claim token."""
        if not claim_token:
            return None
        result = await db.execute(
            select(SourceRegistry.uuid, SourceRegistry.project_module, SourceRegistry.source_identifier)
            .where(
                SourceRegistry.project_module == project_module,
                SourceRegistry.source_identifier == source_identifier,
                SourceRegistry.discovery_claim_token == claim_token,
            )
            .with_for_update()
        )
        row = result.first()
        if row is None:
            return None
        return {
            "uuid": row.uuid,
            "project_module": row.project_module,
            "source_identifier": row.source_identifier,
        }


# instance
source_registry_service = SourceRegistryService()
