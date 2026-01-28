"""Archive metadata service."""
import logging
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_archive_metadata import crud_archive_metadata
from ...schemas.archive import ArchiveMetadataCreateInternal, ArchiveMetadataRead
from ..exceptions.http_exceptions import NotFoundException

logger = logging.getLogger(__name__)


class ArchiveMetadataService:
    @staticmethod
    async def get_metadata(
        db: AsyncSession,
        metadata_id: UUID,
    ) -> dict[str, Any]:
        """Get archive metadata by UUID."""
        record = await crud_archive_metadata.get(
            db=db,
            uuid=metadata_id,
            schema_to_select=ArchiveMetadataRead,
        )
        if not record:
            raise NotFoundException(f"Archive metadata with id {metadata_id} not found")
        return record

    @staticmethod
    async def get_metadata_by_key(
        db: AsyncSession,
        project_module: str,
        source_identifier: str,
        sbid: str,
    ) -> dict[str, Any] | None:
        """Get archive metadata by composite key."""
        return await crud_archive_metadata.get(
            db=db,
            project_module=project_module,
            source_identifier=source_identifier,
            sbid=sbid,
            schema_to_select=ArchiveMetadataRead,
        )

    @staticmethod
    async def upsert_metadata(
        db: AsyncSession,
        project_module: str,
        source_identifier: str,
        sbid: str,
        metadata_json: dict | None = None,
    ) -> dict[str, Any]:
        """Create or update archive metadata for an SBID."""
        existing = await crud_archive_metadata.get(
            db=db,
            project_module=project_module,
            source_identifier=source_identifier,
            sbid=sbid,
            schema_to_select=ArchiveMetadataRead,
        )
        if existing:
            await crud_archive_metadata.update(
                db=db,
                object={"metadata_json": metadata_json, "updated_at": datetime.now(UTC)},
                uuid=existing["uuid"],
            )
            updated = await crud_archive_metadata.get(
                db=db,
                uuid=existing["uuid"],
                schema_to_select=ArchiveMetadataRead,
            )
            if not updated:
                raise NotFoundException(
                    f"Archive metadata with id {existing['uuid']} not found after update"
                )
            return updated

        create_data = ArchiveMetadataCreateInternal(
            project_module=project_module,
            source_identifier=source_identifier,
            sbid=sbid,
            metadata_json=metadata_json,
        )
        return await crud_archive_metadata.create(
            db=db,
            object=create_data,
            schema_to_select=ArchiveMetadataRead,
        )

    @staticmethod
    async def list_metadata_for_source(
        db: AsyncSession,
        project_module: str,
        source_identifier: str,
    ) -> list[dict[str, Any]]:
        """List archive metadata entries for a source."""
        records = await crud_archive_metadata.get_multi(
            db=db,
            project_module=project_module,
            source_identifier=source_identifier,
            schema_to_select=ArchiveMetadataRead,
        )
        # FastCRUD returns {"data": [...], "total_count": N}
        return cast(list[dict[str, Any]], records.get("data", []))


archive_metadata_service = ArchiveMetadataService()
