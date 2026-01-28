from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import Response
from fastcrud import PaginatedListResponse, compute_offset, paginated_response
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_user
from ...core.archive.service import archive_metadata_service
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...core.registry.service import source_registry_service
from ...crud.crud_source_registry import crud_source_registry
from ...schemas.registry import (
    SourceRegistryBulkCreate,
    SourceRegistryBulkCreateResponse,
    SourceRegistryCreate,
    SourceRegistryRead,
    SourceRegistryUpdate,
)

router = APIRouter(prefix="/sources", tags=["sources"])

@router.get("", response_model=PaginatedListResponse[SourceRegistryRead])
async def list_sources(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    project_module: str | None = None,
    enabled: bool | None = None,
    page: int = 1,
    items_per_page: int = 10,
) -> dict[str, Any]:
    """List sources from the registry.

    Args:
        request: FastAPI request object
        db: Database session
        project_module: Filter by project module (e.g., "wallaby")
        enabled: Filter by enabled status (True/False)
        page: Page number (1-indexed)
        items_per_page: Number of items per page

    Returns:
        Paginated list of sources
    """
    filters: dict[str, Any] = {}
    if project_module:
        filters["project_module"] = project_module
    if enabled is not None:
        filters["enabled"] = enabled

    sources_data = await crud_source_registry.get_multi(
        db=db,
        offset=compute_offset(page, items_per_page),
        limit=items_per_page,
        schema_to_select=SourceRegistryRead,
        return_total_count=True,
        **filters,
    )

    response: dict[str, Any] = paginated_response(
        crud_data=sources_data, page=page, items_per_page=items_per_page
    )
    return response

@router.post("", response_model=SourceRegistryRead, status_code=201)
async def register_source(
    request: Request,
    source_data: SourceRegistryCreate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Register a new source in the registry.

    Registration is idempotent - if a source with the same project_module
    and source_identifier already exists, returns the existing source.

    Args:
        request: FastAPI request object
        source_data: Source registration data (project_module, source_identifier, enabled)
        current_user: Current authenticated user
        db: Database session

    Returns:
        Source registry entry (existing or newly created)
    """
    source = await source_registry_service.register_source(
        db=db,
        project_module=source_data.project_module,
        source_identifier=source_data.source_identifier,
        enabled=source_data.enabled,
    )
    return source


@router.post("/bulk", response_model=SourceRegistryBulkCreateResponse, status_code=200)
async def bulk_register_sources(
    request: Request,
    bulk_data: SourceRegistryBulkCreate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    created: list[dict[str, Any]] = []
    existing: list[dict[str, Any]] = []

    for item in bulk_data.items:
        already = await source_registry_service.check_existing_source(
            db=db,
            project_module=item.project_module,
            source_identifier=item.source_identifier,
        )
        if already:
            existing.append(already)
            continue

        new_source = await source_registry_service.register_source(
            db=db,
            project_module=item.project_module,
            source_identifier=item.source_identifier,
            enabled=item.enabled,
        )
        created.append(new_source)

    return {
        "created": created,
        "existing": existing,
        "total_created": len(created),
        "total_existing": len(existing),
    }

@router.get("/{source_id}")
async def get_source(
    request: Request,
    source_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Get a single source by UUID.

    Args:
        request: FastAPI request object
        source_id: Source UUID
        db: Database session

    Returns:
        Source registry entry details

    Raises:
        NotFoundException: If source not found
    """
    source = await source_registry_service.get_source(
        db=db,
        source_id=source_id,
    )
    return source


@router.get("/{source_id}/metadata")
async def get_source_metadata(
    request: Request,
    source_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Get archive metadata for a source.

    Returns all metadata entries (grouped by SBID) for the specified source.

    Args:
        request: FastAPI request object
        source_id: Source UUID
        db: Database session

    Returns:
        Dictionary containing source information and list of metadata entries

    Raises:
        NotFoundException: If source not found
    """
    source = await source_registry_service.get_source(
        db=db,
        source_id=source_id,
    )
    
    metadata_list = await archive_metadata_service.list_metadata_for_source(
        db=db,
        project_module=source["project_module"],
        source_identifier=source["source_identifier"],
    )
    
    return {
        "source": source,
        "metadata": metadata_list,
        "metadata_count": len(metadata_list),
    }

@router.patch("/{source_id}")
async def update_source(
    request: Request,
    source_id: UUID,
    source_data: SourceRegistryUpdate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Update a source in the registry.

    Currently supports updating the enabled status of a source.

    Args:
        request: FastAPI request object
        source_id: Source UUID
        source_data: Update data (enabled status)
        current_user: Current authenticated user
        db: Database session

    Returns:
        Updated source registry entry

    Raises:
        NotFoundException: If source not found
    """
    source = await source_registry_service.update_source(
        db=db,
        source_id=source_id,
        enabled=source_data.enabled,
    )
    return source


@router.delete("/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_source(
    request: Request,
    source_id: UUID,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> Response:
    """Delete a source from the registry.

    Note: This will prevent new runs from being created for this source,
    but existing runs are not affected.

    Args:
        request: FastAPI request object
        source_id: Source UUID
        current_user: Current authenticated user
        db: Database session

    Returns:
        HTTP 204 No Content on success

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

    # not sure how this will be handled yet, whether we delete run records,
    # or prevent deleting a source if there are runs...
    # from ...crud.crud_run_record import crud_run_records
    # runs = await crud_run_records.get_multi(
    #     db=db,
    #     project_module=source["project_module"],
    #     source_identifier=source["source_identifier"],
    # )
    # run_count = runs.get("total", 0) if isinstance(runs, dict) else len(runs) if isinstance(runs, list) else 0

    # if run_count > 0:
    #     raise Exception(
    #         f"Cannot delete source {source_id} ({source['source_identifier']}): "
    #         f"{run_count} associated run(s) exist. Please remove these runs before deleting the source."
    #     )

    await crud_source_registry.delete(db=db, uuid=source_id)

    return Response(status_code=status.HTTP_204_NO_CONTENT)

# /sources/bulk-add and /sources/bulk-delete and /sources/bulk-update
# will be useful for lots of sources at once
