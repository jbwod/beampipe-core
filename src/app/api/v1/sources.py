from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import Response
from fastcrud import PaginatedListResponse, compute_offset, paginated_response
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_user
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import BadRequestException, NotFoundException
from ...core.registry.service import source_registry_service
from ...crud.crud_source_registry import crud_source_registry
from ...models.registry import SourceRegistry
from ...schemas.registry import SourceRegistryRead, SourceRegistryCreate, SourceRegistryUpdate


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
# nevermind, just use the crud directly
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
    source = await source_registry_service.register_source(
        db=db,
        project_module=source_data.project_module,
        source_identifier=source_data.source_identifier,
        enabled=source_data.enabled,
    )
    return source

@router.get("/{source_id}")
async def get_source(
    request: Request,
    source_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    source = await source_registry_service.get_source(
        db=db,
        source_id=source_id,
    )
    return source

@router.patch("/{source_id}")
async def update_source(
    request: Request,
    source_id: UUID,
    source_data: SourceRegistryUpdate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
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
    """
    source = await crud_source_registry.get(
        db=db,
        uuid=source_id,
        schema_to_select=SourceRegistryRead,
    )
    if not source:
        raise NotFoundException(f"Source with id {source_id} not found")

    # not sure how this will be handled yet, whether we delete run records, or prevent deleting a source if there are runs...    
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