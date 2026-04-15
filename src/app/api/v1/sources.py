import logging
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from fastcrud import PaginatedListResponse, compute_offset, paginated_response
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_user
from ...core.archive.service import archive_metadata_service
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...core.projects import list_project_modules
from ...core.registry.service import invalid_project_module_message, source_registry_service
from ...crud.crud_source_registry import crud_source_registry
from ...schemas.registry import (
    DiscoverTriggerRequest,
    DiscoverTriggerResponse,
    SourceMetadataResponse,
    SourceRegistryBulkCreate,
    SourceRegistryBulkCreateResponse,
    SourceRegistryCreate,
    SourceRegistryRead,
    SourceRegistryUpdate,
)


class SourceSortField(StrEnum):
    created_at = "created_at"
    updated_at = "updated_at"
    source_identifier = "source_identifier"
    last_checked_at = "last_checked_at"

router = APIRouter(prefix="/sources", tags=["sources"])
logger = logging.getLogger(__name__)


def _json_response(data: dict[str, Any], *, status_code: int) -> JSONResponse:
    return JSONResponse(content=jsonable_encoder(data), status_code=status_code)


def _ensure_valid_project_module(project_module: str) -> None:
    available_modules = list_project_modules()
    if project_module not in available_modules:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=invalid_project_module_message(project_module, available_modules),
        )

@router.get("", response_model=PaginatedListResponse[SourceRegistryRead])
async def list_sources(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    project_module: str | None = None,
    enabled: bool | None = None,
    page: int = 1,
    items_per_page: int = 10,
    sort_by: SourceSortField = SourceSortField.created_at,
    order: Annotated[Literal["asc", "desc"], Query(description="Sort direction")] = "desc",
) -> dict[str, Any]:
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
        sort_columns=[sort_by.value],
        sort_orders=[order],
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
) -> Response:
    """Register a new source in the registry.

    """
    _ensure_valid_project_module(source_data.project_module)

    existing = await source_registry_service.check_existing_source(
        db=db,
        project_module=source_data.project_module,
        source_identifier=source_data.source_identifier,
    )
    if existing:
        logger.info(
            "event=sources_register_existing project_module=%s source_identifier=%s",
            source_data.project_module,
            source_data.source_identifier,
        )
        return _json_response(existing, status_code=200)

    source = await source_registry_service.register_source(
        db=db,
        project_module=source_data.project_module,
        source_identifier=source_data.source_identifier,
        enabled=source_data.enabled,
    )
    logger.info(
        "event=sources_register_created project_module=%s source_identifier=%s",
        source_data.project_module,
        source_data.source_identifier,
    )
    return _json_response(source, status_code=201)


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
        _ensure_valid_project_module(item.project_module)
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

    logger.info(
        "event=sources_bulk_register total_created=%s total_existing=%s",
        len(created),
        len(existing),
    )
    return {
        "created": created,
        "existing": existing,
        "total_created": len(created),
        "total_existing": len(existing),
    }


@router.post("/discover", response_model=DiscoverTriggerResponse, status_code=200)
async def trigger_discovery(
    request: Request,
    body: DiscoverTriggerRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    _ensure_valid_project_module(body.project_module)
    if body.source_identifier is not None:
        source_identifiers: list[str] | None = [body.source_identifier]
    else:
        source_identifiers = body.source_identifiers

    identifiers = await source_registry_service.mark_sources_for_rediscovery(
        db=db,
        project_module=body.project_module,
        source_identifiers=source_identifiers,
    )
    logger.info(
        "event=sources_trigger_discovery project_module=%s marked_count=%s",
        body.project_module,
        len(identifiers),
    )
    return {
        "project_module": body.project_module,
        "marked_count": len(identifiers),
        "source_identifiers": identifiers,
    }


@router.get("/{source_id}", response_model=SourceRegistryRead)
async def get_source(
    request: Request,
    source_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    return await source_registry_service.get_source(db=db, source_id=source_id)


@router.get("/{source_id}/metadata", response_model=SourceMetadataResponse)
async def get_source_metadata(
    request: Request,
    source_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    source = await source_registry_service.get_source(db=db, source_id=source_id)

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

@router.patch("/{source_id}", response_model=SourceRegistryRead)
async def update_source(
    request: Request,
    source_id: UUID,
    source_data: SourceRegistryUpdate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    return await source_registry_service.update_source(
        db=db,
        source_id=source_id,
        enabled=source_data.enabled,
        stale_after_hours=source_data.stale_after_hours,
        update_stale_after_hours="stale_after_hours" in source_data.model_fields_set,
    )


@router.delete("/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_source(
    request: Request,
    source_id: UUID,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> Response:
    source = await crud_source_registry.get(
        db=db,
        uuid=source_id,
        schema_to_select=SourceRegistryRead,
    )
    if not source:
        raise NotFoundException(f"Source with id {source_id} not found")

    await crud_source_registry.delete(db=db, uuid=source_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
