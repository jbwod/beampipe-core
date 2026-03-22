from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastcrud import PaginatedListResponse, compute_offset, paginated_response
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_user
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...crud.crud_daliuge_execution_profile import crud_daliuge_execution_profile
from ...schemas.daliuge import (
    DaliugeExecutionProfileCreate,
    DaliugeExecutionProfileRead,
    DaliugeExecutionProfileUpdate,
)

router = APIRouter(prefix="/execution-profiles", tags=["execution-profiles"])


@router.get("", response_model=PaginatedListResponse[DaliugeExecutionProfileRead])
async def list_execution_profiles(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    current_user: Annotated[dict, Depends(get_current_user)],
    page: int = 1,
    items_per_page: int = 10,
    project_module: str | None = None,
    global_only: bool = False,
) -> dict[str, Any]:
    """List DALiuGE execution profiles. Filter by project_module or global_only."""
    filters: dict[str, Any] = {}
    if project_module is not None:
        filters["project_module"] = project_module
    if global_only:
        filters["project_module"] = None

    data = await crud_daliuge_execution_profile.get_multi(
        db=db,
        offset=compute_offset(page, items_per_page),
        limit=items_per_page,
        schema_to_select=DaliugeExecutionProfileRead,
        return_total_count=True,
        **filters,
    )
    return paginated_response(
        crud_data=data, page=page, items_per_page=items_per_page
    )


@router.post("", response_model=DaliugeExecutionProfileRead, status_code=201)
async def create_execution_profile(
    request: Request,
    body: DaliugeExecutionProfileCreate,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    current_user: Annotated[dict, Depends(get_current_user)],
) -> dict[str, Any]:
    profile = await crud_daliuge_execution_profile.create(
        db=db,
        object=body,
        schema_to_select=DaliugeExecutionProfileRead,
    )
    return profile


@router.get("/{profile_id}", response_model=DaliugeExecutionProfileRead)
async def get_execution_profile(
    request: Request,
    profile_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    current_user: Annotated[dict, Depends(get_current_user)],
) -> dict[str, Any]:
    profile = await crud_daliuge_execution_profile.get(
        db=db,
        uuid=profile_id,
        schema_to_select=DaliugeExecutionProfileRead,
    )
    if profile is None:
        raise NotFoundException(f"Execution profile {profile_id} not found")
    return profile


@router.patch("/{profile_id}", response_model=DaliugeExecutionProfileRead)
async def update_execution_profile(
    request: Request,
    profile_id: UUID,
    body: DaliugeExecutionProfileUpdate,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    current_user: Annotated[dict, Depends(get_current_user)],
) -> dict[str, Any]:
    update_data = body.model_dump(exclude_unset=True)
    if not update_data:
        profile = await crud_daliuge_execution_profile.get(
            db=db,
            uuid=profile_id,
            schema_to_select=DaliugeExecutionProfileRead,
        )
        if profile is None:
            raise NotFoundException(f"Execution profile {profile_id} not found")
        return profile

    await crud_daliuge_execution_profile.update(
        db=db,
        object=update_data,
        uuid=profile_id,
    )
    updated = await crud_daliuge_execution_profile.get(
        db=db,
        uuid=profile_id,
        schema_to_select=DaliugeExecutionProfileRead,
    )
    if updated is None:
        raise NotFoundException(f"Execution profile {profile_id} not found")
    return updated


@router.delete("/{profile_id}", status_code=204)
async def delete_execution_profile(
    request: Request,
    profile_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    current_user: Annotated[dict, Depends(get_current_user)],
) -> None:
    #might eneds to be soft delete
    deleted = await crud_daliuge_execution_profile.delete(
        db=db, uuid=profile_id
    )
    if not deleted:
        raise NotFoundException(f"Execution profile {profile_id} not found")
