from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastcrud import PaginatedListResponse, compute_offset, paginated_response
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_user
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...crud.crud_daliuge_execution_profile import crud_daliuge_execution_profile
from ...schemas.daliuge import (
    DaliugeExecutionProfileCreate,
    DaliugeExecutionProfileRead,
    DaliugeExecutionProfileUpdate,
    DeploymentBackend,
    build_execution_profile_create_dict,
    merge_execution_profile_state,
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
    deployment_backend: DeploymentBackend | None = None,
) -> dict[str, Any]:
    """List DALiuGE execution profiles. Filter by project_module or global_only."""
    filters: dict[str, Any] = {}
    if project_module is not None:
        filters["project_module"] = project_module
    if global_only:
        filters["project_module"] = None
    if deployment_backend is not None:
        filters["deployment_backend"] = deployment_backend

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


# @router.post("/validate", response_model=DaliugeExecutionProfileCreate)
# async def validate_execution_profile(
#     request: Request,
#     body: dict[str, Any],
#     db: Annotated[AsyncSession, Depends(async_get_db)],
#     current_user: Annotated[dict, Depends(get_current_user)],
# ) -> DaliugeExecutionProfileCreate:
#     try:
#         validated = DaliugeExecutionProfileCreate.model_validate(body)
#     except ValidationError as e:
#         raise HTTPException(status_code=422, detail=e.errors()) from e
#     return validated


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
    current = await crud_daliuge_execution_profile.get(
        db=db,
        uuid=profile_id,
        schema_to_select=DaliugeExecutionProfileRead,
    )
    if current is None:
        raise NotFoundException(f"Execution profile {profile_id} not found")
    if not update_data:
        return current

    merged = merge_execution_profile_state(current, update_data)
    try:
        validated = DaliugeExecutionProfileCreate.model_validate(
            build_execution_profile_create_dict(merged)
        )
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors()) from e

    final_update = dict(update_data)
    if validated.deployment_backend == "slurm_remote":
        final_update["deploy_host"] = None
        final_update["deploy_port"] = None

    await crud_daliuge_execution_profile.update(
        db=db,
        object=final_update,
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
