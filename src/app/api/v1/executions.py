from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastcrud import PaginatedListResponse, compute_offset, paginated_response
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_user
from ...core.config import settings
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...core.ledger.service import execution_ledger_service
from ...core.orchestration.service import prepare_execution as orchestration_prepare_execution
from ...core.utils import queue
from ...crud.crud_daliuge_deployment_profile import crud_daliuge_deployment_profile
from ...crud.crud_execution_record import crud_batch_execution_records
from ...models.ledger import ExecutionStatus
from ...schemas.ledger import (
    BatchExecutionRecordCreate,
    BatchExecutionRecordListItem,
    BatchExecutionRecordRead,
    BatchExecutionRecordUpdate,
    BatchExecutionStatusResponse,
    ExecuteRequest,
    PrepareExecutionRequest,
    PrepareExecutionResponse,
)


class ExecutionSortField(str, Enum):
    created_at = "created_at"
    updated_at = "updated_at"
    started_at = "started_at"
    completed_at = "completed_at"
    status = "status"

router = APIRouter(prefix="/executions", tags=["executions"])


@router.post("/prepare", response_model=PrepareExecutionResponse)
async def prepare_execution(
    request: Request,
    body: PrepareExecutionRequest,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    return await orchestration_prepare_execution(
        db=db,
        project_module=body.project_module,
        sources=body.sources,
    )


@router.get("", response_model=PaginatedListResponse[BatchExecutionRecordListItem])
async def list_executions(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    page: int = 1,
    items_per_page: int = 10,
    project_module: str | None = None,
    status: ExecutionStatus | None = None,
    sort_by: ExecutionSortField = ExecutionSortField.created_at,
    order: Annotated[Literal["asc", "desc"], Query(description="Sort direction")] = "desc",
) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if project_module:
        filters["project_module"] = project_module
    if status:
        filters["status"] = status

    executions_data = await crud_batch_execution_records.get_multi(
        db=db,
        offset=compute_offset(page, items_per_page),
        limit=items_per_page,
        schema_to_select=BatchExecutionRecordListItem,
        sort_columns=[sort_by.value],
        sort_orders=[order],
        **filters,
    )

    return paginated_response(
        crud_data=executions_data, page=page, items_per_page=items_per_page
    )


@router.post("", response_model=BatchExecutionRecordRead, status_code=201)
async def create_execution(
    request: Request,
    execution_data: BatchExecutionRecordCreate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    deployment_profile_id: UUID | None = None
    if execution_data.deployment_profile_name:
        profile = await crud_daliuge_deployment_profile.get(
            db=db,
            name=execution_data.deployment_profile_name,
        )
        if profile is None:
            raise NotFoundException(
                f"Deployment profile {execution_data.deployment_profile_name} not found"
            )
        deployment_profile_id = profile.get("uuid")

    return await execution_ledger_service.create_execution(
        db=db,
        project_module=execution_data.project_module,
        sources=[s.model_dump() for s in execution_data.sources],
        archive_name=execution_data.archive_name,
        deployment_profile_id=deployment_profile_id,
        created_by_id=current_user.get("id"),
    )


@router.get("/{execution_id}", response_model=BatchExecutionRecordRead)
async def get_execution(
    request: Request,
    execution_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if execution is None:
        raise NotFoundException(f"Execution {execution_id} not found")
    return execution


@router.patch("/{execution_id}", response_model=BatchExecutionRecordRead)
async def update_execution(
    request: Request,
    execution_id: UUID,
    execution_update: BatchExecutionRecordUpdate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    return await execution_ledger_service.update_execution_status(
        db=db,
        execution_id=execution_id,
        status=execution_update.status,
        scheduler_job_id=execution_update.scheduler_job_id,
        scheduler_name=execution_update.scheduler_name,
        workflow_manifest=execution_update.workflow_manifest,
        error=execution_update.last_error,
    )


@router.get("/{execution_id}/status", response_model=BatchExecutionStatusResponse)
async def get_execution_status(
    request: Request,
    execution_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionStatusResponse
    )
    if execution is None:
        raise NotFoundException(f"Execution {execution_id} not found")
    return execution


@router.post("/{execution_id}/execute", status_code=202)
async def execute_execution(
    request: Request,
    execution_id: UUID,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
    body: ExecuteRequest | None = None,
) -> dict[str, Any]:
    do_stage = body.do_stage if body else True
    do_submit = body.do_submit if body else True

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if execution is None:
        raise NotFoundException(f"Execution {execution_id} not found")

    if queue.pool is None:
        raise HTTPException(status_code=503, detail="Queue is not available")

    job = await queue.pool.enqueue_job(
        "execute_execution_job",
        str(execution_id),
        do_stage=do_stage,
        do_submit=do_submit,
        _queue_name=settings.WORKER_QUEUE_NAME,
    )
    if job is None:
        raise HTTPException(status_code=500, detail="Failed to enqueue execution job")

    return {
        "status": "accepted",
        "execution_id": str(execution_id),
        "job_id": job.job_id,
        "do_stage": do_stage,
        "do_submit": do_submit,
    }
