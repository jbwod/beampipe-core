from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastcrud import PaginatedListResponse, compute_offset, paginated_response
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_user
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...core.exceptions.workflow_exceptions import WorkflowFailure
from ...core.ledger.service import execution_ledger_service
from ...core.orchestration.service import execute_execution as orchestration_execute_execution
from ...core.orchestration.service import prepare_execution as orchestration_prepare_execution
from ...crud.crud_execution_record import crud_batch_execution_records
from ...models.ledger import ExecutionStatus
from ...schemas.ledger import (
    BatchExecutionRecordCreate,
    BatchExecutionRecordRead,
    BatchExecutionRecordUpdate,
    PrepareExecutionRequest,
    PrepareExecutionResponse,
)

router = APIRouter(prefix="/executions", tags=["executions"])


@router.post("/prepare", response_model=PrepareExecutionResponse)
async def prepare_execution(
    request: Request,
    body: PrepareExecutionRequest,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    return await orchestration_prepare_execution(
        db=db,
        project_module=body.project_module,
        sources=body.sources,
    )


@router.get("", response_model=PaginatedListResponse[BatchExecutionRecordRead])
async def list_executions(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    page: int = 1,
    items_per_page: int = 10,
    project_module: str | None = None,
    status: ExecutionStatus | None = None,
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
    return await execution_ledger_service.create_execution(
        db=db,
        project_module=execution_data.project_module,
        sources=[s.model_dump() for s in execution_data.sources],
        archive_name=execution_data.archive_name,
        deployment_profile_id=execution_data.deployment_profile_id,
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


@router.post("/{execution_id}/execute")
async def execute_execution(
    request: Request,
    execution_id: UUID,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
    do_stage: bool = True,
    do_submit: bool = True,
) -> dict[str, Any]:
    try:
        return await orchestration_execute_execution(
            db=db,
            execution_id=execution_id,
            do_stage=do_stage,
            do_submit=do_submit,
        )
    except WorkflowFailure as wf:
        raise HTTPException(
            status_code=422,
            detail=wf.format_for_ledger(),
        ) from wf
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Execute failed: {e}. Execution {execution_id} marked as failed. Check execution status for details.",
        ) from e
