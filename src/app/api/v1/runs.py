from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastcrud import PaginatedListResponse, compute_offset, paginated_response
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_user
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...core.ledger.service import run_ledger_service
from ...core.orchestration.service import execute_run as orchestration_execute_run
from ...core.orchestration.service import prepare_run as orchestration_prepare_run
from ...crud.crud_run_record import crud_batch_run_records
from ...models.ledger import RunStatus
from ...schemas.ledger import (
    BatchRunRecordCreate,
    BatchRunRecordRead,
    BatchRunRecordUpdate,
    PrepareRunRequest,
    PrepareRunResponse,
)

router = APIRouter(prefix="/runs", tags=["runs"])


@router.post("/prepare", response_model=PrepareRunResponse)
async def prepare_run(
    request: Request,
    body: PrepareRunRequest,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    return await orchestration_prepare_run(
        db=db,
        project_module=body.project_module,
        sources=body.sources,
    )


@router.get("", response_model=PaginatedListResponse[BatchRunRecordRead])
async def list_runs(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    page: int = 1,
    items_per_page: int = 10,
    project_module: str | None = None,
    status: RunStatus | None = None,
) -> dict[str, Any]:
    """List runs tbd"""
    filters: dict[str, Any] = {}
    if project_module:
        filters["project_module"] = project_module
    if status:
        filters["status"] = status

    runs_data = await crud_batch_run_records.get_multi(
        db=db,
        offset=compute_offset(page, items_per_page),
        limit=items_per_page,
        **filters,
    )

    return paginated_response(
        crud_data=runs_data, page=page, items_per_page=items_per_page
    )


@router.post("", response_model=BatchRunRecordRead, status_code=201)
async def create_run(
    request: Request,
    run_data: BatchRunRecordCreate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Create a new batch run record."""
    return await run_ledger_service.create_run(
        db=db,
        project_module=run_data.project_module,
        sources=[s.model_dump() for s in run_data.sources],
        archive_name=run_data.archive_name,
        created_by_id=current_user.get("id"),
    )


@router.get("/{run_id}", response_model=BatchRunRecordRead)
async def get_run(
    request: Request,
    run_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Get a single run by UUID."""
    run = await crud_batch_run_records.get(
        db=db, uuid=run_id, schema_to_select=BatchRunRecordRead
    )
    if run is None:
        raise NotFoundException(f"Run {run_id} not found")
    return run


@router.patch("/{run_id}", response_model=BatchRunRecordRead)
async def update_run(
    request: Request,
    run_id: UUID,
    run_update: BatchRunRecordUpdate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Update a run record."""
    return await run_ledger_service.update_run_status(
        db=db,
        run_id=run_id,
        status=run_update.status,
        scheduler_job_id=run_update.scheduler_job_id,
        scheduler_name=run_update.scheduler_name,
        workflow_manifest=run_update.workflow_manifest,
        error=run_update.last_error,
    )


@router.post("/{run_id}/execute")
async def execute_run(
    request: Request,
    run_id: UUID,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
    do_stage: bool = True,
    do_submit: bool = True,
) -> dict[str, Any]:
    """Trigger stage + manifest + submit for a run.
        do_stage: Stage data via CASDA (default True)
        do_submit: Submit to DALiuGE (default True).
    """
    try:
        return await orchestration_execute_run(
            db=db, run_id=run_id, do_stage=do_stage, do_submit=do_submit
        )
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Execute failed: {e}. Run {run_id} marked as failed. Check run status for details.",
        )
