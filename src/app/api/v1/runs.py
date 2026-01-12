from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastcrud import PaginatedListResponse, compute_offset, paginated_response
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_user
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import BadRequestException, NotFoundException
from ...core.ledger.service import run_ledger_service
from ...crud.crud_run_record import crud_run_records
from ...models.ledger import RunStatus
from ...schemas.ledger import RunRecordRead, RunRecordCreate, RunRecordUpdate

router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("", response_model=PaginatedListResponse[RunRecordRead])
async def list_runs(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    page: int = 1,
    items_per_page: int = 10,
    project_module: str | None = None,
    source_identifier: str | None = None,
    status: RunStatus | None = None,
) -> dict[str, Any]:
    """List runs
    Args:
        request: FastAPI request object
        db: Database session
        page: Page number (1-indexed)
        items_per_page: Number of items per page
        project_module: Filter by project module
        source_identifier: Filter by source identifier
        status: Filter by run status

    Returns:
        Paginated list of runs
    """
    filters: dict[str, Any] = {}
    if project_module:
        filters["project_module"] = project_module
    if source_identifier:
        filters["source_identifier"] = source_identifier
    if status:
        filters["status"] = status

    runs_data = await crud_run_records.get_multi(
        db=db,
        offset=compute_offset(page, items_per_page),
        limit=items_per_page,
        **filters,
    )

    response: dict[str, Any] = paginated_response(
        crud_data=runs_data, page=page, items_per_page=items_per_page
    )
    return response


@router.post("", response_model=RunRecordRead, status_code=201)
async def create_run(
    request: Request,
    run_data: RunRecordCreate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Create a new run record.
    
    Args:
        request: FastAPI request object
        run_data: Run data
        current_user: Current authenticated user
        db: Database session

    Returns:
        New run record
    """
    run = await run_ledger_service.create_run(
        db=db,
        project_module=run_data.project_module,
        source_identifier=run_data.source_identifier,
        dataset_id=run_data.dataset_id,
        archive_name=run_data.archive_name,
        dataset_metadata=run_data.dataset_metadata,
        created_by_id=current_user.get("id"),
    )
    return run


@router.get("/{run_id}", response_model=RunRecordRead)
async def get_run(
    request: Request,
    run_id: UUID,
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Get a single run by UUID.

    Args:
        request: FastAPI request object
        run_id: Run UUID
        db: Database session

    Returns:
        Run record details

    Raises:
        NotFoundException: If run not found
    """
    run = await crud_run_records.get(
        db=db, uuid=run_id, schema_to_select=RunRecordRead
    )
    if run is None:
        raise NotFoundException(f"Run {run_id} not found")

    return run


@router.patch("/{run_id}", response_model=RunRecordRead)
async def update_run(
    request: Request,
    run_id: UUID,
    run_update: RunRecordUpdate,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(async_get_db)],
) -> dict[str, Any]:
    """Update a run record.

    Updates run status, workflow manifest, scheduler information, etc.

    Args:
        request: FastAPI request object
        run_id: Run UUID
        run_update: Update data
        current_user: Current authenticated user
        db: Database session

    Returns:
        Updated run record
    """
    updated_run = await run_ledger_service.update_run_status(
        db=db,
        run_id=run_id,
        status=run_update.status,
        scheduler_job_id=run_update.scheduler_job_id,
        scheduler_name=run_update.scheduler_name,
        workflow_type=run_update.workflow_type,
        workflow_manifest=run_update.workflow_manifest,
        error=run_update.last_error,
    )

    return updated_run
# - GET /runs - list runs with filtering
# - GET /runs/{id} - get run details
# - POST /runs/{id}/retry - retry failed run
# - GET /runs/{id}/provenance - get run provenance
