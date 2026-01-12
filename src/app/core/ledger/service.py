"""Run ledger service.

Provides idempotent run tracking and duplicate prevention.
"""
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_run_record import crud_run_records
from ...models.ledger import RunRecord, RunStatus
from ...schemas.ledger import RunRecordCreateInternal, RunRecordRead, RunRecordUpdateInternal
from ..config import settings
from ..exceptions.http_exceptions import BadRequestException, NotFoundException

logger = logging.getLogger(__name__)


class RunLedgerService:
    @staticmethod
    async def check_existing_run(
        db: AsyncSession,
        project_module: str,
        source_identifier: str,
        dataset_id: str,
    ) -> dict[str, Any] | None:
        """Check if a run already exists for the given key.

        Args:
            db: Database session
            project_module: Project module identifier
            source_identifier: Source identifier
            dataset_id: Dataset identifier

        Returns:
            Existing RunRecord if found, None otherwise
        """
        try:
            run = await crud_run_records.get(
                db=db,
                project_module=project_module,
                source_identifier=source_identifier,
                dataset_id=dataset_id,
                schema_to_select=RunRecordRead,
            )
            return run
        except Exception as e:
            logger.exception(
                f"Error checking existing run for {project_module}/{source_identifier}/{dataset_id}: {e}"
            )
            return None

    @staticmethod
    async def create_run(
        db: AsyncSession,
        project_module: str,
        source_identifier: str,
        dataset_id: str,
        archive_name: str,
        dataset_metadata: dict | None = None,
        created_by_id: int | None = None,
    ) -> dict[str, Any]:
        """Create a new run record with check.

        If a run exist with the same key, returns the existing run.
        Otherwise, creates a new run

        Args:
            db: Database session
            project_module: Project module identifier
            source_identifier: Source identifier
            dataset_id: Dataset identifier
            archive_name: Archive name
            dataset_metadata: Full dataset metadata
            created_by_id: User ID who triggered the run

        Returns:
            RunRecord (existing or newly created)
        """
        # Check for existing
        existing = await RunLedgerService.check_existing_run(
            db, project_module, source_identifier, dataset_id
        )
        if existing:
            existing_uuid = existing.get("uuid")
            logger.info(
                f"Run already exists for {project_module}/{source_identifier}/{dataset_id}, "
                f"returning existing run {existing_uuid}"
            )
            return existing

        # Create new run
        try:
            run_data = RunRecordCreateInternal(
                project_module=project_module,
                source_identifier=source_identifier,
                dataset_id=dataset_id,
                archive_name=archive_name,
                dataset_metadata=dataset_metadata,
                created_by_id=created_by_id,
                status=RunStatus.PENDING,
            )
            run = await crud_run_records.create(
                db=db, object=run_data, schema_to_select=RunRecordRead
            )
            run_uuid = run.get("uuid")
            logger.info(f"Created new run {run_uuid} for {project_module}/{source_identifier}/{dataset_id}")
            return run
        except Exception as e:
            logger.exception(f"Error creating run for {project_module}/{source_identifier}/{dataset_id}: {e}")
            raise

    @staticmethod
    def _validate_status_transition(current_status: RunStatus, new_status: RunStatus) -> bool:
        allowed_transitions = {
            RunStatus.PENDING: [RunStatus.RUNNING, RunStatus.CANCELLED],
            RunStatus.RUNNING: [RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED],
            RunStatus.COMPLETED: [],  # no more transitions allowed
            RunStatus.FAILED: [RunStatus.RETRYING, RunStatus.CANCELLED],
            RunStatus.RETRYING: [RunStatus.RUNNING, RunStatus.FAILED, RunStatus.CANCELLED],
            RunStatus.CANCELLED: [],  # no more transitions allowed
        }
        return new_status in allowed_transitions.get(current_status, [])

    @staticmethod
    async def update_run_status(
        db: AsyncSession,
        run_id: UUID,
        status: RunStatus | None = None,
        scheduler_job_id: str | None = None,
        scheduler_name: str | None = None,
        workflow_type: str | None = None,
        workflow_manifest: dict | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Update run status and related fields.

        Args:
            db: Database session
            run_id: Run UUID
            status: New status for the run
            scheduler_job_id: ID from the HPC scheduler
            scheduler_name: Name of the scheduler
            workflow_type: Type of workflow
            workflow_manifest: JSON manifest of the workflow
            error: Error message if the run failed

        Returns:
            Updated run record

        Raises:
            NotFoundException: If run not found
            BadRequestException: If status transition is invalid
        """
        # Get existing
        run = await crud_run_records.get(db=db, uuid=run_id, schema_to_select=RunRecordRead)
        if not run:
            raise NotFoundException(f"Run {run_id} not found")

        current_status_value = run.get("status")
        started_at_value = run.get("started_at")
        completed_at_value = run.get("completed_at")

        # status transition if status is being changed
        if status and status != current_status_value:
            current_status = RunStatus(current_status_value)
            if not RunLedgerService._validate_status_transition(current_status, status):
                raise BadRequestException(
                    f"Invalid status transition from {current_status.value} to {status.value}"
                )

        # Prepare
        update_data: dict[str, Any] = {}
        now = datetime.now(UTC)

        if status:
            update_data["status"] = status
            # timestamp management
            if status == RunStatus.RUNNING and not started_at_value:
                update_data["started_at"] = now
            elif status in [RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED]:
                if not completed_at_value:
                    update_data["completed_at"] = now

        if scheduler_job_id is not None:
            update_data["scheduler_job_id"] = scheduler_job_id
        if scheduler_name is not None:
            update_data["scheduler_name"] = scheduler_name
        if workflow_type is not None:
            update_data["workflow_type"] = workflow_type
        if workflow_manifest is not None:
            update_data["workflow_manifest"] = workflow_manifest
        if error is not None:
            update_data["last_error"] = error

        # Always update updated_at
        update_data["updated_at"] = now

        if not update_data:
            # No changes to make
            return run

        # Update run
        update_schema = RunRecordUpdateInternal(**update_data)
        await crud_run_records.update(
            db=db, object=update_schema, uuid=run_id
        )

        # Fetch
        updated_run = await crud_run_records.get(
            db=db, uuid=run_id, schema_to_select=RunRecordRead
        )
        if not updated_run:
            raise NotFoundException(f"Run {run_id} not found after update")

        logger.info(
            f"Updated run {run_id}: status={status}, scheduler_job_id={scheduler_job_id}"
        )
        return updated_run

    # @staticmethod
    # async def mark_for_retry(
    #     db: AsyncSession,
    #     run_id: UUID,
    # ) -> RunRecord:



# instance
run_ledger_service = RunLedgerService()
