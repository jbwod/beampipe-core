"""Run ledger service.

Provides idempotent run tracking and duplicate prevention.
"""
import logging
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_run_record import crud_run_records
from ...models.ledger import RunRecord, RunStatus
from ...schemas.ledger import RunRecordCreateInternal
from ..config import settings

logger = logging.getLogger(__name__)


class RunLedgerService:
    @staticmethod
    async def check_existing_run(
        db: AsyncSession,
        project_module: str,
        source_identifier: str,
        dataset_id: str,
    ) -> RunRecord | None:
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
    ) -> RunRecord:
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
            logger.info(
                f"Run already exists for {project_module}/{source_identifier}/{dataset_id}, "
                f"returning existing run {existing.uuid}"
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
            run = await crud_run_records.create(db=db, object=run_data)
            logger.info(f"Created new run {run.uuid} for {project_module}/{source_identifier}/{dataset_id}")
            return run
        except Exception as e:
            logger.exception(f"Error creating run for {project_module}/{source_identifier}/{dataset_id}: {e}")
            raise

    # @staticmethod
    # async def update_run_status(
    #     db: AsyncSession,
    #     run_id: UUID,
    #     status: RunStatus,
    #     scheduler_job_id: str | None = None,
    #     error: str | None = None,
    #     workflow_manifest: dict | None = None,
    #     workflow_batch_id: UUID | None = None,
    # ) -> RunRecord:

    # @staticmethod
    # async def mark_for_retry(
    #     db: AsyncSession,
    #     run_id: UUID,
    # ) -> RunRecord:



# instance
run_ledger_service = RunLedgerService()
