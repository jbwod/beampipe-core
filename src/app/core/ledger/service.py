"""Run ledger service.

Provides run tracking for batch workflow submissions (multiple sources, datasets).
"""
import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID

from ...crud.crud_run_record import crud_batch_run_records
from ...models.ledger import RunStatus
from ...schemas.ledger import BatchRunRecordCreateInternal, BatchRunRecordRead
from ..exceptions.http_exceptions import BadRequestException, NotFoundException
from ..utils.registry import validate_source_spec

logger = logging.getLogger(__name__)


class RunLedgerService:
    @staticmethod
    async def create_run(
        db: AsyncSession,
        project_module: str,
        sources: list,
        archive_name: str,
        created_by_id: int | None = None,
    ) -> dict[str, Any]:
        """Create a new batch run record.

        Validates all sources are registered and enabled.

        Args:
            db: Database session
            project_module: Project module identifier
            sources: List of RunSourceSpec (source_identifier, optional sbids per source)
            archive_name: Archive name (e.g. casda)
            created_by_id: User ID who triggered the run

        Returns:
            BatchRunRecord (newly created)

        Raises:
            BadRequestException: If any source is not registered or disabled
        """
        # Validate all sources are registered and enabled
        for spec in sources:
            _, _, err = await validate_source_spec(db, project_module, spec)
            if err:
                raise BadRequestException(err)

        try:
            run_data = BatchRunRecordCreateInternal(
                project_module=project_module,
                sources=sources,
                archive_name=archive_name,
                created_by_id=created_by_id,
                status=RunStatus.PENDING,
            )
            run = await crud_batch_run_records.create(
                db=db, object=run_data, schema_to_select=BatchRunRecordRead
            )
            run_uuid = run.get("uuid")
            logger.info(
                "event=ledger_run_created "
                "run_uuid=%s project_module=%s source_count=%s",
                run_uuid,
                project_module,
                len(sources),
            )
            return run
        except Exception as e:
            logger.exception(
                "event=ledger_run_create_error "
                "project_module=%s sources=%s error=%s",
                project_module,
                sources,
                e,
            )
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
            workflow_manifest: JSON manifest of the workflow
            error: Error message if the run failed

        Returns:
            Updated run record

        Raises:
            NotFoundException: If run not found
            BadRequestException: If status transition is invalid
        """
        run = await crud_batch_run_records.get(
            db=db, uuid=run_id, schema_to_select=BatchRunRecordRead
        )
        if not run:
            raise NotFoundException(f"Run {run_id} not found")

        current_status_value = run.get("status")
        started_at_value = run.get("started_at")
        completed_at_value = run.get("completed_at")

        if status and status != current_status_value and current_status_value is not None:
            current_status = RunStatus(str(current_status_value))
            if not RunLedgerService._validate_status_transition(current_status, status):
                raise BadRequestException(
                    f"Invalid status transition from {current_status.value} to {status.value}"
                )

        update_data: dict[str, Any] = {}
        now = datetime.now(UTC)

        if status:
            update_data["status"] = status
            if status == RunStatus.RUNNING and not started_at_value:
                update_data["started_at"] = now
            elif status in [RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED]:
                if not completed_at_value:
                    update_data["completed_at"] = now

        if scheduler_job_id is not None:
            update_data["scheduler_job_id"] = scheduler_job_id
        if scheduler_name is not None:
            update_data["scheduler_name"] = scheduler_name
        if workflow_manifest is not None:
            update_data["workflow_manifest"] = workflow_manifest
        if error is not None:
            update_data["last_error"] = error

        update_data["updated_at"] = now

        if not update_data:
            return run

        await crud_batch_run_records.update(db=db, object=update_data, uuid=run_id)

        updated_run = await crud_batch_run_records.get(
            db=db, uuid=run_id, schema_to_select=BatchRunRecordRead
        )
        if not updated_run:
            raise NotFoundException(f"Run {run_id} not found after update")

        logger.info(
            "event=ledger_run_updated run_id=%s status=%s scheduler_job_id=%s",
            run_id,
            status,
            scheduler_job_id,
        )
        return updated_run


run_ledger_service = RunLedgerService()
