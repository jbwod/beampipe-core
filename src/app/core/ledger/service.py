"""Execution ledger service.

Provides execution tracking for batch workflow submissions (multiple sources, datasets).
"""
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_execution_record import crud_batch_execution_records
from ...models.ledger import BatchExecutionRecord, ExecutionPhase, ExecutionStatus
from ...schemas.ledger import BatchExecutionRecordCreateInternal, BatchExecutionRecordRead
from ..archive.service import archive_metadata_service
from ..config import settings
from ..exceptions.http_exceptions import BadRequestException, NotFoundException
from .source_readiness import parse_execution_source_spec, parsed_source_readiness_error
from ..registry.service import source_registry_service

logger = logging.getLogger(__name__)

_EXECUTION_PHASE_UNSET: object = object()


class ExecutionLedgerService:
    @staticmethod
    async def count_in_flight_auto_executions_for_module(
        db: AsyncSession,
        project_module: str,
    ) -> int:
        """Count PENDING/RUNNING/RETRYING executions for this module under the automation scheduler."""
        result = await db.execute(
            select(func.count(BatchExecutionRecord.uuid)).where(
                and_(
                    BatchExecutionRecord.project_module == project_module,
                    BatchExecutionRecord.scheduler_name == settings.WORKFLOW_AUTOMATION_SCHEDULER_NAME,
                    BatchExecutionRecord.status.in_(
                        [
                            ExecutionStatus.PENDING,
                            ExecutionStatus.RUNNING,
                            ExecutionStatus.RETRYING,
                        ]
                    ),
                )
            )
        )
        return int(result.scalar() or 0)

    @staticmethod
    async def create_execution(
        db: AsyncSession,
        project_module: str,
        sources: list,
        archive_name: str,
        *,
        deployment_profile_id: UUID | None = None,
        created_by_id: int | None = None,
    ) -> dict[str, Any]:
        """Create a new batch execution record.

        Validates all sources are registered and enabled.

        Args:
            db: Database session
            project_module: Project module identifier
            sources: List of ExecutionSourceSpec (source_identifier, optional sbids per source)
            archive_name: Archive name (e.g. casda)
            created_by_id: User ID who triggered the execution

        Returns:
            BatchExecutionRecord (newly created)

        Raises:
            BadRequestException: If any source is not registered or disabled
        """
        for spec in sources:
            sid, _, err = await validate_source_spec(db, project_module, spec)
            if err:
                raise BadRequestException(err)

            sbids = spec.get("sbids") if isinstance(spec, dict) else getattr(spec, "sbids", None)
            metadata = await archive_metadata_service.list_metadata_for_source(
                db=db,
                project_module=project_module,
                source_identifier=sid,
                sbids=sbids,
            )
            if not metadata:
                hint = f" (SBIDs: {sbids})" if sbids else ""
                raise BadRequestException(
                    f"Source {sid} has no discovered metadata{hint}. "
                    f"(POST /api/v1/sources/discover)."
                )

        try:
            execution_data = BatchExecutionRecordCreateInternal(
                project_module=project_module,
                sources=sources,
                archive_name=archive_name,
                deployment_profile_id=deployment_profile_id,
                created_by_id=created_by_id,
                status=ExecutionStatus.PENDING,
            )
            execution = await crud_batch_execution_records.create(
                db=db, object=execution_data, schema_to_select=BatchExecutionRecordRead
            )
            execution_uuid = execution.get("uuid")
            logger.info(
                "event=ledger_execution_created "
                "execution_uuid=%s project_module=%s source_count=%s",
                execution_uuid,
                project_module,
                len(sources),
            )
            return execution
        except Exception as e:
            logger.exception(
                "event=ledger_execution_create_error "
                "project_module=%s sources=%s error=%s",
                project_module,
                sources,
                e,
            )
            raise

    @staticmethod
    def _validate_status_transition(current_status: ExecutionStatus, new_status: ExecutionStatus) -> bool:
        allowed_transitions = {
            ExecutionStatus.PENDING: [ExecutionStatus.RUNNING, ExecutionStatus.CANCELLED],
            ExecutionStatus.RUNNING: [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED],
            ExecutionStatus.COMPLETED: [],  # no more transitions allowed
            # FAILED -> RUNNING: worker/ARQ retry picks up the same execution after a transient error
            ExecutionStatus.FAILED: [ExecutionStatus.RETRYING, ExecutionStatus.CANCELLED, ExecutionStatus.RUNNING],
            ExecutionStatus.RETRYING: [ExecutionStatus.RUNNING, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED],
            ExecutionStatus.CANCELLED: [],  # no more transitions allowed
        }
        return new_status in allowed_transitions.get(current_status, [])

    @staticmethod
    async def update_execution_status(
        db: AsyncSession,
        execution_id: UUID,
        status: ExecutionStatus | None = None,
        scheduler_job_id: str | None = None,
        scheduler_name: str | None = None,
        workflow_manifest: dict | None = None,
        error: str | None = None,
        execution_phase: ExecutionPhase | None | object = _EXECUTION_PHASE_UNSET,
    ) -> dict[str, Any]:
        """Update execution status and related fields.

        Args:
            db: Database session
            execution_id: Execution UUID
            status: New status for the execution
            scheduler_job_id: ID from the HPC scheduler
            scheduler_name: Name of the scheduler
            workflow_manifest: JSON manifest of the workflow
            error: Error message if the execution failed
            execution_phase: Checkpoint for execute workflow retries; pass None to clear the column

        Returns:
            Updated execution record

        Raises:
            NotFoundException: If execution not found
            BadRequestException: If status transition is invalid
        """
        execution = await crud_batch_execution_records.get(
            db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
        )
        if not execution:
            raise NotFoundException(f"Execution {execution_id} not found")

        current_status_value = execution.get("status")
        started_at_value = execution.get("started_at")
        completed_at_value = execution.get("completed_at")

        if status and status != current_status_value and current_status_value is not None:
            current_status = ExecutionStatus(str(current_status_value))
            if not ExecutionLedgerService._validate_status_transition(current_status, status):
                raise BadRequestException(
                    f"Invalid status transition from {current_status.value} to {status.value}"
                )

        update_data: dict[str, Any] = {}
        now = datetime.now(UTC)

        if status:
            update_data["status"] = status
            if status == ExecutionStatus.RUNNING and not started_at_value:
                update_data["started_at"] = now
            elif status in [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED]:
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
        if execution_phase is not _EXECUTION_PHASE_UNSET:
            update_data["execution_phase"] = execution_phase

        update_data["updated_at"] = now

        if not update_data:
            return execution

        await crud_batch_execution_records.update(db=db, object=update_data, uuid=execution_id)

        updated_execution = await crud_batch_execution_records.get(
            db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
        )
        if not updated_execution:
            raise NotFoundException(f"Execution {execution_id} not found after update")

        logger.info(
            "event=ledger_execution_updated execution_id=%s status=%s scheduler_job_id=%s",
            execution_id,
            status,
            scheduler_job_id,
        )
        return updated_execution


execution_ledger_service = ExecutionLedgerService()
