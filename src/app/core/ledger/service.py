"""Execution ledger service.

Provides execution tracking for batch workflow submissions (multiple sources, datasets).
"""
import asyncio
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
from ..registry.service import source_registry_service
from .source_readiness import parse_execution_source_spec, parsed_source_readiness_error

logger = logging.getLogger(__name__)

_EXECUTION_PHASE_UNSET: object = object()


def _validate_parsed_source_for_execution(
    sid: str,
    sbids: list[str] | None,
    registered: dict[str, Any] | None,
    all_rows_for_source: list[dict[str, Any]],
) -> None:
    err = parsed_source_readiness_error(sid, sbids, registered, all_rows_for_source)
    if err:
        raise BadRequestException(err)


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
    async def partition_sources_ready_for_execution(
        db: AsyncSession,
        project_module: str,
        sources: list,
    ) -> tuple[list[Any], list[dict[str, str]]]:
        valid: list[Any] = []
        skipped: list[dict[str, str]] = []
        parsed_ok: list[tuple[Any, str, list[str] | None]] = []

        for spec in sources:
            raw_sid = (
                spec.get("source_identifier")
                if isinstance(spec, dict)
                else getattr(spec, "source_identifier", None)
            )
            parse_err, sid, sbids = parse_execution_source_spec(spec)
            if parse_err:
                skipped.append(
                    {
                        "source_identifier": str(raw_sid) if raw_sid else "",
                        "reason": parse_err,
                    }
                )
                continue
            assert sid is not None
            parsed_ok.append((spec, sid, sbids))

        if not parsed_ok:
            return valid, skipped

        unique_sids = list(dict.fromkeys(s for _, s, _ in parsed_ok))
        registry_map, metadata_map = await asyncio.gather(
            source_registry_service.get_registry_read_by_identifiers(
                db, project_module, unique_sids
            ),
            archive_metadata_service.list_metadata_grouped_by_sources(
                db, project_module, unique_sids
            ),
        )

        for spec, sid, sbids in parsed_ok:
            err = parsed_source_readiness_error(
                sid,
                sbids,
                registry_map.get(sid),
                metadata_map.get(sid, []),
            )
            if err:
                skipped.append({"source_identifier": sid, "reason": err})
            else:
                valid.append(spec)

        return valid, skipped

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

        Validates all sources are registed and enabled, and all checks pass.

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
        parsed: list[tuple[str, list[str] | None]] = []
        for spec in sources:
            parse_err, sid, sbids = parse_execution_source_spec(spec)
            if parse_err:
                raise BadRequestException(parse_err)
            assert sid is not None
            parsed.append((sid, sbids))

        unique_sids = list(dict.fromkeys(s for s, _ in parsed))
        registry_map, metadata_map = await asyncio.gather(
            source_registry_service.get_registry_read_by_identifiers(
                db, project_module, unique_sids
            ),
            archive_metadata_service.list_metadata_grouped_by_sources(
                db, project_module, unique_sids
            ),
        )
        for sid, sbids in parsed:
            _validate_parsed_source_for_execution(
                sid,
                sbids,
                registry_map.get(sid),
                metadata_map.get(sid, []),
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
            ExecutionStatus.RUNNING: [
                ExecutionStatus.COMPLETED,
                ExecutionStatus.NOT_SUBMITTED,
                ExecutionStatus.FAILED,
                ExecutionStatus.CANCELLED,
            ],
            ExecutionStatus.COMPLETED: [],  # no more transitions allowed
            # Manifest-only run finished; may re-execute with submit enabled.
            ExecutionStatus.NOT_SUBMITTED: [ExecutionStatus.RUNNING, ExecutionStatus.CANCELLED],
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
            elif status in [
                ExecutionStatus.COMPLETED,
                ExecutionStatus.NOT_SUBMITTED,
                ExecutionStatus.FAILED,
                ExecutionStatus.CANCELLED,
            ]:
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
