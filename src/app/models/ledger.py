import uuid as uuid_pkg
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import DateTime, ForeignKey, Index, String, Text
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from uuid6 import uuid7

from ..core.db.database import Base


class ExecutionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    NOT_SUBMITTED = "not_submitted"
    AWAITING_SCHEDULER = "awaiting_scheduler"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class ExecutionPhase(StrEnum):
    """
    STAGE_AND_MANIFEST: staging (if enabled) and manifest build not yet persisted.
    SUBMIT: workflow_manifest is on the row; remainder is graph resolve + TM/DIM (or slurm).
    """

    STAGE_AND_MANIFEST = "stage_and_manifest"
    SUBMIT = "submit"


class BatchExecutionRecord(Base):
    __tablename__ = "batch_execution_record"

    # Required
    project_module: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    sources: Mapped[list] = mapped_column(JSONB, nullable=False)  # list[{source_identifier, sbids?}]
    archive_name: Mapped[str] = mapped_column(String(50), nullable=False)

    # Optional workflow
    deployment_profile_id: Mapped[uuid_pkg.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("daliuge_deployment_profile.uuid"),
        nullable=True,
        index=True,
        default=None,
    )
    workflow_manifest: Mapped[dict | None] = mapped_column(JSONB, nullable=True, default=None)
    execution_phase: Mapped[ExecutionPhase | None] = mapped_column(
        SQLEnum(ExecutionPhase, native_enum=False, length=32),
        nullable=True,
        default=None,
    )
    scheduler_name: Mapped[str | None] = mapped_column(String(50), nullable=True, default=None)
    scheduler_job_id: Mapped[str | None] = mapped_column(String(512), nullable=True, index=True, default=None)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    created_by_id: Mapped[int | None] = mapped_column(
        ForeignKey("user.id"), nullable=True, index=True, default=None
    )
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)

    # default
    status: Mapped[ExecutionStatus] = mapped_column(
        SQLEnum(ExecutionStatus), default=ExecutionStatus.PENDING, nullable=False, index=True
    )
    retry_count: Mapped[int] = mapped_column(default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default_factory=lambda: datetime.now(UTC), nullable=False
    )

    # Primary
    uuid: Mapped[uuid_pkg.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default_factory=uuid7, unique=True, init=False
    )

    __table_args__ = (Index("idx_batch_execution_record_status", "status"),)
