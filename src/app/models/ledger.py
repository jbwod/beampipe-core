import uuid as uuid_pkg
from datetime import UTC, datetime
from enum import Enum

from sqlalchemy import DateTime, Enum as SQLEnum, ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from uuid6 import uuid7

from ..core.db.database import Base

class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class RunRecord(Base):
    __tablename__ = "run_record"

    # Required
    project_module: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    source_identifier: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    archive_name: Mapped[str] = mapped_column(String(50), nullable=False)
    dataset_id: Mapped[str] = mapped_column(String(255), nullable=False)

    # Optional
    dataset_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True, default=None)
    workflow_manifest: Mapped[dict | None] = mapped_column(JSONB, nullable=True, default=None)
    workflow_type: Mapped[str | None] = mapped_column(String(50), nullable=True, default=None)
    scheduler_name: Mapped[str | None] = mapped_column(String(50), nullable=True, default=None)
    scheduler_job_id: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True, default=None)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    created_by_id: Mapped[int | None] = mapped_column(
        ForeignKey("user.id"), nullable=True, index=True, default=None
    )
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)

    # default
    status: Mapped[RunStatus] = mapped_column(
        SQLEnum(RunStatus), default=RunStatus.PENDING, nullable=False, index=True
    )
    retry_count: Mapped[int] = mapped_column(default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default_factory=lambda: datetime.now(UTC), nullable=False
    )

    # Primary
    uuid: Mapped[uuid_pkg.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default_factory=uuid7, unique=True, init=False
    )

    # unique constraint
    __table_args__ = (
        UniqueConstraint(
            "project_module", "source_identifier", "dataset_id", name="uq_run_record_composite"
        ),
        Index("idx_run_record_status", "status"),
    )