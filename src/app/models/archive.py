import uuid as uuid_pkg
from datetime import UTC, datetime

from sqlalchemy import DateTime, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from uuid6 import uuid7

from ..core.db.database import Base


class ArchiveMetadata(Base):
    __tablename__ = "archive_metadata"

    # Required
    project_module: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    source_identifier: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    sbid: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Optional
    metadata_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True, default=None)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)

    # default
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default_factory=lambda: datetime.now(UTC), nullable=False
    )

    # Primary
    uuid: Mapped[uuid_pkg.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default_factory=uuid7, unique=True, init=False
    )

    __table_args__ = (
        UniqueConstraint(
            "project_module", "source_identifier", "sbid", name="uq_archive_metadata_composite"
        ),
        Index("idx_archive_metadata_project_source", "project_module", "source_identifier"),
    )
