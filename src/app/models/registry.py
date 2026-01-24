import uuid as uuid_pkg
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from uuid6 import uuid7

from ..core.db.database import Base


class SourceRegistry(Base):
    __tablename__ = "source_registry"

    # Required
    project_module: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    source_identifier: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    # default
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default_factory=lambda: datetime.now(UTC), nullable=False
    )

    # Optional
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
    last_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None, index=True
    )

    uuid: Mapped[uuid_pkg.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default_factory=uuid7, unique=True, init=False
    )

    # unique constraint
    __table_args__ = (
        UniqueConstraint(
            "project_module", "source_identifier", name="uq_source_registry_composite"
        ),
        Index("idx_source_registry_enabled", "enabled"),
    )

