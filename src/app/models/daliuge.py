import uuid as uuid_pkg
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Index, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from uuid6 import uuid7

from ..core.db.database import Base


class DaliugeExecutionProfile(Base):
    """DALiuGE execution profile: partitioning, URLs, ports for graph translation and deploy."""

    __tablename__ = "daliuge_execution_profile"

    # Identity (required first for dataclass field order)
    uuid: Mapped[uuid_pkg.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default_factory=uuid7, unique=True, init=False
    )
    name: Mapped[str] = mapped_column(String(50), nullable=False, unique=True, index=True)
    tm_url: Mapped[str] = mapped_column(String(255), nullable=False)
    dim_host_for_tm: Mapped[str] = mapped_column(String(100), nullable=False)
    dim_port_for_tm: Mapped[int] = mapped_column(nullable=False)
    deploy_host: Mapped[str] = mapped_column(String(100), nullable=False)

    # Optional with defaults
    description: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)
    project_module: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True, default=None)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    algo: Mapped[str] = mapped_column(String(20), nullable=False, default="metis")
    num_par: Mapped[int] = mapped_column(nullable=False, default=1)
    num_islands: Mapped[int] = mapped_column(nullable=False, default=0)
    deploy_port: Mapped[int] = mapped_column(nullable=False, default=8001)
    verify_ssl: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default_factory=lambda: datetime.now(UTC), nullable=False
    )
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)

    __table_args__ = (
        Index("idx_daliuge_profile_project_default", "project_module", "is_default"),
    )
