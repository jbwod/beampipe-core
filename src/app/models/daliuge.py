import uuid as uuid_pkg
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Index, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from uuid6 import uuid7

from ..core.db.database import Base


class DaliugeDeploymentProfile(Base):
    """DALiuGE deployment profile with nested translation/deployment JSON blobs."""

    __tablename__ = "daliuge_deployment_profile"

    uuid: Mapped[uuid_pkg.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default_factory=uuid7, unique=True, init=False
    )
    name: Mapped[str] = mapped_column(String(50), nullable=False, unique=True, index=True)
    description: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None)
    project_module: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True, default=None)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    translation: Mapped[dict] = mapped_column(JSONB, nullable=False, default_factory=dict)
    deployment: Mapped[dict] = mapped_column(JSONB, nullable=False, default_factory=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default_factory=lambda: datetime.now(UTC), nullable=False
    )
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)

    __table_args__ = (
        Index("idx_daliuge_profile_project_default", "project_module", "is_default"),
    )
