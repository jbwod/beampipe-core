from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from ..core.schemas import TimestampSchema, UUIDSchema
from ..models.ledger import RunStatus


# /Users/jblackwo/beampipe-core/docs/user-guide/database/schemas.md
class RunSourceSpec(BaseModel):
    """Per-source spec with optional SBID filter."""

    source_identifier: Annotated[str, Field(min_length=1, max_length=100)]
    sbids: list[str] | None = Field(default=None, description="Optional: restrict to these SBIDs for this source")


class BatchRunRecordBase(BaseModel):
    project_module: Annotated[
        str, Field(min_length=1, max_length=50, examples=["wallaby_hires"], description="Project module identifier")
    ]
    sources: Annotated[
        list[RunSourceSpec],
        Field(min_length=1, description="Sources with optional per-source SBID filters"),
    ]
    archive_name: Annotated[
        str, Field(min_length=1, max_length=50, examples=["casda"], description="Archive name")
    ]


class BatchRunRecordCreate(BatchRunRecordBase):
    model_config = ConfigDict(extra="forbid")

    execution_profile_id: UUID | None = Field(default=None, description="DALiuGE execution profile to use")
    created_by_id: int | None = Field(default=None, description="User ID who triggered the run")


class BatchRunRecordCreateInternal(BatchRunRecordCreate):
    status: RunStatus = Field(default=RunStatus.PENDING, description="Initial run status")


class BatchRunRecordRead(TimestampSchema, BatchRunRecordBase, UUIDSchema):
    model_config = ConfigDict(from_attributes=True)

    execution_profile_id: UUID | None = None
    status: RunStatus
    workflow_manifest: dict | None = None
    scheduler_name: str | None = None
    scheduler_job_id: str | None = None
    retry_count: int = 0
    last_error: str | None = None
    created_by_id: int | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class BatchRunRecordUpdate(BaseModel):
    """Schema for updating run records via API.

    Note: status, started_at, and completed_at are managed automatically
    by the service layer based on status transitions.
    """
    model_config = ConfigDict(extra="forbid")

    status: RunStatus | None = Field(default=None, description="New run status")
    workflow_manifest: dict | None = Field(default=None, description="Workflow manifest JSON")
    scheduler_name: str | None = Field(default=None, max_length=50, description="Name of scheduler")
    scheduler_job_id: str | None = Field(default=None, max_length=100, description="Scheduler job ID")
    last_error: str | None = Field(default=None, description="Error message if run failed")


class BatchRunRecordUpdateInternal(BatchRunRecordUpdate):
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None


class BatchRunRecordDelete(BaseModel):
    model_config = ConfigDict(extra="forbid")
    is_deleted: bool = Field(default=True, description="Soft delete flag for the run record")
    deleted_at: datetime | None = Field(default=None, description="Timestamp when the record was deleted")


# Prepare run (validate + preview, no DB write)
class PrepareRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_module: Annotated[str, Field(min_length=1, max_length=50)]
    sources: Annotated[list[RunSourceSpec], Field(min_length=1)]


class PrepareRunResponse(BaseModel):
    """Preview of what would be included in a run."""

    project_module: str
    sources: list[RunSourceSpec]
    sources_preview: list[dict]  # per-source: source_identifier, sbid_count, dataset_count
    total_datasets: int
    valid: bool
    errors: list[str] = Field(default_factory=list)
