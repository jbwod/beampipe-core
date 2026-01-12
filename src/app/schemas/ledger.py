from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from ..core.schemas import TimestampSchema, UUIDSchema
from ..models.ledger import RunStatus

# /Users/jblackwo/beampipe-core/docs/user-guide/database/schemas.md
class RunRecordBase(BaseModel):
    project_module: Annotated[
        str, Field(min_length=1, max_length=50, examples=["wallaby"], description="Project module identifier")
    ]
    source_identifier: Annotated[
        str,
        Field(
            min_length=1,
            max_length=100,
            examples=["HIPASSXXXX+XX"],
            description="Source identifier",
        ),
    ]
    archive_name: Annotated[
        str, Field(min_length=1, max_length=50, examples=["casda"], description="Archive name")
    ]
    dataset_id: Annotated[
        str,
        Field(
            min_length=1,
            max_length=255,
            examples=["SBXXXXX_visibilities.ms.tar"],
            description="Dataset identifier",
        ),
    ]


class RunRecordCreate(RunRecordBase):
    model_config = ConfigDict(extra="forbid")

    dataset_metadata: dict | None = Field(default=None, description="Full dataset metadata")
    created_by_id: int | None = Field(default=None, description="User ID who triggered the run")


class RunRecordCreateInternal(RunRecordCreate):
    status: RunStatus = Field(default=RunStatus.PENDING, description="Initial run status")


class RunRecordRead(TimestampSchema, RunRecordBase, UUIDSchema):
    model_config = ConfigDict(from_attributes=True)

    dataset_metadata: dict | None = None
    status: RunStatus
    workflow_manifest: dict | None = None
    workflow_type: str | None = None
    scheduler_name: str | None = None
    scheduler_job_id: str | None = None
    retry_count: int = 0
    last_error: str | None = None
    created_by_id: int | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class RunRecordUpdate(BaseModel):
    """Schema for updating run records via API.

    Note: status, started_at, and completed_at are managed automatically
    by the service layer based on status transitions.
    """
    model_config = ConfigDict(extra="forbid")

    status: RunStatus | None = Field(default=None, description="New run status")
    workflow_manifest: dict | None = Field(default=None, description="Workflow manifest JSON")
    workflow_type: str | None = Field(default=None, max_length=50, description="Type of workflow")
    scheduler_name: str | None = Field(default=None, max_length=50, description="Name of scheduler")
    scheduler_job_id: str | None = Field(default=None, max_length=100, description="Scheduler job ID")
    last_error: str | None = Field(default=None, description="Error message if run failed")


class RunRecordUpdateInternal(RunRecordUpdate):
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None



class RunRecordDelete(BaseModel):
    model_config = ConfigDict(extra="forbid")
    is_deleted: bool = Field(default=True, description="Soft delete flag for the run record")
    deleted_at: datetime | None = Field(default=None, description="Timestamp when the record was deleted")

