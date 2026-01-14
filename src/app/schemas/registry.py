from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from ..core.schemas import TimestampSchema, UUIDSchema

class SourceRegistryBase(BaseModel):
    project_module: Annotated[
        str, Field(min_length=1, max_length=50, examples=["wallaby"], description="Project module identifier")
    ]
    source_identifier: Annotated[
        str,
        Field(
            min_length=1,
            max_length=100,
            examples=["HIPASSJ1303+07"],
            description="Source identifier",
        ),
    ]


class SourceRegistryCreate(SourceRegistryBase):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=False, description="monitoring is enabled for this source?")


class SourceRegistryCreateInternal(SourceRegistryCreate):
    pass


class SourceRegistryRead(TimestampSchema, SourceRegistryBase, UUIDSchema):
    model_config = ConfigDict(from_attributes=True)

    enabled: bool


class SourceRegistryUpdate(BaseModel):
    """Schema for updating source registry entries via API."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool | None = Field(default=None, description="monitoring is enabled for this source?")


class SourceRegistryUpdateInternal(SourceRegistryUpdate):
    updated_at: datetime


class SourceRegistryDelete(BaseModel):
    model_config = ConfigDict(extra="forbid")
    is_deleted: bool = Field(default=True, description="Soft delete flag for the source registry entry")
    deleted_at: datetime | None = Field(default=None, description="Timestamp when the record was deleted")

