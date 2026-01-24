from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer

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
            examples=["HIPASSJ1318-21"],
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
    last_checked_at: datetime | None = Field(default=None, description="Last discovery check timestamp")

    @field_serializer("last_checked_at")
    def serialize_last_checked_at(self, last_checked_at: datetime | None, _info):  # type: ignore[override]
        if last_checked_at is not None:
            return last_checked_at.isoformat()
        return None


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


class SourceRegistryBulkCreate(BaseModel):
    """Schema for bulk source registration."""

    model_config = ConfigDict(extra="forbid")
    items: Annotated[
        list[SourceRegistryCreate],
        Field(min_length=1, description="List of sources to register"),
    ]


class SourceRegistryBulkCreateResponse(BaseModel):
    """Response for bulk source registration."""

    model_config = ConfigDict(extra="forbid")
    created: list[SourceRegistryRead]
    existing: list[SourceRegistryRead]
    total_created: int
    total_existing: int

