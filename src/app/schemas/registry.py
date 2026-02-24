from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer, model_validator

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
    last_attempted_at: datetime | None = Field(
        default=None,
        description="Last failed discovery attempt timestamp (retry cooldown anchor).",
    )
    stale_after_hours: int | None = Field(
        default=None,
        description="Recheck after this many hours; null = use default (DISCOVERY_STALE_HOURS)",
    )
    discovery_signature: str | None = Field(
        default=None,
        description="Hash of last discovery state; used to skip writes when unchanged.",
    )

    @field_serializer("last_checked_at")
    def serialize_last_checked_at(self, last_checked_at: datetime | None, _info):  # type: ignore[override]
        if last_checked_at is not None:
            return last_checked_at.isoformat()
        return None

    @field_serializer("last_attempted_at")
    def serialize_last_attempted_at(self, last_attempted_at: datetime | None, _info):  # type: ignore[override]
        if last_attempted_at is not None:
            return last_attempted_at.isoformat()
        return None


class SourceRegistryUpdate(BaseModel):
    """Schema for updating source registry entries via API."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool | None = Field(default=None, description="monitoring is enabled for this source?")
    stale_after_hours: int | None = Field(
        default=None,
        description="Recheck after this many hours; null = use default. Omit to leave unchanged.",
    )


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


class DiscoverTriggerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_module: Annotated[
        str, Field(min_length=1, max_length=50, examples=["wallaby"], description="Project module identifier")
    ]
    source_identifier: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        description="Single source to mark.",
    )
    source_identifiers: list[str] | None = Field(
        default=None,
        description="List of sources to mark. Omit to all enabled sources for the project.",
    )

    @model_validator(mode="after")
    def check_mutually_exclusive(self) -> "DiscoverTriggerRequest":
        if self.source_identifier is not None and self.source_identifiers is not None:
            raise ValueError("Provide only one of source_identifier or source_identifiers")
        return self


class DiscoverTriggerResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project_module: str = Field(description="Project module that was used")
    marked_count: int = Field(description="Number of sources marked for recheck")
    source_identifiers: list[str] = Field(description="Source identifiers that were updated")


class ProjectModuleContractStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project_module: str = Field(description="Project module identifier")
    valid: bool = Field(description="Whether discovery contract validation passed")
    required_adapters: list[str] = Field(default_factory=list)
    error: str | None = Field(default=None, description="Validation/import error when invalid")
    exports: list[str] = Field(
        default_factory=list,
        description="Known module exports relevant to discovery integration",
    )


class ProjectModuleContractListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    count: int
    modules: list[ProjectModuleContractStatus]

