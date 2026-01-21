from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from ..core.schemas import TimestampSchema, UUIDSchema


class ArchiveMetadataBase(BaseModel):
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
    sbid: Annotated[
        str,
        Field(
            min_length=1,
            max_length=50,
            examples=["34166"],
            description="SBID observation identifier",
        ),
    ]


class ArchiveMetadataCreate(ArchiveMetadataBase):
    model_config = ConfigDict(extra="forbid")

    metadata_json: dict | None = Field(default=None, description="Survey-specific metadata payload")


class ArchiveMetadataRead(TimestampSchema, ArchiveMetadataBase, UUIDSchema):
    model_config = ConfigDict(from_attributes=True)

    metadata_json: dict | None = None
    updated_at: datetime | None = None
