from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..core.schemas import TimestampSchema, UUIDSchema

# per https://daliuge.readthedocs.io/en/v6.3.0/cli/cli_translator.html
DaliugeAlgo = Literal["metis", "mysarkar"]


class DaliugeExecutionProfileBase(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=50)]
    description: str | None = Field(default=None, max_length=255)
    project_module: str | None = Field(default=None, max_length=50)
    is_default: bool = Field(default=False)

    algo: DaliugeAlgo = Field(default="metis", description="Partition algorithm (metis, mysarkar)")
    num_par: Annotated[int, Field(ge=1)] = Field(default=1, description="Number of partitions")
    num_islands: Annotated[int, Field(ge=0)] = Field(default=0, description="Number of islands")

    tm_url: Annotated[str, Field(min_length=1, max_length=255)]
    dim_host_for_tm: Annotated[str, Field(min_length=1, max_length=100)]
    dim_port_for_tm: Annotated[int, Field(ge=1, le=65535)]

    deploy_host: Annotated[str, Field(min_length=1, max_length=100)]
    deploy_port: Annotated[int, Field(ge=1, le=65535)] = 8001

    verify_ssl: bool = Field(default=False)


class DaliugeExecutionProfileCreate(DaliugeExecutionProfileBase):
    model_config = ConfigDict(extra="forbid")


class DaliugeExecutionProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=50)
    description: str | None = Field(default=None, max_length=255)
    project_module: str | None = Field(default=None, max_length=50)
    is_default: bool | None = None

    algo: DaliugeAlgo | None = Field(default=None)
    num_par: int | None = Field(default=None, ge=1)
    num_islands: int | None = Field(default=None, ge=0)

    tm_url: str | None = Field(default=None, min_length=1, max_length=255)
    dim_host_for_tm: str | None = Field(default=None, min_length=1, max_length=100)
    dim_port_for_tm: int | None = Field(default=None, ge=1, le=65535)

    deploy_host: str | None = Field(default=None, min_length=1, max_length=100)
    deploy_port: int | None = Field(default=None, ge=1, le=65535)

    verify_ssl: bool | None = None


class DaliugeExecutionProfileRead(TimestampSchema, DaliugeExecutionProfileBase, UUIDSchema):
    model_config = ConfigDict(from_attributes=True)

    project_module: str | None = None
    is_default: bool = False


class DaliugeExecutionProfileDelete(BaseModel):
    model_config = ConfigDict(extra="forbid")
