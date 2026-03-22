from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..core.schemas import TimestampSchema, UUIDSchema

# per https://daliuge.readthedocs.io/en/v6.3.0/cli/cli_translator.html
DaliugeAlgo = Literal["metis", "mysarkar"]

DeploymentBackend = Literal["rest_dim", "slurm_remote"]


class RestDimDeploymentConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    deploy_timeout_seconds: float | None = Field(
        default=None,
        ge=1.0,
        description="",
    )


class SlurmRemoteDeploymentConfig(BaseModel):
# [ENGINE]
# NUM_NODES = 1
# NUM_ISLANDS = 1
# ALL_NICS =


# [FACILITY]
# ACCOUNT = pawsey0411
# USER =
# LOGIN_NODE = setonix.pawsey.org.au
# HOME_DIR = /scratch/${ACCOUNT}
# DLG_ROOT = ${HOME_DIR}/${USER}/dlg
# LOG_DIR = ${DLG_ROOT}/log
# MODULES = module use /group/askap/modulefiles
#     module load singularity/4.1.0-mpi
#     module load py-mpi4py/3.1.5-py3.11.6
#     module load py-numpy/1.26.4
# VENV = source /software/projects/${ACCOUNT}/venv/bin/activate
# EXEC_PREFIX = srun -l   

    model_config = ConfigDict(extra="forbid")

    login_node: Annotated[str, Field(min_length=1, max_length=255)]
    dlg_root: Annotated[str, Field(min_length=1, max_length=512)]
    account: Annotated[str, Field(min_length=1, max_length=100)]
    job_duration_minutes: Annotated[int, Field(ge=1, le=10080)]
    remote_user: str | None = Field(default=None, max_length=100)
    modules: str | None = Field(default=None, description="Multiline module load commands")
    venv: str | None = Field(default=None, description="Shell snippet to activate venv")
    exec_prefix: str | None = Field(default="srun -l", max_length=100)


def parse_deployment_config(
    backend: DeploymentBackend, raw: dict[str, Any] | None
) -> dict[str, Any] | None:
    if raw is None:
        return None
    if backend == "rest_dim":
        return RestDimDeploymentConfig.model_validate(raw).model_dump(exclude_none=True)
    return SlurmRemoteDeploymentConfig.model_validate(raw).model_dump(exclude_none=True)


class DaliugeExecutionProfileFields(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=50)]
    description: str | None = Field(default=None, max_length=255)
    project_module: str | None = Field(default=None, max_length=50)
    is_default: bool = Field(default=False)

    deployment_backend: DeploymentBackend = Field(
        default="rest_dim",
        description="rest_dim",
    )
    deployment_config: dict[str, Any] | None = Field(
        default=None,
        description="deployment_backend",
    )

    algo: DaliugeAlgo = Field(default="metis", description="Partition algorithm (metis, mysarkar)")
    num_par: Annotated[int, Field(ge=1)] = Field(default=1, description="Number of partitions")
    num_islands: Annotated[int, Field(ge=0)] = Field(default=0, description="Number of islands")

    tm_url: Annotated[str, Field(min_length=1, max_length=255)]
    dim_host_for_tm: Annotated[str, Field(min_length=1, max_length=100)]
    dim_port_for_tm: Annotated[int, Field(ge=1, le=65535)]

    deploy_host: str | None = Field(default=None, max_length=100)
    deploy_port: int | None = Field(default=None, ge=1, le=65535)

    verify_ssl: bool = Field(default=False)


class DaliugeExecutionProfileCreate(DaliugeExecutionProfileFields):
    model_config = ConfigDict(extra="forbid")

# validation
    @model_validator(mode="after")
    def validate_deployment(self) -> Self:
        if self.deployment_backend == "rest_dim":
            if not self.deploy_host or not str(self.deploy_host).strip():
                raise ValueError("deploy_host is required when deployment_backend is rest_dim")
            if self.deploy_port is None:
                object.__setattr__(self, "deploy_port", 8001)
            if self.deployment_config is not None:
                RestDimDeploymentConfig.model_validate(self.deployment_config)
        elif self.deployment_backend == "slurm_remote":
            if self.deployment_config is None:
                raise ValueError("deployment_config is required when deployment_backend is slurm_remote")
            SlurmRemoteDeploymentConfig.model_validate(self.deployment_config)
            object.__setattr__(self, "deploy_host", None)
            object.__setattr__(self, "deploy_port", None)
        return self


class DaliugeExecutionProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=50)
    description: str | None = Field(default=None, max_length=255)
    project_module: str | None = Field(default=None, max_length=50)
    is_default: bool | None = None

    deployment_backend: DeploymentBackend | None = None
    deployment_config: dict[str, Any] | None = None

    algo: DaliugeAlgo | None = Field(default=None)
    num_par: int | None = Field(default=None, ge=1)
    num_islands: int | None = Field(default=None, ge=0)

    tm_url: str | None = Field(default=None, min_length=1, max_length=255)
    dim_host_for_tm: str | None = Field(default=None, min_length=1, max_length=100)
    dim_port_for_tm: int | None = Field(default=None, ge=1, le=65535)

    deploy_host: str | None = Field(default=None, max_length=100)
    deploy_port: int | None = Field(default=None, ge=1, le=65535)

    verify_ssl: bool | None = None


class DaliugeExecutionProfileRead(TimestampSchema, DaliugeExecutionProfileFields, UUIDSchema):
    model_config = ConfigDict(from_attributes=True)

    project_module: str | None = None
    is_default: bool = False


class DaliugeExecutionProfileDelete(BaseModel):
    model_config = ConfigDict(extra="forbid")
