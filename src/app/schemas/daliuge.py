from typing import Annotated, Any, Literal, Self, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..core.schemas import TimestampSchema, UUIDSchema

# per https://daliuge.readthedocs.io/en/v6.3.0/cli/cli_translator.html
DaliugeAlgo = Literal["metis", "mysarkar"]

DeploymentBackend = Literal["rest_dim", "slurm_remote"]
SlurmPreset = Literal["setonix", "default"]


class RestDimDeploymentConfig(BaseModel):

    model_config = ConfigDict(extra="forbid")

    kind: Literal["rest_dim"] = "rest_dim"
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

    kind: Literal["slurm_remote"] = "slurm_remote"

    # Facility (dlg deploy INI [FACILITY])
    login_node: Annotated[str, Field(min_length=1, max_length=255)]
    dlg_root: Annotated[str, Field(min_length=1, max_length=512)]
    account: Annotated[str, Field(min_length=1, max_length=100)]
    job_duration_minutes: Annotated[int, Field(ge=1, le=10080)]
    remote_user: str | None = Field(default=None, max_length=100)
    home_dir: str | None = Field(default=None, max_length=512, description="e.g. /scratch/account")
    modules: str | None = Field(default=None, description="Multiline module load commands")
    venv: str | None = Field(default=None, description="Shell snippet to activate venv")
    exec_prefix: str | None = Field(default="srun -l", max_length=100)

    # sbatch_mem: str | None = Field(default="16G", max_length=32, description="e.g. 16G, 64G")
    # sbatch_partition: str | None = Field(default=None, max_length=100)
    # sbatch_qos: str | None = Field(default=None, max_length=100)
    # sbatch_extra_directives: list[str] | None = Field(
    #     default=None,
    #     description="Additional #SBATCH lines",
    # )
    # shell_preamble: str | None = Field(
    #     default=None,
    #     description="Shell block before engine line",
    # )

    # verbose_level: int = Field(default=1, ge=0, le=5)
    # max_threads: int = Field(default=0, ge=0)
    # zerorun: bool = False
    # sleepncopy: bool = False
    # all_nics: bool = False
    # check_with_session: bool = False
    # visualise_graph: bool = False

    # slurm_preset: SlurmPreset | None = None
    # slurm_header_template: str | None = Field(
    #     default=None,
    #     description="Full SLURM header + preamble",
    # )


DeploymentConfigUnion = Annotated[
    Union[RestDimDeploymentConfig, SlurmRemoteDeploymentConfig],
    Field(discriminator="kind"),
]


def parse_deployment_config(
    backend: DeploymentBackend, raw: dict[str, Any] | None
) -> dict[str, Any] | None:
    if raw is None:
        return None
    if "kind" not in raw:
        raw = {**raw, "kind": "rest_dim" if backend == "rest_dim" else "slurm_remote"}
    if raw.get("kind") == "rest_dim":
        return RestDimDeploymentConfig.model_validate(raw).model_dump(exclude_none=True)
    return SlurmRemoteDeploymentConfig.model_validate(raw).model_dump(exclude_none=True)


def validate_deployment_config_dict(
    backend: DeploymentBackend, raw: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Validate and normalize deployment_config for JSONB storage."""
    return parse_deployment_config(backend, raw)


DEPLOYMENT_PROFILE_STATE_KEYS: frozenset[str] = frozenset({
    "name",
    "description",
    "project_module",
    "is_default",
    "deployment_backend",
    "deployment_config",
    "algo",
    "num_par",
    "num_islands",
    "tm_url",
    "dim_host_for_tm",
    "dim_port_for_tm",
    "deploy_host",
    "deploy_port",
    "verify_ssl",
})


def merge_deployment_profile_state(
    current: dict[str, Any], patch: dict[str, Any]
) -> dict[str, Any]:
    merged = {k: current[k] for k in DEPLOYMENT_PROFILE_STATE_KEYS if k in current}
    for k, v in patch.items():
        if k in DEPLOYMENT_PROFILE_STATE_KEYS:
            merged[k] = v
    return merged


def build_deployment_profile_create_dict(merged: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": merged["name"],
        "description": merged.get("description"),
        "project_module": merged.get("project_module"),
        "is_default": merged.get("is_default", False),
        "deployment_backend": merged.get("deployment_backend", "rest_dim"),
        "deployment_config": merged.get("deployment_config"),
        "algo": merged.get("algo", "mysarkar"),
        "num_par": merged.get("num_par", 1),
        "num_islands": merged.get("num_islands", 0),
        "tm_url": merged.get("tm_url"),
        "dim_host_for_tm": merged.get("dim_host_for_tm"),
        "dim_port_for_tm": merged.get("dim_port_for_tm"),
        "deploy_host": merged.get("deploy_host"),
        "deploy_port": merged.get("deploy_port"),
        "verify_ssl": merged.get("verify_ssl", False),
    }


# validation
# def check_deployment_profile_consistency(
#     deployment_backend: DeploymentBackend,
#     deployment_config: dict[str, Any] | None,
#     tm_url: str | None,
#     dim_host_for_tm: str | None,
#     deploy_host: str | None,
# ) -> None:
#     if deployment_backend == "rest_dim":
#         if not tm_url or not str(tm_url).strip():
#             raise ValueError("tm_url is required when deployment_backend is rest_dim")
#         if not dim_host_for_tm or not str(dim_host_for_tm).strip():
#             raise ValueError("dim_host_for_tm is required when deployment_backend is rest_dim")
#         if not deploy_host or not str(deploy_host).strip():
#             raise ValueError("deploy_host is required when deployment_backend is rest_dim")
#         if deployment_config is not None:
#             dc = dict(deployment_config)
#             if "kind" not in dc:
#                 dc["kind"] = "rest_dim"
#             RestDimDeploymentConfig.model_validate(dc)
#     elif deployment_backend == "slurm_remote":
#         if deployment_config is None:
#             raise ValueError("deployment_config is required when deployment_backend is slurm_remote")
#         dc = dict(deployment_config)
#         if "kind" not in dc:
#             dc["kind"] = "slurm_remote"
#         SlurmRemoteDeploymentConfig.model_validate(dc)


class DaliugeDeploymentProfileFields(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=50)]
    description: str | None = Field(default=None, max_length=255)
    project_module: str | None = Field(default=None, max_length=50)
    is_default: bool = Field(default=False)

    deployment_backend: DeploymentBackend = Field(
        default="rest_dim",
        description="rest_dim gen_pgt+gen_pg + DIM REST or SLURM remote submission",
    )
    deployment_config: dict[str, Any] | None = Field(
        default=None,
        description="SLURM remote deployment config",
    )

    algo: DaliugeAlgo = Field(
        default="metis",
        description="Partition algorithm",
    )
    num_par: Annotated[int, Field(ge=1)] = Field(
        default=1,
        description="Partitions / nodes",
    )
    num_islands: Annotated[int, Field(ge=0)] = Field(
        default=0,
        description="Data islands; dlg -s",
    )

    tm_url: str | None = Field(
        default=None,
        max_length=255,
        description="Translator base URL",
    )
    dim_host_for_tm: str | None = Field(
        default=None,
        max_length=100,
        description="DIM host as seen by translator (gen_pg)",
    )
    dim_port_for_tm: int | None = Field(default=None, ge=1, le=65535)

    deploy_host: str | None = Field(default=None, max_length=100)
    deploy_port: int | None = Field(default=None, ge=1, le=65535)

    verify_ssl: bool = Field(default=False)


class DaliugeDeploymentProfileCreate(DaliugeDeploymentProfileFields):
    model_config = ConfigDict(extra="forbid")

# validation
    @model_validator(mode="after")
    def validate_deployment(self) -> Self:
        # check_deployment_profile_consistency(
        #     self.deployment_backend,
        #     self.deployment_config,
        #     self.tm_url,
        #     self.dim_host_for_tm,
        #     self.deploy_host,
        # )
        if self.deployment_backend == "rest_dim":
            if self.dim_port_for_tm is None:
                object.__setattr__(self, "dim_port_for_tm", 8001)
            if self.deploy_port is None:
                object.__setattr__(self, "deploy_port", 8001)
        elif self.deployment_backend == "slurm_remote":
            object.__setattr__(self, "deploy_host", None)
            object.__setattr__(self, "deploy_port", None)
        return self


class DaliugeDeploymentProfileUpdate(BaseModel):
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


class DaliugeDeploymentProfileRead(TimestampSchema, DaliugeDeploymentProfileFields, UUIDSchema):
    model_config = ConfigDict(from_attributes=True)

    project_module: str | None = None
    is_default: bool = False


class DaliugeDeploymentProfileDelete(BaseModel):
    model_config = ConfigDict(extra="forbid")
