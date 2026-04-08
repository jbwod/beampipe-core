from typing import Annotated, Any, Literal, Self, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..core.schemas import TimestampSchema, UUIDSchema

# per https://daliuge.readthedocs.io/en/v6.3.0/cli/cli_translator.html
DaliugeAlgo = Literal["metis", "mysarkar"]
DeploymentBackend = Literal["rest_dim", "slurm_remote"]
SlurmPreset = Literal["setonix", "default"]



class DaliugeTranslationConfig(BaseModel):
    """DALiuGE translator (partition + TM URL)."""

    model_config = ConfigDict(extra="forbid")

    algo: Literal["metis", "mysarkar"] = Field(
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


class RestDimDeploymentConfig(BaseModel):
    """DALiuGE REST DIM deployment"""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["rest_dim"] = "rest_dim"
    dim_host_for_tm: str | None = Field(
        default=None,
        max_length=100,
        description="DIM host as seen by translator (gen_pg)",
    )
    dim_port_for_tm: int | None = Field(default=None, ge=1, le=65535)
    deploy_host: str | None = Field(default=None, max_length=100)
    deploy_port: int | None = Field(default=None, ge=1, le=65535)
    verify_ssl: bool = Field(default=False)


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


def _validate_deployment_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw = dict(payload)
    if "kind" not in raw:
        raw["kind"] = "rest_dim"
    if raw.get("kind") == "rest_dim":
        return RestDimDeploymentConfig.model_validate(raw).model_dump(exclude_none=True)
    return SlurmRemoteDeploymentConfig.model_validate(raw).model_dump(exclude_none=True)


def deployment_profile_stored_to_read_dict(row: dict[str, Any]) -> dict[str, Any]:
    translation = DaliugeTranslationConfig.model_validate(row.get("translation") or {}).model_dump(
        exclude_none=True
    )
    deployment_validated = _validate_deployment_payload(row.get("deployment") or {})
    return {
        "uuid": row["uuid"],
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "name": row["name"],
        "description": row.get("description"),
        "project_module": row.get("project_module"),
        "is_default": row.get("is_default", False),
        "translation": translation,
        "deployment": deployment_validated,
    }


DEPLOYMENT_PROFILE_STATE_KEYS: frozenset[str] = frozenset(
    {"name", "description", "project_module", "is_default", "translation", "deployment"}
)


def merge_deployment_profile_state(
    current: dict[str, Any], patch: dict[str, Any]
) -> dict[str, Any]:
    merged = {k: current[k] for k in DEPLOYMENT_PROFILE_STATE_KEYS if k in current}
    for k, v in patch.items():
        if k in DEPLOYMENT_PROFILE_STATE_KEYS:
            merged[k] = v
    return merged


class DaliugeDeploymentProfileDbCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, max_length=50)]
    description: str | None = Field(default=None, max_length=255)
    project_module: str | None = Field(default=None, max_length=50)
    is_default: bool = Field(default=False)

    translation: dict[str, Any]
    deployment: dict[str, Any]

    @model_validator(mode="after")
    def _validate_nested_payloads(self) -> Self:
        object.__setattr__(
            self,
            "translation",
            DaliugeTranslationConfig.model_validate(self.translation).model_dump(exclude_none=True),
        )
        object.__setattr__(self, "deployment", _validate_deployment_payload(self.deployment))
        return self


class DaliugeDeploymentProfileCreate(BaseModel):
    """API create: nested ``translation`` + ``deployment`` only."""

    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Field(min_length=1, max_length=50)]
    description: str | None = Field(default=None, max_length=255)
    project_module: str | None = Field(default=None, max_length=50)
    is_default: bool = Field(default=False)

    translation: DaliugeTranslationConfig
    deployment: DeploymentConfigUnion

    @model_validator(mode="after")
    def _default_rest_dim_ports(self) -> Self:
        dep = self.deployment
        if dep.kind == "rest_dim":
            updates: dict[str, Any] = {}
            if dep.dim_port_for_tm is None:
                updates["dim_port_for_tm"] = 8001
            if dep.deploy_port is None:
                updates["deploy_port"] = 8001
            if updates:
                new_dep = dep.model_copy(update=updates)
                return self.model_copy(update={"deployment": new_dep})
        return self

    def to_db_create(self) -> DaliugeDeploymentProfileDbCreate:
        return DaliugeDeploymentProfileDbCreate.model_validate(
            {
                "name": self.name,
                "description": self.description,
                "project_module": self.project_module,
                "is_default": self.is_default,
                "translation": self.translation.model_dump(exclude_none=True),
                "deployment": self.deployment.model_dump(exclude_none=True),
            }
        )


class DaliugeTranslationPatch(BaseModel):
    """Partial translation."""

    model_config = ConfigDict(extra="forbid")

    algo: DaliugeAlgo | None = None
    num_par: int | None = Field(default=None, ge=1)
    num_islands: int | None = Field(default=None, ge=0)
    tm_url: str | None = Field(default=None, max_length=255)


class DaliugeDeploymentProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=50)
    description: str | None = Field(default=None, max_length=255)
    project_module: str | None = Field(default=None, max_length=50)
    is_default: bool | None = None

    translation: DaliugeTranslationPatch | None = None
    deployment: dict[str, Any] | None = Field(
        default=None,
        description="Deployment config",
    )


class DaliugeDeploymentProfileStored(TimestampSchema, UUIDSchema):
    """DB row from JSON-backed deployment profiles."""

    model_config = ConfigDict(from_attributes=True)

    name: str
    description: str | None = None
    project_module: str | None = None
    is_default: bool = False
    translation: dict[str, Any]
    deployment: dict[str, Any]


class DaliugeDeploymentProfileRead(TimestampSchema, UUIDSchema):
    """API read."""

    model_config = ConfigDict(from_attributes=True)

    name: str
    description: str | None = None
    project_module: str | None = None
    is_default: bool = False
    translation: DaliugeTranslationConfig
    deployment: DeploymentConfigUnion

    @classmethod
    def from_stored_dict(cls, row: dict[str, Any]) -> "DaliugeDeploymentProfileRead":
        return cls.model_validate(deployment_profile_stored_to_read_dict(row))


class DaliugeDeploymentProfileDelete(BaseModel):
    model_config = ConfigDict(extra="forbid")


def expand_update_with_nested_optional(
    current: dict[str, Any], body: DaliugeDeploymentProfileUpdate
) -> dict[str, Any]:
    return None
    # patch = body.model_dump(exclude_unset=True)
    # translation_patch = patch.pop("translation", None)
    # deployment_patch = patch.pop("deployment", None)
    # if translation_patch:
    #     tr_patch = (
    #         translation_patch
    #         if isinstance(translation_patch, dict)
    #         else translation_patch.model_dump(exclude_unset=True)
    #     )
    #     if isinstance(tr_patch, dict):
    #         current_tr = dict(current.get("translation") or {})
    #         patch["translation"] = {**current_tr, **tr_patch}
    # if deployment_patch and isinstance(deployment_patch, dict):
    #     current_dep = dict(current.get("deployment") or {})
    #     merged_dep = {**current_dep, **deployment_patch}
    #     kind = merged_dep.get("kind", "rest_dim")
    #     if kind == "rest_dim":
    #         patch["deployment"] = RestDimDeploymentConfig.model_validate(merged_dep).model_dump(
    #             exclude_none=True
    #         )
    #     else:
    #         patch["deployment"] = SlurmRemoteDeploymentConfig.model_validate(merged_dep).model_dump(
    #             exclude_none=True
    #         )
    # return patch
