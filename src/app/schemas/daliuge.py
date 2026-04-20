from typing import Annotated, Any, Literal, Self, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..core.schemas import TimestampSchema, UUIDSchema

# per https://daliuge.readthedocs.io/en/v6.3.0/cli/cli_translator.html
DaliugeAlgo = Literal["metis", "mysarkar"]
DeploymentBackend = Literal["rest_remote", "slurm_remote"]
SlurmFacility = Literal[
    "galaxy_mwa",
    "galaxy_askap",
    "magnus",
    "galaxy",
    "setonix",
    "shao",
    "hyades",
    "ood",
    "ood_cloud",
]



class DaliugeTranslationConfig(BaseModel):
    """DALiuGE translator (partition + TM URL)."""

    model_config = ConfigDict(extra="forbid")

    algo: Literal["metis", "mysarkar"] = Field(
        default="metis",
        description="Partition algorithm",
    )
    num_par: int = Field(default=1, ge=1, description="Partitions / nodes")
    num_islands: int = Field(default=0, ge=0, description="Data islands; dlg -s")
    tm_url: str | None = Field(
        default=None,
        max_length=255,
        description="Translator base URL",
    )


class RestRemoteDeploymentConfig(BaseModel):
    """DALiuGE REST remote deployment"""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["rest_remote"] = "rest_remote"
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
    """DALiuGE remote SLURM deployment (generated INI + SSH submit).

    At submit time Beampipe stages a generated DALiuGE ``--config_file`` INI on
    the login node and calls ``python3 -m dlg.deploy.create_dlg_job`` over SSH.
    Profile fields therefore capture both transport/auth details and the
    INI-style facility/engine controls used by DALiuGE.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["slurm_remote"] = "slurm_remote"

    # SSH transport to the login node.
    login_node: Annotated[str, Field(min_length=1, max_length=255)]
    ssh_port: int = Field(default=22, ge=1, le=65535, description="SSH port on the login node.")
    remote_user: str | None = Field(
        default=None,
        max_length=100,
        description="Remote user for SLURM_REMOTE_USER / USER",
    )

    # Environment / facility values for generated DALiuGE config_file ([FACILITY]).
    account: Annotated[str, Field(min_length=1, max_length=64)]
    home_dir: Annotated[str, Field(min_length=1, max_length=512)]
    log_dir: Annotated[str, Field(min_length=1, max_length=512)]
    exec_prefix: str = Field(
        default="srun -l",
        description="FACILITY EXEC_PREFIX used in generated jobsub command prefix.",
    )

    # Environment to reach the dlg install on the cluster.
    dlg_root: Annotated[str, Field(min_length=1, max_length=512)]
    venv: str | None = Field(
        default=None,
        description=(
            "Shell snippet to source before invoking"
            "(e.g. 'source /software/projects/<acc>/venv/bin/activate')."
        ),
    )
    modules: str | None = Field(
        default=None,
        description="Optional multi-line 'module load' snippet sourced before venv.",
    )

    # create_dlg_job CLI arguments
    facility: SlurmFacility = Field(
        default="setonix",
        description="``-f`` facility known to dlg.deploy.configs (setonix, hyades, ...).",
    )
    job_duration_minutes: Annotated[int, Field(ge=1, le=10080)] = Field(
        default=30,
        description="``-t`` minutes passed to create_dlg_job.",
    )
    num_nodes: int = Field(default=1, ge=1, le=1024, description="``-n`` number of compute nodes.")
    num_islands: int = Field(default=1, ge=0, le=64, description="``-s`` number of data islands.")
    verbose_level: int = Field(default=1, ge=0, le=5, description="``-v`` verbosity level.")
    max_threads: int = Field(default=0, ge=0, description="``-T`` drop thread pool size.")
    all_nics: bool = Field(default=False, description="``--all_nics`` flag.")
    zerorun: bool = Field(default=False, description="``--zerorun`` flag.")
    sleepncopy: bool = Field(default=False, description="``--sleepncopy`` flag.")
    check_with_session: bool = Field(default=False, description="``--check_with_session`` flag.")
    verify_ssl: bool | None = Field(
        default=None,
        description="TLS verification used by TM translation HTTP calls.",
    )

    # # Optional inline override of the SLURM template
    # slurm_template: str | None = Field(
    #     default=None,
    #     description=(
    #         "Optional inline SLURM template body; when set it is staged on the "
    #         "login node and passed to create_dlg_job via ``--slurm_template``."
    #     ),
    # )


DeploymentConfigUnion = Annotated[
    Union[RestRemoteDeploymentConfig, SlurmRemoteDeploymentConfig],
    Field(discriminator="kind"),
]


def _validate_deployment_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw = dict(payload)
    if "kind" not in raw:
        raw["kind"] = "rest_remote"
    if raw.get("kind") == "rest_remote":
        return RestRemoteDeploymentConfig.model_validate(raw).model_dump(exclude_none=True)
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
    def _default_rest_remote_ports(self) -> Self:
        dep = self.deployment
        if dep.kind == "rest_remote":
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
    patch = body.model_dump(exclude_unset=True)
    translation_patch = patch.pop("translation", None)
    deployment_patch = patch.pop("deployment", None)
    if translation_patch:
        tr_patch = (
            translation_patch
            if isinstance(translation_patch, dict)
            else translation_patch.model_dump(exclude_unset=True)
        )
        if isinstance(tr_patch, dict):
            current_tr = dict(current.get("translation") or {})
            patch["translation"] = {**current_tr, **tr_patch}
    if deployment_patch and isinstance(deployment_patch, dict):
        current_dep = dict(current.get("deployment") or {})
        merged_dep = {**current_dep, **deployment_patch}
        kind = merged_dep.get("kind", "rest_remote")
        if kind == "rest_remote":
            patch["deployment"] = RestRemoteDeploymentConfig.model_validate(merged_dep).model_dump(
                exclude_none=True
            )
        else:
            patch["deployment"] = SlurmRemoteDeploymentConfig.model_validate(merged_dep).model_dump(
                exclude_none=True
            )
    return patch
