from pydantic import BaseModel, ConfigDict, Field


class ProjectModuleListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    projects: list[str] = Field(description="Registered project module names")


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
    enrichment_keys: list[str] = Field(
        default_factory=list,
        description="Discover bundle enrichment keys this module uses (e.g. ra_dec_vsys, sbid_to_eval_file)",
    )
    graph_path: str | None = Field(default=None, description="Local path to .graph file if set")
    graph_github_url: str | None = Field(default=None, description="GitHub raw URL to graph if set")


class ProjectModuleContractListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    count: int
    modules: list[ProjectModuleContractStatus]
