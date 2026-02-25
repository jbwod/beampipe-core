from fastapi import APIRouter, HTTPException, Request, status

from ...core.projects.service import project_module_service
from ...schemas.projects import (
    ProjectModuleContractListResponse,
    ProjectModuleContractStatus,
    ProjectModuleListResponse,
)

router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("", response_model=ProjectModuleListResponse)
async def list_projects(request: Request) -> dict[str, object]:
    """List registered project module names (from beampipe.projects entry points)."""
    _ = request
    projects = project_module_service.list_project_names()
    return {"projects": projects}


# @router.get("/contracts", response_model=ProjectModuleContractListResponse)
# async def list_project_contracts(request: Request) -> dict[str, object]:
#     _ = request
#     statuses = project_module_service.list_contract_statuses()
#     return {"count": len(statuses), "modules": statuses}


@router.get(
    "/contracts/{project_module}",
    response_model=ProjectModuleContractStatus,
)
async def get_project_module_contract(
    request: Request,
    project_module: str,
) -> dict[str, object]:
    _ = request
    if not project_module_service.project_exists(project_module):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown project_module '{project_module}'.",
        )
    return project_module_service.get_contract_status(project_module)