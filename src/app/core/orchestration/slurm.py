from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from .slurm_client import session_debug_paths

__all__ = [
    "poll_session",
    "slurm_session_debug_paths",
    "submit_session_payload",
    "translate",
]


def slurm_session_debug_paths(scheduler_job_id: str) -> dict[str, str]:
    return session_debug_paths(scheduler_job_id)


async def translate(
    *,
    db: AsyncSession,
    execution: dict,
    execution_id: UUID,
    project_module: str,
    session_id: str,
    graph_json: Any,
    lg_name: str,
    profile: dict,
) -> dict[str, Any]:
    raise NotImplementedError("slurm translate")


async def submit_session_payload(
    db: AsyncSession,
    execution_id: UUID,
    *,
    session_id: str,
    pgt_name: str,
    pgt_json: Any,
    deployment_config: dict[str, Any],
    dlg_root: str,
    login_node: str,
    username: str,
) -> None:
    raise NotImplementedError("slurm submit_session_payload")


async def poll_session(
    db: AsyncSession,
    execution_id: UUID,
    *,
    execution: dict[str, Any],
    profile: dict[str, Any],
) -> dict[str, Any]:
    raise NotImplementedError("slurm poll_session")
