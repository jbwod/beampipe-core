import json
import logging
import os
import re
import shlex
from configparser import ConfigParser
from io import StringIO
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from ...models.ledger import ExecutionStatus
from ..config import settings
from ..exceptions.workflow_exceptions import (
    WorkflowErrorCode,
    WorkflowFailure,
    wf_execution_not_found,
)
from ..ledger.service import execution_ledger_service
from ..ledger.source_readiness import source_identifiers_from_specs
from ..registry.service import source_registry_service
from .slurm_client import (
    SBATCH_PARSABLE_RE,
    SlurmClientError,
    SlurmDeployClient,
    compose_scheduler_job_id,
    parse_scheduler_job_id,
    session_debug_paths,
    shell_quote,
)
from .translate import (
    fail_execution_after_translate_error,
    translate_lg_to_pgt_artifact,
)

logger = logging.getLogger(__name__)

__all__ = [
    "poll_session",
    "slurm_session_debug_paths",
    "submit_session_payload",
    "translate",
]


def slurm_session_debug_paths(scheduler_job_id: str) -> dict[str, str]:
    return session_debug_paths(scheduler_job_id)

def _resolve_remote_user(deployment_config: dict[str, Any]) -> str | None:
    return (
        deployment_config.get("remote_user")
        or os.environ.get("SLURM_REMOTE_USER")
        or os.environ.get("USER")
    )
def _ssh_port_from_deployment(deployment_config: dict[str, Any] | None) -> int:
    if not deployment_config:
        # Probably 22
        return 22
    raw = deployment_config.get("ssh_port")
    if raw is None:
        return 22
    try:
        p = int(raw)
    except (TypeError, ValueError):
        return 22
    if 1 <= p <= 65535:
        return p
    return 22

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
