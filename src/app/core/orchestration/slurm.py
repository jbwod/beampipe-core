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

# https://github.com/ICRAR/daliuge/blob/master/daliuge-engine/dlg/deploy/slurm_client.py
_JOBSUB_CREATED_RE = re.compile(
    r"Created job submission script\s+(?P<path>\S+/jobsub\.sh)"
)

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
# https://github.com/ICRAR/daliuge/tree/master/daliuge-engine/dlg/deploy/configs
# https://daliuge.readthedocs.io/en/latest/deployment/slurm_deployment.html#configuration-ini
def _render_generated_ini(
    *,
    deployment_config: dict[str, Any],
    username: str,
    pgt_remote_path: str,
    dlg_root: str,
) -> str:
    cfg = ConfigParser(interpolation=None)
    cfg.optionxform = str

    all_nics = bool(deployment_config.get("all_nics"))
    check_with_session = bool(deployment_config.get("check_with_session"))
    zerorun = bool(deployment_config.get("zerorun"))
    sleepncopy = bool(deployment_config.get("sleepncopy"))

    cfg["DEPLOYMENT"] = {
        "remote": "False",
        "submit": "False",
    }
    cfg["ENGINE"] = {
        "NUM_NODES": str(int(deployment_config.get("num_nodes") or 1)),
        "NUM_ISLANDS": str(int(deployment_config.get("num_islands") or 1)),
        "ALL_NICS": "True" if all_nics else "",
        "CHECK_WITH_SESSION": "True" if check_with_session else "",
        "MAX_THREADS": str(int(deployment_config.get("max_threads") or 0)),
        "VERBOSE_LEVEL": str(int(deployment_config.get("verbose_level") or 1)),
        "ZERORUN": "True" if zerorun else "",
        "SLEEPNCOPY": "True" if sleepncopy else "",
    }
    cfg["GRAPH"] = {
        "PHYSICAL_GRAPH": pgt_remote_path,
    }
    cfg["FACILITY"] = {
        "USER": username,
        "ACCOUNT": str(deployment_config.get("account") or ""),
        "LOGIN_NODE": str(deployment_config.get("login_node") or ""),
        "HOME_DIR": str(deployment_config.get("home_dir") or ""),
        "DLG_ROOT": dlg_root,
        "LOG_DIR": str(deployment_config.get("log_dir") or f"{dlg_root.rstrip('/')}/log"),
        "MODULES": str(deployment_config.get("modules") or ""),
        "VENV": str(deployment_config.get("venv") or ""),
        "EXEC_PREFIX": str(deployment_config.get("exec_prefix") or "srun -l"),
    }

    out = StringIO()
    cfg.write(out)
    return out.getvalue()


def _parse_jobsub_path(stdout: str, *, stderr: str = "") -> str:
    match = _JOBSUB_CREATED_RE.search(stdout or "")
    if not match:
        stderr_clean = (stderr or "").strip()
        stderr_suffix = f" stderr={stderr_clean!r}" if stderr_clean else ""
        raise SlurmClientError(
            "create_dlg_job did not print a 'Created job submission script ...' "
            f"line; stdout was: {stdout!r}{stderr_suffix}"
        )
    return match.group("path")


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
