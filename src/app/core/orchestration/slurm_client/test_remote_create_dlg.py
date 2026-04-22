"""
PYTHONPATH=src uv run python -m app.core.orchestration.slurm_client.test_remote_create_dlg src/app/core/orchestration/slurm_client/exampleprofile.json --pgt-json ./HelloUniverse.pgt.json --host setonix.pawsey.org.au --username jblackwood --ssh-key ~/.ssh/pawsey_ed25519_key  --known-hosts ~/.ssh/known_hosts;
"""

import argparse
import asyncio
import getpass
import json
import os
import shlex
import uuid
from pathlib import Path
from typing import Any

from app.core.orchestration.slurm import (
    # _create_dlg_job_argv,
    # _env_prelude,
    # _parse_jobsub_path,
    _render_generated_ini,
    _ssh_port_from_deployment,
)
from app.core.orchestration.slurm_client.client import SlurmDeployClient, shell_quote


def _create_dlg_job_argv(
    *,
    deployment_config: dict[str, Any],
    pgt_remote_path: str,
    config_file_remote_path: str,
    slurm_template_remote_path: str | None,
) -> list[str]:
    facility = str(deployment_config.get("facility") or "setonix")

    argv: list[str] = [
        "python3",
        "-m",
        "dlg.deploy.create_dlg_job",
        "--action",
        "submit",
        "-f",
        facility,
        "-P",
        pgt_remote_path,
        "--config_file",
        config_file_remote_path,
    ]
    if slurm_template_remote_path:
        argv.extend(["--slurm_template", slurm_template_remote_path])
    return argv

def _env_prelude(deployment_config: dict[str, Any]) -> str:
    # set -euo pipefail
    # modules load
    # set +u
    # source <path>/venv/bin/activate
    # set -u
    # source <path>/venv/bin/activete
    parts = ["set -euo pipefail"]
    modules = str(deployment_config.get("modules") or "").strip()
    if modules:
        parts.append("set +u")
        for line in modules.splitlines():
            line = line.strip()
            if line:
                parts.append(line)
        parts.append("set -u")
    venv = str(deployment_config.get("venv") or "").strip()
    if venv:
        parts.append("set +u")
        parts.append(venv)
        parts.append("set -u")
    return "\n".join(parts)
    # return None

import re
# match the output of create_dlg_job
_JOBSUB_CREATED_RE = re.compile(
    r"Created job submission script\s+(?P<path>\S+/jobsub\.sh)"
)

def _parse_jobsub_path(stdout: str, *, stderr: str = "") -> str:
    match = _JOBSUB_CREATED_RE.search(stdout or "")
    if not match:
        stderr_clean = (stderr or "").strip()
        stderr_suffix = f" stderr={stderr_clean!r}" if stderr_clean else ""
        raise SystemExit(f"create_dlg_job did not print a 'Created job submission script ...' line; stdout was: {stdout!r}{stderr_suffix}")
        # raise SlurmClientError(
        #     "create_dlg_job did not print a 'Created job submission script ...' "
        #     f"line; stdout was: {stdout!r}{stderr_suffix}"
        # )
    return match.group("path")

def _load_deployment(data: dict[str, Any]) -> dict[str, Any]:
    return dict(data.get("deployment") or data)


def _known_hosts(path: str | None) -> Any:
    if not path or path.strip().lower() == "none":
        return None
    return path


def _passphrase(args: argparse.Namespace) -> str | None:
    if args.passphrase_prompt:
        return getpass.getpass()
    return args.passphrase or os.environ.get("SSH_KEY_PASSPHRASE")


async def _run(args: argparse.Namespace) -> None:
    print("[1] load profile", args.profile_json)
    deployment_config = _load_deployment(json.loads(Path(args.profile_json).read_text()))
    dlg_root = str(deployment_config["dlg_root"]).rstrip("/")
    login_node = args.host or str(deployment_config["login_node"])
    username = args.username or deployment_config.get("remote_user") or os.environ["USER"]
    port = args.port if args.port is not None else _ssh_port_from_deployment(deployment_config)

    ex_id = args.execution_id
    staging_dir = f"{dlg_root}/staging"
    pgt_path = f"{staging_dir}/BeampipeExecution_{ex_id}.pgt.graph"
    ini_path = f"{staging_dir}/BeampipeExecution_{ex_id}.ini"
    tpl_raw = deployment_config.get("slurm_template")
    tpl_path = (
        f"{staging_dir}/BeampipeExecution_{ex_id}.slurm"
        if tpl_raw and str(tpl_raw).strip()
        else None
    )

    pgt_body = (
        Path(args.pgt_json).read_text()
        if args.pgt_json
        else json.dumps({"_smoke": True, "execution_id": str(ex_id)})
    )
    ini_body = _render_generated_ini(
        deployment_config=deployment_config,
        username=username,
        pgt_remote_path=pgt_path,
        dlg_root=dlg_root,
    )
    print("generated INI")
    print(ini_body.rstrip("\n"))
    print("end INI")

    kw: dict[str, Any] = {
        "host": login_node,
        "port": port,
        "username": username,
        "connect_timeout": args.connect_timeout,
        "command_timeout": args.command_timeout,
        "known_hosts": _known_hosts(args.known_hosts),
    }
    if args.ssh_key:
        kw["client_keys"] = list(args.ssh_key)
    pp = _passphrase(args)
    if pp:
        kw["passphrase"] = pp

    print(f"[2] SSH connect {login_node!r} user={username!r} port={port}")
    async with SlurmDeployClient(**kw) as client:
        print("[3] mkdir -p", staging_dir)
        await client.mkdir_p(staging_dir)
        print("[4] upload PGT", pgt_path)
        await client.put_text(pgt_path, pgt_body)
        print("[5] upload INI", ini_path)
        await client.put_text(ini_path, ini_body)
        if tpl_path:
            print("[6] upload slurm template", tpl_path)
            await client.put_text(tpl_path, str(tpl_raw))

        inner = (
            f"{_env_prelude(deployment_config)}\n"
            f"export DLG_ROOT={shell_quote(dlg_root)}\n"
            f"{shlex.join(_create_dlg_job_argv(deployment_config=deployment_config, pgt_remote_path=pgt_path, config_file_remote_path=ini_path, slurm_template_remote_path=tpl_path))}"
        )
        create_cmd = f"bash -lc {shell_quote(inner)}"
        step = "[7]" if tpl_path else "[6]"
        print(step, "create_dlg_job")
        print(create_cmd)
        out, err, _ = await client.run_command(create_cmd, check=True)
        jobsub = _parse_jobsub_path(out, stderr=err)
        print("[done] jobsub", jobsub)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("profile_json", type=Path)
    p.add_argument("--execution-id", type=uuid.UUID, default=uuid.uuid4())
    p.add_argument("--host")
    p.add_argument("--username")
    p.add_argument("--port", type=int)
    p.add_argument("--ssh-key", action="append", default=[])
    p.add_argument("--known-hosts", default="none")
    p.add_argument("--connect-timeout", type=float, default=30.0)
    p.add_argument("--command-timeout", type=float, default=300.0)
    p.add_argument("--passphrase")
    p.add_argument("--passphrase-prompt", action="store_true")
    p.add_argument("--pgt-json", type=Path)
    asyncio.run(_run(p.parse_args()))


if __name__ == "__main__":
    main()
