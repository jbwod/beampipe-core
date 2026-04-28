"""Async SSH wrapper for the Slurm Backend
"""
import asyncssh

import logging
import re
from dataclasses import dataclass, field
from typing import Any, ClassVar, Self

from .state import normalize_state, parse_sacct_exit_code

logger = logging.getLogger(__name__)

# squeue -h prints "<state>|<reason>" while queued / running; sacct is the
# source of truth once the job is no longer active.
# https://curc.readthedocs.io/en/latest/running-jobs/squeue-status-codes.html
_SQUEUE_FORMAT = "%T|%R"
_SACCT_FORMAT = "State,ExitCode"

SBATCH_PARSABLE_RE = re.compile(r"^\s*(\d+)(?:;.*)?\s*$", re.MULTILINE)


class SlurmClientError(RuntimeError):
    """Raised when a remote SSH command or SFTP transfer fails."""


@dataclass
class SlurmDeployClient:
    """Async SSH helper for SLURM login-node interactions."""
    host: str
    username: str | None = None
    port: int = 22
    client_keys: list[str] | None = None
    passphrase: str | None = None
    known_hosts: str | None = None
    connect_timeout: float = 30.0
    command_timeout: float = 60.0

    _conn: asyncssh.SSHClientConnection | None = field(default=None, init=False, repr=False)

    _KNOWN_HOSTS_DISABLED: ClassVar[str] = "none"

    # https://docs.python.org/3/reference/datamodel.html#async-context-managers
    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def connect(self) -> None:
        import asyncssh

        if self._conn is not None:
            return
        kwargs: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "connect_timeout": self.connect_timeout,
        }
        if self.client_keys:
            kwargs["client_keys"] = list(self.client_keys)
        if self.passphrase:
            kwargs["passphrase"] = self.passphrase
        if self.known_hosts is not None:
            kwargs["known_hosts"] = (
                None
                if self.known_hosts.strip().lower() == self._KNOWN_HOSTS_DISABLED
                else self.known_hosts
            )
        logger.info(
            "event=slurm_ssh_connect host=%s port=%s username=%s",
            self.host,
            self.port,
            self.username,
        )
        self._conn = await asyncssh.connect(**kwargs)

    async def aclose(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
            await self._conn.wait_closed()
        finally:
            self._conn = None

    async def run_command(
        self, command: str, *, check: bool = True
    ) -> tuple[str, str, int]:
        """Run commandc on the login node and return."""
        if self._conn is None:
            raise SlurmClientError("SlurmDeployClient is not connected")
        result = await self._conn.run(command, check=False, timeout=self.command_timeout)
        stdout = (
            result.stdout
            if isinstance(result.stdout, str)
            else (result.stdout or b"").decode("utf-8", "replace")
        )
        stderr = (
            result.stderr
            if isinstance(result.stderr, str)
            else (result.stderr or b"").decode("utf-8", "replace")
        )
        exit_status = int(result.exit_status or 0)
        if check and exit_status != 0:
            raise SlurmClientError(
                f"remote command failed (exit={exit_status}): {command!r}\n"
                f"stderr={stderr.strip()}"
            )
        return stdout, stderr, exit_status

    async def put_text(self, remote_path: str, contents: str) -> None:
        if self._conn is None:
            raise SlurmClientError("SlurmDeployClient is not connected; call connect() first")
        async with self._conn.start_sftp_client() as sftp:
            async with sftp.open(remote_path, "w") as remote_file:
                await remote_file.write(contents)

    async def mkdir_p(self, remote_path: str) -> None:
        await self.run_command(f"mkdir -p {shell_quote(remote_path)}")

    async def query_job_state(self, slurm_job_id: str) -> dict[str, Any]:
        stdout, _stderr, _ = await self.run_command(
            f"squeue -h -j {shell_quote(slurm_job_id)} -o {shell_quote(_SQUEUE_FORMAT)}",
            check=False,
        )
        lines = (stdout or "").strip().splitlines()
        if lines:
            state, _, _ = (lines[0] + "|").partition("|")
            return {
                "state": normalize_state(state.strip().upper()),
                "exit_code": None,
                "raw": lines[0],
                "source": "squeue",
            }
        # sacct (-P pipe-separated, -n no header) once the job has left the controller.
        # https://curc.readthedocs.io/en/latest/running-jobs/slurm-commands.html#formatting-sacct-output
        stdout, _stderr, _ = await self.run_command(
            f"sacct -j {shell_quote(slurm_job_id)} --format={_SACCT_FORMAT} -P -n",
            check=False,
        )
        for raw in (stdout or "").splitlines():
            row = raw.strip()
            if not row:
                continue
            state_str, _, exit_code_str = (row + "|").partition("|")
            normalized = normalize_state(
                state_str.split()[0].strip().upper() if state_str else ""
            )
            return {
                "state": normalized,
                "exit_code": parse_sacct_exit_code(exit_code_str),
                "raw": row,
                "source": "sacct",
            }
        return {"state": "UNKNOWN", "exit_code": None, "raw": "", "source": "none"}

    async def cancel_job(self, slurm_job_id: str) -> None:
        """Best-effort"""
        await self.run_command(f"scancel {shell_quote(slurm_job_id)}", check=False)


def shell_quote(value: str) -> str:
    if not value:
        return "''"
    return "'" + value.replace("'", "'\"'\"'") + "'"


__all__ = [
    "SBATCH_PARSABLE_RE",
    "SlurmClientError",
    "SlurmDeployClient",
    "shell_quote",
]
