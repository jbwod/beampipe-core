"""Helpers for the SLURM backend and ledger

we need the
- the beampipe session id (BeampipeExecution_<uuid>
- the raw SLURM job id returned by sbatch,
- the absolute remote session_dir can reconstruct file paths.
therfore: "<session_id>:<slurm_job_id>|<session_dir>"
"""
from dataclasses import dataclass

_FIELD_SEP = ":"
_PATH_SEP = "|"
SCHEDULER_JOB_ID_MAX_LEN = 512


@dataclass(frozen=True)
class SlurmSchedulerJobId:
    session_id: str
    slurm_job_id: str
    session_dir: str | None

    def encode(self) -> str:
        return compose_scheduler_job_id(
            session_id=self.session_id,
            slurm_job_id=self.slurm_job_id,
            session_dir=self.session_dir,
        )


def compose_scheduler_job_id(
    *,
    session_id: str,
    slurm_job_id: str,
    session_dir: str | None,
) -> str:
    body = f"{session_id}{_FIELD_SEP}{slurm_job_id}"
    if session_dir:
        body = f"{body}{_PATH_SEP}{session_dir}"
    if len(body) > SCHEDULER_JOB_ID_MAX_LEN:
        raise ValueError(
            "scheduler_job_id exceeds max length "
            f"{SCHEDULER_JOB_ID_MAX_LEN}: len={len(body)}"
        )
    return body


def parse_scheduler_job_id(raw: str) -> SlurmSchedulerJobId:
    raw = (raw or "").strip()
    head, _, session_dir = raw.partition(_PATH_SEP)
    if _FIELD_SEP in head:
        session_id, _, slurm_part = head.partition(_FIELD_SEP)
    else:
        session_id, slurm_part = "", head
    return SlurmSchedulerJobId(
        session_id=session_id,
        slurm_job_id=slurm_part.strip(),
        session_dir=(session_dir.strip() or None),
    )


def session_debug_paths(scheduler_job_id: str) -> dict[str, str]:
    parsed = parse_scheduler_job_id(scheduler_job_id)
    out: dict[str, str] = {}
    if parsed.slurm_job_id:
        out["slurm_job_id"] = parsed.slurm_job_id
    if parsed.session_id:
        out["slurm_session_id"] = parsed.session_id
    if parsed.session_dir:
        rstripped = parsed.session_dir.rstrip("/")
        out["slurm_session_dir"] = rstripped
        out["slurm_jobsub_path"] = f"{rstripped}/jobsub.sh"
        out["slurm_pgt_glob"] = f"{rstripped}/*.pgt.graph"
        out["slurm_stderr_glob"] = f"{rstripped}/logs/err-*.log"
    return out


__all__ = [
    "SCHEDULER_JOB_ID_MAX_LEN",
    "SlurmSchedulerJobId",
    "compose_scheduler_job_id",
    "parse_scheduler_job_id",
    "session_debug_paths",
]