"""Normalize Slurm job state strings for Beampipe polling.

https://slurm.schedmd.com/job_state_codes.html
https://slurm.schedmd.com/squeue.html
https://slurm.schedmd.com/sacct.html
Beampipe does not preserve every Slurm code, so we collapse to
COMPLETED, FAILED, TIMEOUT, CANCELLED, PENDING, RUNNING
"""

SLURM_BASE_STATES: frozenset[str] = frozenset(
    {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PENDING",
        "PREEMPTED",
        "RUNNING",
        "SUSPENDED",
        "TIMEOUT",
    }
)

SLURM_FLAG_STATES: frozenset[str] = frozenset(
    {
        "COMPLETING",
        "CONFIGURING",
        "EXPEDITING",
        "LAUNCH_FAILED",
        "POWER_UP_NODE",
        "RECONFIG_FAIL",
        "REQUEUED",
        "REQUEUE_FED",
        "REQUEUE_HOLD",
        "RESIZING",
        "RESV_DEL_HOLD",
        "REVOKED",
        "SIGNALING",
        "SPECIAL_EXIT",
        "STAGE_OUT",
        "STOPPED",
        "UPDATE_DB",
    }
)

SLURM_TERMINAL_OK_STATES: frozenset[str] = frozenset({"COMPLETED"})

SLURM_TERMINAL_FAILED_STATES: frozenset[str] = frozenset(
    {
        "BOOT_FAIL",
        "DEADLINE",
        "FAILED",
        "LAUNCH_FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "RECONFIG_FAIL",
    }
)

SLURM_TERMINAL_CANCELLED_STATES: frozenset[str] = frozenset({"CANCELLED", "REVOKED"})

SLURM_TRANSIENT_STATES: frozenset[str] = frozenset({"PENDING", "RUNNING"})

_RUNNING_LIKE: frozenset[str] = frozenset(
    {
        "COMPLETING",
        "CONFIGURING",
        "EXPEDITING",
        "POWER_UP_NODE",
        "REQUEUED",
        "REQUEUE_FED",
        "RESIZING",
        "SIGNALING",
        "STAGE_OUT",
        "UPDATE_DB",
    }
)

_PENDING_LIKE: frozenset[str] = frozenset(
    {
        "RESV_DEL_HOLD",
        "REQUEUE_HOLD",
        "SPECIAL_EXIT",
        "STOPPED",
        "SUSPENDED",
    }
)

_NORM_RANK: dict[str, int] = {
    "UNKNOWN": 0,
    "PENDING": 1,
    "RUNNING": 2,
    "COMPLETED": 3,
    "CANCELLED": 4,
    "TIMEOUT": 5,
    "FAILED": 6,
}


def _normalize_one_token(u: str) -> str:
    if u.startswith("CANCELLED"):
        return "CANCELLED"

    if u in SLURM_TERMINAL_OK_STATES:
        return "COMPLETED"
    if u in SLURM_TRANSIENT_STATES:
        return u
    if u in SLURM_TERMINAL_FAILED_STATES:
        return "FAILED"
    if u == "TIMEOUT":
        return "TIMEOUT"
    if u in SLURM_TERMINAL_CANCELLED_STATES:
        return "CANCELLED"
    if u in _RUNNING_LIKE:
        return "RUNNING"
    if u in _PENDING_LIKE:
        return "PENDING"
    return "UNKNOWN"


def normalize_state(state: str) -> str:
    raw = (state or "").strip()
    if not raw:
        return "UNKNOWN"
    u = raw.upper()

    if "+" in u:
        parts = [p.strip() for p in u.split("+") if p.strip()]
        if len(parts) > 1:
            norms = [_normalize_one_token(p) for p in parts]
            return max(norms, key=lambda n: _NORM_RANK.get(n, 0))

    return _normalize_one_token(u)


def parse_sacct_exit_code(raw: str) -> int | None:
    if not raw:
        return None
    head = raw.split(":")[0].strip()
    if not head:
        return None
    try:
        return int(head)
    except ValueError:
        return None


__all__ = [
    "SLURM_BASE_STATES",
    "SLURM_FLAG_STATES",
    "SLURM_TERMINAL_CANCELLED_STATES",
    "SLURM_TERMINAL_FAILED_STATES",
    "SLURM_TERMINAL_OK_STATES",
    "SLURM_TRANSIENT_STATES",
    "normalize_state",
    "parse_sacct_exit_code",
]
