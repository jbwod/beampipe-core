from enum import StrEnum
from uuid import UUID


class WorkflowErrorCode(StrEnum):
    DISCOVERY_INVALID_PAYLOAD = "DISCOVERY_INVALID_PAYLOAD"
    DISCOVERY_UNKNOWN_PROJECT_MODULE = "DISCOVERY_UNKNOWN_PROJECT_MODULE"
    DISCOVERY_REQUEST_MISSING_FIELD = "DISCOVERY_REQUEST_MISSING_FIELD"
    DISCOVERY_EMPTY_SOURCE_LIST = "DISCOVERY_EMPTY_SOURCE_LIST"
    DISCOVERY_ADAPTER_NOT_REGISTERED = "DISCOVERY_ADAPTER_NOT_REGISTERED"

    EXEC_RUN_INVALID_PAYLOAD = "EXEC_RUN_INVALID_PAYLOAD"
    EXEC_RUN_INVALID_WORKFLOW_KEY = "EXEC_RUN_INVALID_WORKFLOW_KEY"
    EXEC_RUN_RUN_NOT_FOUND = "EXEC_RUN_RUN_NOT_FOUND"
    EXEC_RUN_STAGING_PRECONDITION = "EXEC_RUN_STAGING_PRECONDITION"
    EXEC_RUN_MANIFEST_STATE = "EXEC_RUN_MANIFEST_STATE"
    EXEC_RUN_PROJECT_MODULE_CONTRACT = "EXEC_RUN_PROJECT_MODULE_CONTRACT"
    EXEC_RUN_NO_EXECUTION_PROFILE = "EXEC_RUN_NO_EXECUTION_PROFILE"
    EXEC_RUN_EXECUTION_PROFILE = "EXEC_RUN_EXECUTION_PROFILE"
    EXEC_RUN_DIM_STATE = "EXEC_RUN_DIM_STATE"
    EXEC_RUN_UNEXPECTED = "EXEC_RUN_UNEXPECTED"
    # more


_MAX_TERMINAL_LEN = 900


class WorkflowFailure(Exception):
    def __init__(
        self,
        code: WorkflowErrorCode,
        detail: str,
        *,
        cause: BaseException | None = None,
    ) -> None:
        self.code = code
        self.detail = detail.strip()
        self.cause = cause
        super().__init__(self.detail)

    def format_for_terminal(self) -> str:
        text = f"[{self.code.value}] {self.detail}"
        if len(text) <= _MAX_TERMINAL_LEN:
            return text
        return text[: _MAX_TERMINAL_LEN - 3] + "..."

    def format_for_ledger(self) -> str:
        """Persist to ``last_error``; slightly more room than terminal UI."""
        return f"[{self.code.value}] {self.detail}"


def wf_run_not_found(run_id: UUID) -> WorkflowFailure:
    return WorkflowFailure(
        WorkflowErrorCode.EXEC_RUN_RUN_NOT_FOUND,
        f"Run {run_id} not found",
    )


def wf_staging_requires_casda() -> WorkflowFailure:
    return WorkflowFailure(
        WorkflowErrorCode.EXEC_RUN_STAGING_PRECONDITION,
        "CASDA_USERNAME required for staging",
    )


def wf_no_execution_profile() -> WorkflowFailure:
    return WorkflowFailure(
        WorkflowErrorCode.EXEC_RUN_NO_EXECUTION_PROFILE,
        "No DALiuGE execution profile found. Create a profile via POST /api/v1/execution-profiles "
    )


def wf_unexpected(exc: BaseException) -> WorkflowFailure:
    return WorkflowFailure(
        WorkflowErrorCode.EXEC_RUN_UNEXPECTED,
        f"{type(exc).__name__}: {exc}",
        cause=exc if exc is not None else None,
    )
