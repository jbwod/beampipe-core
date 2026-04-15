from enum import StrEnum
from uuid import UUID


class WorkflowErrorCode(StrEnum):
    DISCOVERY_INVALID_PAYLOAD = "DISCOVERY_INVALID_PAYLOAD"
    DISCOVERY_UNKNOWN_PROJECT_MODULE = "DISCOVERY_UNKNOWN_PROJECT_MODULE"
    DISCOVERY_REQUEST_MISSING_FIELD = "DISCOVERY_REQUEST_MISSING_FIELD"
    DISCOVERY_EMPTY_SOURCE_LIST = "DISCOVERY_EMPTY_SOURCE_LIST"
    DISCOVERY_ADAPTER_NOT_REGISTERED = "DISCOVERY_ADAPTER_NOT_REGISTERED"

    EXECUTION_INVALID_PAYLOAD = "EXECUTION_INVALID_PAYLOAD"
    EXECUTION_INVALID_WORKFLOW_KEY = "EXECUTION_INVALID_WORKFLOW_KEY"
    EXECUTION_NOT_FOUND = "EXECUTION_NOT_FOUND"
    EXECUTION_STAGING_PRECONDITION = "EXECUTION_STAGING_PRECONDITION"
    EXECUTION_MANIFEST_STATE = "EXECUTION_MANIFEST_STATE"
    EXECUTION_PROJECT_MODULE_CONTRACT = "EXECUTION_PROJECT_MODULE_CONTRACT"
    EXECUTION_NO_DEPLOYMENT_PROFILE = "EXECUTION_NO_DEPLOYMENT_PROFILE"
    EXECUTION_DEPLOYMENT_PROFILE = "EXECUTION_DEPLOYMENT_PROFILE"
    EXECUTION_DIM_STATE = "EXECUTION_DIM_STATE"
    EXECUTION_UNEXPECTED = "EXECUTION_UNEXPECTED"
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


def wf_execution_not_found(execution_id: UUID) -> WorkflowFailure:
    return WorkflowFailure(
        WorkflowErrorCode.EXECUTION_NOT_FOUND,
        f"Execution {execution_id} not found",
    )


def wf_staging_requires_casda() -> WorkflowFailure:
    return WorkflowFailure(
        WorkflowErrorCode.EXECUTION_STAGING_PRECONDITION,
        "CASDA_USERNAME required for staging",
    )


def wf_no_deployment_profile() -> WorkflowFailure:
    return WorkflowFailure(
        WorkflowErrorCode.EXECUTION_NO_DEPLOYMENT_PROFILE,
        "No DALiuGE deployment profile found. Create a profile via POST /api/v1/deployment-profiles "
    )


def wf_unexpected(exc: BaseException) -> WorkflowFailure:
    return WorkflowFailure(
        WorkflowErrorCode.EXECUTION_UNEXPECTED,
        f"{type(exc).__name__}: {exc}",
        cause=exc if exc is not None else None,
    )
