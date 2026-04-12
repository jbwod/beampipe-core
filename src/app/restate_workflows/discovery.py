"""Discovery Restate workflow: tap then persist in Postgres."""
import logging
import time
from typing import Any, cast

import restate
from pydantic import BaseModel, ConfigDict, ValidationError

from ..core.db.database import local_session
from ..core.exceptions.workflow_exceptions import WorkflowErrorCode, WorkflowFailure
from ..core.log_context import bind_execution_log_context
from ..core.projects import resolve_workflow_discovery_step_overrides
from ..core.worker.tasks.discovery_phases import (
    parse_discovery_batch_request,
    run_discovery_persist_phase,
    run_discovery_tap_phase,
)
from .options import _run_opts_database, _run_opts_external_io
from .runtime import _ingress_terminal, _run_step

logger = logging.getLogger(__name__)

DiscoveryBatchWorkflow = restate.Workflow("DiscoveryBatchWorkflow")


class DiscoveryBatchWorkflowInput(BaseModel):
    model_config = ConfigDict(extra="ignore")
    discovery_id: str | None = None
    project_module: str
    source_identifiers: list[str]
    claim_token: str | None = None
    arq_job_id: str | None = None
    arq_job_try: int | None = None


async def _discovery_tap_batch(req: dict[str, Any]) -> dict[str, Any]:
    try:
        project_module, source_identifiers, claim_token = parse_discovery_batch_request(req)
    except KeyError as e:
        raise WorkflowFailure(
            WorkflowErrorCode.DISCOVERY_REQUEST_MISSING_FIELD,
            f"discovery tap: missing field in request: {e}",
            cause=e,
        ) from e
    wall_started_at = time.time()
    tap_results = await run_discovery_tap_phase(
        project_module,
        source_identifiers,
        claim_token=claim_token,
    )
    return {"tap_results": tap_results, "wall_started_at": wall_started_at}


def _discovery_require_keys(req: dict[str, Any], *keys: str) -> None:
    for k in keys:
        if k not in req:
            raise WorkflowFailure(
                WorkflowErrorCode.DISCOVERY_REQUEST_MISSING_FIELD,
                f"discovery durable step request missing required field {k!r}",
            )


async def _discovery_persist_batch(req: dict[str, Any]) -> dict[str, Any]:
    _discovery_require_keys(
        req, "project_module", "source_identifiers", "tap_results", "wall_started_at"
    )
    project_module = str(req["project_module"])
    source_identifiers = list(req["source_identifiers"])
    tap_results = list(req["tap_results"])
    claim_token_raw = req.get("claim_token")
    claim_token = None if claim_token_raw is None else str(claim_token_raw)
    wall_started_at = float(req["wall_started_at"])
    async with local_session() as db:
        result, _released = await run_discovery_persist_phase(
            db,
            project_module=project_module,
            source_identifiers=source_identifiers,
            source_results=tap_results,
            claim_token=claim_token,
            job_started_at=0.0,
            wall_started_at=wall_started_at,
        )
        return result


def _sources_preview(source_identifiers: list[str], *, limit: int = 10) -> dict[str, Any]:
    sids = [str(s) for s in source_identifiers]
    head = sids[:limit]
    tail = sids[-limit:] if len(sids) > limit else []
    return {
        "count": len(sids),
        "head": head,
        "tail": tail,
        "truncated": len(sids) > limit,
    }


def _summarize_tap_results(tap_results: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for r in tap_results:
        outcome = str(r.get("outcome") or "unknown")
        counts[outcome] = counts.get(outcome, 0) + 1
    return {"outcomes": counts, "total": len(tap_results)}


@DiscoveryBatchWorkflow.main()
async def discovery_batch_workflow(
    ctx: restate.WorkflowContext,
    req: dict[str, Any],
) -> dict[str, Any]:
    discovery_id = ctx.key()
    if not isinstance(req, dict):
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.DISCOVERY_INVALID_PAYLOAD,
                "DiscoveryBatchWorkflow payload must be a JSON object",
            )
        )
    try:
        body = DiscoveryBatchWorkflowInput.model_validate(req)
    except ValidationError as e:
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.DISCOVERY_INVALID_PAYLOAD,
                f"Invalid discovery workflow payload: {e}",
                cause=e,
            )
        )
    if not body.source_identifiers:
        _ingress_terminal(
            WorkflowFailure(
                WorkflowErrorCode.DISCOVERY_EMPTY_SOURCE_LIST,
                "source_identifiers must be non-empty",
            )
        )

    with bind_execution_log_context(
        execution_id=str(discovery_id),
        arq_job_id=body.arq_job_id,
        job_try=body.arq_job_try,
    ):
        run_policy_overrides = resolve_workflow_discovery_step_overrides(body.project_module)
        await _run_step(
            ctx,
            "discovery.meta",
            _run_opts_database(run_policy_overrides),
            lambda discovery_id, project_module, sources, claim_token: {
                "discovery_id": discovery_id,
                "project_module": project_module,
                "sources": _sources_preview(sources),
                "claim_token": claim_token,
            },
            discovery_id=discovery_id,
            project_module=body.project_module,
            sources=body.source_identifiers,
            claim_token=body.claim_token,
        )

        project_module = body.project_module
        source_identifiers = body.source_identifiers
        claim_token = body.claim_token

        tap_payload_base = {
            "project_module": project_module,
            "claim_token": claim_token,
        }
        tap_out = await _run_step(
            ctx,
            "discovery.tap",
            _run_opts_external_io(run_policy_overrides),
            _discovery_tap_batch,
            req={**tap_payload_base, "source_identifiers": source_identifiers},
        )
        await _run_step(
            ctx,
            "discovery.tap_summary",
            _run_opts_database(run_policy_overrides),
            lambda tap_results: _summarize_tap_results(tap_results),
            tap_results=tap_out["tap_results"],
        )
        persist_out = await _run_step(
            ctx,
            "discovery.persist",
            _run_opts_database(run_policy_overrides),
            _discovery_persist_batch,
            req={
                "project_module": project_module,
                "source_identifiers": source_identifiers,
                "claim_token": claim_token,
                "tap_results": tap_out["tap_results"],
                "wall_started_at": tap_out["wall_started_at"],
            },
        )
        return cast(dict[str, Any], persist_out)
