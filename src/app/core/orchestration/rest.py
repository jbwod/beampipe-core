"""REST DIM (Data Island Manager) deployment backend.
"""
import json
import logging
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from ...models.ledger import ExecutionPhase, ExecutionStatus
from ..exceptions.workflow_exceptions import (
    WorkflowErrorCode,
    WorkflowFailure,
    wf_execution_not_found,
)
from ..ledger.service import execution_ledger_service
from ..ledger.source_readiness import source_identifiers_from_specs
from ..registry.service import source_registry_service
from ..utils.daliuge import (
    classify_dim_session_status as _parse_dim_session_status,
)
from ..utils.daliuge import (
    dim_graph_status_error_uids,
    dim_rest_http_base,
)
from .translate import fail_execution_after_translate_error

logger = logging.getLogger(__name__)


def session_debug_urls(profile: dict[str, Any], session_id: str) -> dict[str, str]:
    """Return operator-facing DIM REST URLs for a session (status + graph)."""
    if profile.get("deployment_backend") != "rest_remote":
        return {}
    deploy_host = profile.get("deploy_host")
    deploy_port = profile.get("deploy_port")
    if deploy_host is None or deploy_port is None:
        return {}
    dim_base = dim_rest_http_base(str(deploy_host), int(deploy_port))
    sid = quote(str(session_id))
    base = dim_base.rstrip("/")
    return {
        "dim_session_status_url": f"{base}/api/sessions/{sid}/status",
        "dim_graph_status_url": f"{base}/api/sessions/{sid}/graph/status",
    }


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
    """rest_remote: TM then handover to DIM."""
    from ..utils.daliuge import get_roots
    from .rest_client.translator_client import DaliugeTranslatorClient

    tm_url = profile.get("tm_url")
    if not tm_url:
        raise WorkflowFailure(
            WorkflowErrorCode.EXECUTION_DEPLOYMENT_PROFILE,
            "rest_remote requires tm_url on the deployment profile",
        )
    dim_host = profile.get("dim_host_for_tm")
    dim_port = profile.get("dim_port_for_tm")
    if dim_host is None or dim_port is None:
        raise WorkflowFailure(
            WorkflowErrorCode.EXECUTION_DEPLOYMENT_PROFILE,
            "rest_remote requires dim_host_for_tm and dim_port_for_tm for gen_pg",
        )

    translator = DaliugeTranslatorClient(
        base_url=tm_url,
        verify=profile["verify_ssl"],
    )
    try:
        try:
            # from service
            pgt_id = translator.translate_lg_to_pgt(
                lg_name,
                graph_json,
                algo=profile["algo"],
                num_par=profile["num_par"],
                num_islands=profile["num_islands"],
            )
            pg_spec = translator.translate_pgt_to_pg(
                pgt_id,
                dim_host_for_tm=dim_host,
                dim_port_for_tm=dim_port,
            )
        except (httpx.RequestError, json.JSONDecodeError) as e:
            err_detail = str(e)
            if isinstance(e, httpx.HTTPStatusError) and e.response is not None:
                body = (e.response.text or "").strip()[:1200]
                if body:
                    err_detail = f"{err_detail} response_body={body}"
            logger.warning(
                "event=translate_dim_tm_error execution_id=%s project_module=%s error=%s",
                execution_id,
                project_module,
                err_detail,
                exc_info=True,
            )
            return await fail_execution_after_translate_error(
                db=db,
                execution=execution,
                execution_id=execution_id,
                project_module=project_module,
                error_message=err_detail,
                session_id=session_id,
            )
    finally:
        translator.close()

    if not isinstance(pg_spec, list) or len(pg_spec) == 0:
        return await fail_execution_after_translate_error(
            db=db,
            execution=execution,
            execution_id=execution_id,
            project_module=project_module,
            error_message="Empty physical graph from translator",
            session_id=session_id,
        )

    drops = pg_spec[1:] if isinstance(pg_spec[0], str) else pg_spec
    specs = [x for x in drops if isinstance(x, dict) and x.get("oid")]
    roots = list(get_roots(specs))

    deploy_host = profile.get("deploy_host")
    deploy_port = profile.get("deploy_port")
    if deploy_host is None or deploy_port is None:
        raise WorkflowFailure(
            WorkflowErrorCode.EXECUTION_DEPLOYMENT_PROFILE,
            "REST DIM deploy requires deploy_host and deploy_port on the deployment profile",
        )
    dim_base = dim_rest_http_base(str(deploy_host), int(deploy_port))

    return {
        "status": "ready_rest_remote",
        "session_id": session_id,
        "pg_spec": pg_spec,
        "roots": roots,
        "dim_base": dim_base,
        "verify_ssl": profile["verify_ssl"],
    }


async def deploy_session_payload(
    db: AsyncSession,
    execution_id: UUID,
    *,
    session_id: str,
    pg_spec: list[Any],
    roots: list[Any],
    dim_base: str,
    verify_ssl: bool,
) -> None:
    """Deploy physical graph to DIM and checkpoint scheduler_job_id (replay safe)"""
    from ...crud.crud_execution_record import crud_batch_execution_records
    from ...schemas.ledger import BatchExecutionRecordRead
    from .rest_client.deploy_client import DaliugeDeployClient

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if not execution:
        raise wf_execution_not_found(execution_id)
    if execution.get("scheduler_name") == "daliuge" and execution.get("scheduler_job_id") == session_id:
        return

    deploy = DaliugeDeployClient(
        base_url=dim_base,
        verify=verify_ssl,
    )
    try:
        deploy.deploy_session(session_id, pg_spec, roots)
    finally:
        deploy.close()

    await execution_ledger_service.update_execution_status(
        db=db,
        execution_id=execution_id,
        scheduler_name="daliuge",
        scheduler_job_id=session_id,
        execution_phase=ExecutionPhase.SUBMIT,
    )


async def _fetch_dim_graph_status(
    client: httpx.AsyncClient,
    session_id: str,
) -> dict[str, Any]:
    """``graph/status`` endpoint.

    Returns ``{"drop_statuses": {...}, "error_drops": [...], "has_errors": bool}``.
    """
    sid = quote(str(session_id))
    try:
        r = await client.get(f"/api/sessions/{sid}/graph/status")
        r.raise_for_status()
        drop_statuses = r.json()
    except Exception as e:
        logger.warning("event=dim_graph_status_error session_id=%s error=%s", session_id, e)
        return {"drop_statuses": {}, "error_drops": [], "has_errors": False}

    if not isinstance(drop_statuses, dict):
        return {"drop_statuses": {}, "error_drops": [], "has_errors": False}

    error_drops = dim_graph_status_error_uids(drop_statuses)
    return {
        "drop_statuses": drop_statuses,
        "error_drops": error_drops,
        "has_errors": len(error_drops) > 0,
    }


async def poll_session(
    db: AsyncSession,
    execution_id: UUID,
    *,
    execution: dict[str, Any],
    profile: dict[str, Any],
    poll_timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Poll the DIM session and update the ledger when it reaches a terminal state."""
    session_id = execution.get("scheduler_job_id")
    project_module = execution["project_module"]

    deploy_host = profile.get("deploy_host")
    deploy_port = profile.get("deploy_port")
    if deploy_host is None or deploy_port is None:
        raise WorkflowFailure(
            WorkflowErrorCode.EXECUTION_DEPLOYMENT_PROFILE,
            "REST DIM polling requires deploy_host and deploy_port on the deployment profile",
        )
    dim_base = dim_rest_http_base(str(deploy_host), int(deploy_port))

    sid = quote(str(session_id))
    async with httpx.AsyncClient(
        base_url=dim_base.rstrip("/"),
        verify=profile["verify_ssl"],
        timeout=poll_timeout_seconds,
    ) as client:
        r = await client.get(f"/api/sessions/{sid}/status")
        r.raise_for_status()
        status_payload = r.json()

        session_state = _parse_dim_session_status(status_payload)

        if session_state == "running":
            return {"terminal": False}

        graph_info: dict[str, Any] = {}
        if session_state == "finished":
            graph_info = await _fetch_dim_graph_status(client, session_id)

    source_identifiers = source_identifiers_from_specs(execution.get("sources"))
    await source_registry_service.clear_workflow_pending_for_sources(
        db=db,
        project_module=project_module,
        source_identifiers=source_identifiers,
        commit=False,
    )

    if session_state == "finished" and not graph_info.get("has_errors"):
        logger.info(
            "event=dim_session_completed execution_id=%s session_id=%s",
            execution_id,
            session_id,
        )
        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=execution_id,
            status=ExecutionStatus.COMPLETED,
            scheduler_name="daliuge",
            scheduler_job_id=str(session_id),
            execution_phase=None,
        )
        return {"terminal": True, "status": "completed"}

    if session_state == "cancelled":
        logger.info(
            "event=dim_session_cancelled execution_id=%s session_id=%s",
            execution_id,
            session_id,
        )
        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=execution_id,
            status=ExecutionStatus.CANCELLED,
            scheduler_name="daliuge",
            scheduler_job_id=str(session_id),
            execution_phase=ExecutionPhase.SUBMIT,
        )
        return {"terminal": True, "status": "cancelled"}

    error_drops = graph_info.get("error_drops", [])
    if session_state == "finished" and graph_info.get("has_errors"):
        error_msg = (
            f"DLG session finished with {len(error_drops)} errored drop(s): "
            f"{error_drops[:20]}"
        )
    else:
        error_msg = f"DLG session failed: {status_payload}"

    logger.error(
        "event=dim_session_failed execution_id=%s session_id=%s error=%s",
        execution_id,
        session_id,
        error_msg,
    )
    await execution_ledger_service.update_execution_status(
        db=db,
        execution_id=execution_id,
        status=ExecutionStatus.FAILED,
        error=error_msg,
        scheduler_name="daliuge",
        scheduler_job_id=str(session_id),
        execution_phase=ExecutionPhase.SUBMIT,
    )
    return {
        "terminal": True,
        "status": "failed",
        "error": error_msg,
        "error_drops": error_drops[:50],
    }


__all__ = [
    "deploy_session_payload",
    "poll_session",
    "session_debug_urls",
    "translate",
]
