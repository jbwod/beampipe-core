import asyncio
import json
import logging
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import httpx
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_daliuge_deployment_profile import crud_daliuge_deployment_profile
from ...models.daliuge import DaliugeDeploymentProfile
from ...models.ledger import ExecutionPhase, ExecutionStatus
from ...schemas.daliuge import DaliugeDeploymentProfileStored
from ..archive.service import archive_metadata_service
from ..config import settings
from ..exceptions.workflow_exceptions import (
    WorkflowErrorCode,
    WorkflowFailure,
    wf_execution_not_found,
    wf_no_deployment_profile,
    wf_staging_requires_casda,
    wf_unexpected,
)
from ..ledger.service import execution_ledger_service
from ..ledger.source_readiness import (
    filter_archive_rows_by_sbids,
    parse_execution_source_spec,
    parsed_source_readiness_error,
    source_identifiers_from_specs,
)
from ..projects.service import get_graph_path, resolve_graph_content
from ..registry.service import source_registry_service
from . import rest, slurm
from .manifest import inject_manifest_config_into_graph
from .manifest_builder import build_manifest
from .staging import stage_sources_for_manifest

logger = logging.getLogger(__name__)


async def _record_execute_execution_failure(
    db: AsyncSession,
    execution_id: UUID,
    exc: Exception,
) -> None:
    """Persist FAILED before re-raising from :func:`execute_execution`.

    ``execute_execution`` runs inside Restate ``run_typed`` with the safe terminal states.
    """
    err_s = (
        exc.format_for_ledger()
        if isinstance(exc, WorkflowFailure)
        else wf_unexpected(exc).format_for_ledger()
    )
    logger.exception("event=execute_execution_error execution_id=%s error=%s", execution_id, exc)
    await execution_ledger_service.update_execution_status(
        db=db,
        execution_id=execution_id,
        status=ExecutionStatus.FAILED,
        error=err_s,
    )


async def read_existing_workflow_manifest(
    db: AsyncSession,
    execution_id: UUID,
) -> dict:
    """Return persisted ``workflow_manifest`` for the execution, or ``{}`` if absent.
    """
    from ...crud.crud_execution_record import crud_batch_execution_records
    from ...schemas.ledger import BatchExecutionRecordRead

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if not execution:
        return {}
    existing = execution.get("workflow_manifest")
    return existing if isinstance(existing, dict) else {}


async def read_execution_ledger_snapshot(
    db: AsyncSession,
    execution_id: UUID,
) -> dict[str, Any]:
    """Return a small view of the execution row for Restate/operators.

    Postgres remains the source of truth just for the journals and API correlation.
    """
    from ...crud.crud_execution_record import crud_batch_execution_records
    from ...schemas.ledger import BatchExecutionRecordRead

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if not execution:
        raise wf_execution_not_found(execution_id)

    phase = execution.get("execution_phase")
    st = execution.get("status")
    out: dict[str, Any] = {
        "execution_id": str(execution_id),
        "project_module": execution.get("project_module"),
        "status": str(st) if st is not None else None,
        "execution_phase": str(phase) if phase is not None else None,
        "scheduler_job_id": execution.get("scheduler_job_id"),
        "scheduler_name": execution.get("scheduler_name"),
        "has_manifest": bool(execution.get("workflow_manifest")),
        "last_error": execution.get("last_error"),
    }
    out.update(await enrich_execution_dim_rest_urls(db, execution))
    return out


async def begin_restate_execution_for_execution(
    db: AsyncSession,
    execution_id: UUID,
) -> dict[str, Any]:
    """Mark the execution RUNNING and align ``execution_phase`` for Restate execute.

    Matches the opening transition of :func:`execute_execution` so ledger state matches
    whether we resume at stage/manifest or at submit (manifest already persisted).
    """
    from ...crud.crud_execution_record import crud_batch_execution_records
    from ...schemas.ledger import BatchExecutionRecordRead

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if not execution:
        raise wf_execution_not_found(execution_id)

    execution_phase = _coerce_execution_phase(execution)

    if execution_phase == ExecutionPhase.SUBMIT:
        await execution_ledger_service.update_execution_status(
            db=db, execution_id=execution_id, status=ExecutionStatus.RUNNING
        )
    else:
        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=execution_id,
            status=ExecutionStatus.RUNNING,
            execution_phase=ExecutionPhase.STAGE_AND_MANIFEST,
        )

    return await read_execution_ledger_snapshot(db=db, execution_id=execution_id)


def _coerce_execution_phase(run: dict) -> ExecutionPhase | None:
    raw = run.get("execution_phase")
    if raw is None:
        return None
    if isinstance(raw, ExecutionPhase):
        return raw
    return ExecutionPhase(str(raw))


def _profile_to_dict(profile: dict) -> dict:
    translation = dict(profile.get("translation") or {})
    deployment = dict(profile.get("deployment") or {})
    backend = str(deployment.get("kind") or "rest_remote")
    return {
        "algo": translation.get("algo", "metis"),
        "num_par": translation.get("num_par", 1),
        "num_islands": translation.get("num_islands", 0),
        "tm_url": translation.get("tm_url"),
        "dim_host_for_tm": deployment.get("dim_host_for_tm"),
        "dim_port_for_tm": deployment.get("dim_port_for_tm"),
        "deploy_host": deployment.get("deploy_host"),
        "deploy_port": deployment.get("deploy_port"),
        "verify_ssl": bool(deployment.get("verify_ssl", False)),
        "deployment_backend": backend,
        "deployment_config": deployment,
    }


async def enrich_execution_dim_rest_urls(
    db: AsyncSession,
    execution: dict[str, Any],
) -> dict[str, str]:
    sid = execution.get("scheduler_job_id")
    if not sid:
        return {}
    scheduler_name = execution.get("scheduler_name")
    # need to align as scheduler name a bit redundant
    if scheduler_name not in {"daliuge", "slurm"}:
        return {}
    try:
        profile = await _resolve_deployment_profile(db, execution)
    except Exception:
        logger.debug(
            "event=execution_paths_unresolved execution_id=%s",
            execution.get("uuid"),
            exc_info=True,
        )
        return {}


async def _resolve_deployment_profile(
    db: AsyncSession, run: dict
) -> dict:
    """Resolve DALiuGE deployment profile: run's profile_id > project default > global default."""

    async def _load_by_uuid(uid) -> dict | None:
        p = await crud_daliuge_deployment_profile.get(
            db=db, uuid=uid, schema_to_select=DaliugeDeploymentProfileStored
        )
        return _profile_to_dict(p) if p else None

    profile_id = run.get("deployment_profile_id")
    if profile_id:
        got = await _load_by_uuid(profile_id)
        if got:
            return got

    project_module = run.get("project_module")
    if project_module:
        result = await db.execute(
            select(DaliugeDeploymentProfile.uuid).where(
                and_(
                    DaliugeDeploymentProfile.project_module == project_module,
                    DaliugeDeploymentProfile.is_default.is_(True),
                )
            ).limit(1)
        )
        row = result.scalar_one_or_none()
        if row:
            got = await _load_by_uuid(row)
            if got:
                return got

    result = await db.execute(
        select(DaliugeDeploymentProfile.uuid).where(
            and_(
                DaliugeDeploymentProfile.project_module.is_(None),
                DaliugeDeploymentProfile.is_default.is_(True),
            )
        ).limit(1)
    )
    row = result.scalar_one_or_none()
    if row:
        got = await _load_by_uuid(row)
        if got:
            return got

    raise wf_no_deployment_profile()


async def prepare_execution(
    db: AsyncSession,
    project_module: str,
    sources: list,
) -> dict:
    """Validate sources and return preview of what would be included in a run."""
    errors: list[str] = []
    sources_preview: list[dict] = []
    total_datasets = 0
    parsed_ok: list[tuple[Any, str, list[str] | None]] = []

    for spec in sources:
        parse_err, sid, sbids = parse_execution_source_spec(spec)
        if parse_err:
            errors.append(parse_err)
            continue
        assert sid is not None
        parsed_ok.append((spec, sid, sbids))

    if parsed_ok:
        unique_sids = list(dict.fromkeys(s for _, s, _ in parsed_ok))
        registry_map, metadata_map = await asyncio.gather(
            source_registry_service.get_registry_read_by_identifiers(
                db, project_module, unique_sids
            ),
            archive_metadata_service.list_metadata_grouped_by_sources(
                db, project_module, unique_sids
            ),
        )

        for _, sid, sbids in parsed_ok:
            rows = metadata_map.get(sid, [])
            reg = registry_map.get(sid)
            ready_err = parsed_source_readiness_error(sid, sbids, reg, rows)
            if ready_err:
                errors.append(ready_err)
                continue
            records = filter_archive_rows_by_sbids(rows, sbids)
            sbid_count = len(records)
            dataset_count = sum(
                len((r.get("metadata_json") or {}).get("datasets") or [])
                for r in records
            )
            total_datasets += dataset_count
            sources_preview.append({
                "source_identifier": sid,
                "sbid_count": sbid_count,
                "dataset_count": dataset_count,
            })

    return {
        "project_module": project_module,
        "sources": sources,
        "sources_preview": sources_preview,
        "total_datasets": total_datasets,
        "valid": len(errors) == 0,
        "errors": errors,
    }


async def stage_sources_for_execution(
    db: AsyncSession,
    execution_id: UUID,
    *,
    casda_username: str | None = None,
    do_stage: bool = True,
) -> dict[str, dict[str, str]]:
    from ...crud.crud_execution_record import crud_batch_execution_records
    from ...schemas.ledger import BatchExecutionRecordRead

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if not execution:
        raise wf_execution_not_found(execution_id)

    execution_phase = _coerce_execution_phase(execution)
    existing_manifest = execution.get("workflow_manifest")
    st = execution.get("status")
    if (
        st in (ExecutionStatus.COMPLETED, ExecutionStatus.NOT_SUBMITTED)
        and existing_manifest
    ):
        return {
            "staged_urls_by_scan_id": {},
            "eval_urls_by_sbid": {},
            "checksum_urls_by_scan_id": {},
            "eval_checksum_urls_by_sbid": {},
        }
    if execution_phase == ExecutionPhase.SUBMIT and existing_manifest:
        return {
            "staged_urls_by_scan_id": {},
            "eval_urls_by_sbid": {},
            "checksum_urls_by_scan_id": {},
            "eval_checksum_urls_by_sbid": {},
        }

    project_module = execution["project_module"]
    sources = execution.get("sources") or []

    casda_user = casda_username or settings.CASDA_USERNAME
    if do_stage and not casda_user:
        raise wf_staging_requires_casda()

    await execution_ledger_service.update_execution_status(
        db=db,
        execution_id=execution_id,
        status=ExecutionStatus.RUNNING,
        execution_phase=ExecutionPhase.STAGE_AND_MANIFEST,
    )

    if not do_stage:
        return {
            "staged_urls_by_scan_id": {},
            "eval_urls_by_sbid": {},
            "checksum_urls_by_scan_id": {},
            "eval_checksum_urls_by_sbid": {},
        }

    staged_urls, eval_urls, checksum_urls, eval_checksum_urls = (
        await stage_sources_for_manifest(
            db=db,
            project_module=project_module,
            sources=sources,
            casda_username=str(casda_user),
        )
    )
    return {
        "staged_urls_by_scan_id": staged_urls,
        "eval_urls_by_sbid": eval_urls,
        "checksum_urls_by_scan_id": checksum_urls,
        "eval_checksum_urls_by_sbid": eval_checksum_urls,
    }


async def build_manifest_for_execution(
    db: AsyncSession,
    execution_id: UUID,
    *,
    staged_urls_by_scan_id: dict[str, str] | None = None,
    eval_urls_by_sbid: dict[str, str] | None = None,
    checksum_urls_by_scan_id: dict[str, str] | None = None,
    eval_checksum_urls_by_sbid: dict[str, str] | None = None,
) -> dict:
    """Build the daliuge manifest for an execution (replay-safe)."""
    from ...crud.crud_execution_record import crud_batch_execution_records
    from ...schemas.ledger import BatchExecutionRecordRead

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if not execution:
        raise wf_execution_not_found(execution_id)

    execution_phase = _coerce_execution_phase(execution)
    existing_manifest = execution.get("workflow_manifest")
    st = execution.get("status")
    if st in (ExecutionStatus.COMPLETED, ExecutionStatus.NOT_SUBMITTED) and existing_manifest:
        return cast(dict[Any, Any], existing_manifest)
    if execution_phase == ExecutionPhase.SUBMIT and existing_manifest:
        return cast(dict[Any, Any], existing_manifest)

    project_module = execution["project_module"]
    sources = execution.get("sources") or []

    manifest = await build_manifest(
        db=db,
        project_module=project_module,
        sources=sources,
        staged_urls_by_scan_id=staged_urls_by_scan_id or {},
        eval_urls_by_sbid=eval_urls_by_sbid or {},
        checksum_urls_by_scan_id=checksum_urls_by_scan_id or {},
        eval_checksum_urls_by_sbid=eval_checksum_urls_by_sbid or {},
    )

    await execution_ledger_service.update_execution_status(
        db=db,
        execution_id=execution_id,
        workflow_manifest=manifest,
        execution_phase=ExecutionPhase.SUBMIT,
    )
    return manifest


async def translate_dim_session_for_execution(
    db: AsyncSession,
    execution_id: UUID,
) -> dict[str, Any]:
    """Resolve graph, translate LG | PG via TM, return deploy inputs or a terminal outcome.

    Restate-visible step: no DIM deploy.
    """
    from ...crud.crud_execution_record import crud_batch_execution_records
    from ...schemas.ledger import BatchExecutionRecordRead

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if not execution:
        raise wf_execution_not_found(execution_id)

    project_module = execution["project_module"]
    manifest = execution.get("workflow_manifest")
    if not manifest:
        raise WorkflowFailure(
            WorkflowErrorCode.EXECUTION_MANIFEST_STATE,
            f"Execution {execution_id} missing workflow_manifest; run staging and manifest build first",
        )

    session_id = f"BeampipeExecution_{execution_id}"
    if execution.get("scheduler_name") == "daliuge" and execution.get("scheduler_job_id") == session_id:
        return {"status": "noop", "session_id": session_id}

    graph_content: str | None = None
    graph_fetch_error: str | None = None
    try:
        graph_content = resolve_graph_content(project_module)
    except ValueError as e:
        # we still stage/manifest so the workflow can clear its pending sources.
        logger.warning(
            "event=execute_execution_no_graph project_module=%s error=%s",
            project_module,
            e,
        )
        graph_content = None
    except (httpx.HTTPError, FileNotFoundError) as e:
          # Graph fetch failures (e.g. 404 from GitHub) should not be retried by ARQ.
          # immediately keep re-creating failing runs in a tight loop.
        graph_fetch_error = str(e)
        logger.warning(
            "event=execute_execution_graph_fetch_error project_module=%s error=%s",
            project_module,
            graph_fetch_error,
            exc_info=True,
        )

    if graph_fetch_error:
        source_identifiers = source_identifiers_from_specs(execution.get("sources"))
        await source_registry_service.clear_workflow_pending_for_sources(
            db=db,
            project_module=project_module,
            source_identifiers=source_identifiers,
            commit=False,
        )
        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=execution_id,
            status=ExecutionStatus.FAILED,
            error=graph_fetch_error,
            execution_phase=ExecutionPhase.SUBMIT,
        )
        return {"status": "terminal_failed", "session_id": session_id}

    if not graph_content:
        source_identifiers = source_identifiers_from_specs(execution.get("sources"))
        await source_registry_service.clear_workflow_pending_for_sources(
            db=db,
            project_module=project_module,
            source_identifiers=source_identifiers,
            commit=False,
        )
        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=execution_id,
            status=ExecutionStatus.COMPLETED,
            execution_phase=None,
        )
        return {"status": "terminal_completed", "session_id": session_id}

    graph_json = json.loads(graph_content)
    inject_manifest_config_into_graph(graph_json, manifest)

    graph_path = get_graph_path(project_module)
    lg_name = f"{project_module}.graph"
    if graph_path:
        lg_name = Path(graph_path).name

    profile = await _resolve_deployment_profile(db, execution)
    deployment_backend = profile.get("deployment_backend")
    if deployment_backend == "slurm_remote":
        return await slurm.translate(
            db=db,
            execution=execution,
            execution_id=execution_id,
            project_module=project_module,
            session_id=session_id,
            graph_json=graph_json,
            lg_name=lg_name,
            profile=profile,
        )

    return await rest.translate(
        db=db,
        execution=execution,
        execution_id=execution_id,
        project_module=project_module,
        session_id=session_id,
        graph_json=graph_json,
        lg_name=lg_name,
        profile=profile,
    )


async def deploy_dim_session_payload_for_execution(
    db: AsyncSession,
    execution_id: UUID,
    *,
    session_id: str,
    pg_spec: list[Any],
    roots: list[Any],
    dim_base: str,
    verify_ssl: bool,
) -> None:
    """Deploy physical graph to DIM and checkpoint ``scheduler_job_id`` (replay-safe)."""
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


async def submit_dim_session_for_execution(
    db: AsyncSession,
    execution_id: UUID,
) -> str:
    """Submit the execution to DIM (rest_dim): translate + deploy in one call.

    Replay-safe: if the execution already has ``scheduler_job_id`` == session id, it no-ops.
    """
    tr = await translate_dim_session_for_execution(db=db, execution_id=execution_id)
    session_id = str(tr["session_id"])
    if tr["status"] == "ready":
        await deploy_dim_session_payload_for_execution(
            db=db,
            execution_id=execution_id,
            session_id=session_id,
            pg_spec=tr["pg_spec"],
            roots=tr["roots"],
            dim_base=str(tr["dim_base"]),
            verify_ssl=bool(tr["verify_ssl"]),
        )
    return session_id


async def _fetch_dim_graph_status(
    client: httpx.AsyncClient,
    session_id: str,
) -> dict[str, Any]:
    """graph/status`` endpoint

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


async def poll_dim_session_for_execution(
    db: AsyncSession,
    execution_id: UUID,
    *,
    poll_timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Poll DIM session and update execution status when terminal.
    """
    from ...crud.crud_execution_record import crud_batch_execution_records
    from ...schemas.ledger import BatchExecutionRecordRead

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if not execution:
        raise wf_execution_not_found(execution_id)

    st_poll = execution["status"]
    if st_poll == ExecutionStatus.COMPLETED:
        return {"terminal": True, "status": "completed"}
    if st_poll == ExecutionStatus.NOT_SUBMITTED:
        return {"terminal": True, "status": "not_submitted"}
    if st_poll == ExecutionStatus.FAILED:
        return {"terminal": True, "status": "failed", "error": execution.get("last_error")}

    session_id = execution.get("scheduler_job_id")
    project_module = execution["project_module"]
    if not session_id:
        raise WorkflowFailure(
            WorkflowErrorCode.EXECUTION_DIM_STATE,
            f"Execution {execution_id} has no scheduler_job_id; call submit_dim_session_for_execution first",
        )

    profile = await _resolve_deployment_profile(db, execution)
    deployment_backend = profile.get("deployment_backend")
    if deployment_backend != "rest_dim":
        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=execution_id,
            status=ExecutionStatus.FAILED,
            error=f"durable DIM polling not implemented for backend={deployment_backend}",
            execution_phase=ExecutionPhase.SUBMIT,
        )
        return {"terminal": True, "status": "failed", "error": f"backend={deployment_backend}"}

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

    # Terminal: update ledger and clear registry pending sources.
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

    error_drops = graph_info.get("error_drops", [])
    if session_state == "finished" and graph_info.get("has_errors"):
        error_msg = (
            f"DLG session finished with {len(error_drops)} errored drop(s): "
            f"{error_drops[:20]}"
        )
    else:
        error_msg = f"DLG session error: {status_payload}"

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


async def execute_execution(
    db: AsyncSession,
    execution_id: UUID,
    *,
    casda_username: str | None = None,
    do_stage: bool = True,
    do_submit: bool = True,
) -> dict:
    """Execute an execution.
    1. Update execution status to RUNNING (and execution checkpoint)
    2. Stage data and build manifest unless ``execution_phase`` is already ``submit``
    3. Submit to DALiuGE (optional)
    4. Update execution status to NOT_SUBMITTED (manifest-only), COMPLETED, or FAILED

    ``batch_execution_record.execution_phase`` survives ARQ retries so staging/manifest are not
    repeated after the manifest row has been persisted. See docs/execution_run_phases.md.
    """
    from ...crud.crud_execution_record import crud_batch_execution_records
    from ...schemas.ledger import BatchExecutionRecordRead

    execution = await crud_batch_execution_records.get(
        db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
    )
    if not execution:
        raise wf_execution_not_found(execution_id)

    project_module = execution["project_module"]
    sources = execution.get("sources") or []
    execution_phase = _coerce_execution_phase(execution)

    if execution_phase == ExecutionPhase.SUBMIT:
        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=execution_id,
            status=ExecutionStatus.RUNNING,
        )
    else:
        casda_user = casda_username or settings.CASDA_USERNAME
        if do_stage and not casda_user:
            raise wf_staging_requires_casda()
        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=execution_id,
            status=ExecutionStatus.RUNNING,
            execution_phase=ExecutionPhase.STAGE_AND_MANIFEST,
        )

    try:
        if execution_phase == ExecutionPhase.SUBMIT:
            manifest = execution.get("workflow_manifest")
            if not manifest:
                raise WorkflowFailure(
                    WorkflowErrorCode.EXECUTION_MANIFEST_STATE,
                    f"Execution {execution_id} has execution_phase submit but workflow_manifest is missing",
                )
        else:
            stage_out = await stage_sources_for_execution(
                db=db,
                execution_id=execution_id,
                casda_username=casda_username,
                do_stage=do_stage,
            )
            manifest = await build_manifest_for_execution(
                db=db,
                execution_id=execution_id,
                staged_urls_by_scan_id=stage_out["staged_urls_by_scan_id"],
                eval_urls_by_sbid=stage_out["eval_urls_by_sbid"],
                checksum_urls_by_scan_id=stage_out["checksum_urls_by_scan_id"],
                eval_checksum_urls_by_sbid=stage_out["eval_checksum_urls_by_sbid"],
            )

        if do_submit:
            await submit_dim_session_for_execution(db=db, execution_id=execution_id)
            execution_after = await crud_batch_execution_records.get(
                db=db, uuid=execution_id, schema_to_select=BatchExecutionRecordRead
            )
            if not execution_after:
                raise WorkflowFailure(
                    WorkflowErrorCode.EXECUTION_NOT_FOUND,
                    f"Execution {execution_id} not found after DIM submit",
                )
            manifest = execution_after.get("workflow_manifest") or manifest
            st = execution_after.get("status")
            if isinstance(st, ExecutionStatus):
                st_enum = st
            else:
                st_enum = ExecutionStatus(str(st)) if st else ExecutionStatus.RUNNING

            if st_enum == ExecutionStatus.FAILED:
                return {
                    "execution_id": str(execution_id),
                    "status": "failed",
                    "error": execution_after.get("last_error") or "failed",
                    "manifest": manifest,
                }
            if st_enum == ExecutionStatus.COMPLETED:
                return {
                    "execution_id": str(execution_id),
                    "status": "completed",
                    "manifest": manifest,
                }

            sid = execution_after.get("scheduler_job_id")
            if sid:
                out: dict[str, Any] = {
                    "execution_id": str(execution_id),
                    "status": "submitted",
                    "scheduler_job_id": str(sid),
                    "manifest": manifest,
                }
                out.update(await enrich_execution_dim_rest_urls(db, execution_after))
                return out

        source_identifiers = source_identifiers_from_specs(sources)
        await source_registry_service.clear_workflow_pending_for_sources(
            db=db,
            project_module=project_module,
            source_identifiers=source_identifiers,
            commit=False,
        )
        if not do_submit:
            await execution_ledger_service.update_execution_status(
                db=db,
                execution_id=execution_id,
                status=ExecutionStatus.NOT_SUBMITTED,
                execution_phase=None,
            )
            return {
                "execution_id": str(execution_id),
                "status": "not_submitted",
                "manifest": manifest,
            }

        await execution_ledger_service.update_execution_status(
            db=db,
            execution_id=execution_id,
            status=ExecutionStatus.COMPLETED,
            execution_phase=None,
        )
        return {
            "execution_id": str(execution_id),
            "status": "completed",
            "manifest": manifest,
        }
    except Exception as e:
        await _record_execute_execution_failure(db, execution_id, e)
        raise
