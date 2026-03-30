import json
import logging
from pathlib import Path
from uuid import UUID

import httpx
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_daliuge_execution_profile import crud_daliuge_execution_profile
from ...models.daliuge import DaliugeExecutionProfile
from ...models.ledger import RunStatus
from ...schemas.daliuge import DaliugeExecutionProfileRead
from ..archive.service import archive_metadata_service
from ..config import settings
from ..ledger.service import run_ledger_service
from ..projects.service import get_graph_path, resolve_graph_content
from ..registry.service import source_registry_service
from ..utils.registry import validate_source_spec
from .manifest import inject_manifest_config_into_graph
from .manifest_builder import _get_sbids_for_source, build_manifest
from .staging import stage_sources_for_manifest

logger = logging.getLogger(__name__)


def _profile_to_dict(profile: dict) -> dict:
    return {
        "algo": profile["algo"],
        "num_par": profile["num_par"],
        "num_islands": profile["num_islands"],
        "tm_url": profile.get("tm_url"),
        "dim_host_for_tm": profile.get("dim_host_for_tm"),
        "dim_port_for_tm": profile.get("dim_port_for_tm"),
        "deploy_host": profile.get("deploy_host"),
        "deploy_port": profile.get("deploy_port"),
        "verify_ssl": profile["verify_ssl"],
        "deployment_backend": profile.get("deployment_backend"),
        "deployment_config": profile.get("deployment_config"),
    }


async def _resolve_execution_profile(
    db: AsyncSession, run: dict
) -> dict:
    """Resolve DALiuGE execution profile: run's profile_id > project default > global default."""

    async def _load_by_uuid(uid) -> dict | None:
        p = await crud_daliuge_execution_profile.get(
            db=db, uuid=uid, schema_to_select=DaliugeExecutionProfileRead
        )
        return _profile_to_dict(p) if p else None

    profile_id = run.get("execution_profile_id")
    if profile_id:
        got = await _load_by_uuid(profile_id)
        if got:
            return got

    project_module = run.get("project_module")
    if project_module:
        result = await db.execute(
            select(DaliugeExecutionProfile.uuid).where(
                and_(
                    DaliugeExecutionProfile.project_module == project_module,
                    DaliugeExecutionProfile.is_default.is_(True),
                )
            ).limit(1)
        )
        row = result.scalar_one_or_none()
        if row:
            got = await _load_by_uuid(row)
            if got:
                return got

    result = await db.execute(
        select(DaliugeExecutionProfile.uuid).where(
            and_(
                DaliugeExecutionProfile.project_module.is_(None),
                DaliugeExecutionProfile.is_default.is_(True),
            )
        ).limit(1)
    )
    row = result.scalar_one_or_none()
    if row:
        got = await _load_by_uuid(row)
        if got:
            return got

    raise ValueError(
        "No DALiuGE execution profile found. Create a profile via POST /api/v1/execution-profiles "
        "and set is_default=True for a global default, or pass execution_profile_id when creating a run."
    )


async def prepare_run(
    db: AsyncSession,
    project_module: str,
    sources: list,
) -> dict:
    """Validate sources and return preview of what would be included in a run."""
    errors: list[str] = []
    sources_preview: list[dict] = []
    total_datasets = 0

    for spec in sources:
        sid, registered, err = await validate_source_spec(db, project_module, spec)
        if err:
            errors.append(err)
            continue

        sbids = _get_sbids_for_source(spec)
        records = await archive_metadata_service.list_metadata_for_source(
            db=db,
            project_module=project_module,
            source_identifier=sid,
            sbids=sbids,
        )
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

    if not sources_preview and sources:
        errors.append("No metadata found for any source")

    return {
        "project_module": project_module,
        "sources": sources,
        "sources_preview": sources_preview,
        "total_datasets": total_datasets,
        "valid": len(errors) == 0,
        "errors": errors,
    }


async def execute_run(
    db: AsyncSession,
    run_id: UUID,
    *,
    casda_username: str | None = None,
    do_stage: bool = True,
    do_submit: bool = True,
) -> dict:
    """Execute a run.
    1. Update run status to RUNNING
    2. Stage data
    3. Build manifest
    4. Submit to DALiuGE
    5. Update run status to COMPLETED or FAILED
    """
    from ...crud.crud_run_record import crud_batch_run_records
    from ...schemas.ledger import BatchRunRecordRead
    from ..utils.daliuge import get_roots
    from .rest_client.deploy_client import DaliugeDeployClient
    from .rest_client.translator_client import DaliugeTranslatorClient

    run = await crud_batch_run_records.get(
        db=db, uuid=run_id, schema_to_select=BatchRunRecordRead
    )
    if not run:
        raise ValueError(f"Run {run_id} not found")

    project_module = run["project_module"]
    sources = run.get("sources") or []

    await run_ledger_service.update_run_status(db=db, run_id=run_id, status=RunStatus.RUNNING)

    try:
        casda_user = casda_username or settings.CASDA_USERNAME
        if do_stage and not casda_user:
            raise ValueError("CASDA_USERNAME required for staging")

        staged_urls: dict[str, str] = {}
        eval_urls: dict[str, str] = {}
        checksum_urls: dict[str, str] = {}
        eval_checksum_urls: dict[str, str] = {}

        if do_stage and casda_user:
            staged_urls, eval_urls, checksum_urls, eval_checksum_urls = (
                await stage_sources_for_manifest(
                    db=db,
                    project_module=project_module,
                    sources=sources,
                    casda_username=casda_user,
                )
            )

        manifest = await build_manifest(
            db=db,
            project_module=project_module,
            sources=sources,
            staged_urls_by_scan_id=staged_urls,
            eval_urls_by_sbid=eval_urls,
            checksum_urls_by_scan_id=checksum_urls,
            eval_checksum_urls_by_sbid=eval_checksum_urls,
        )

        await run_ledger_service.update_run_status(
            db=db,
            run_id=run_id,
            workflow_manifest=manifest,
        )

        if do_submit:
            graph_content: str | None = None
            graph_fetch_error: str | None = None
            try:
                graph_content = resolve_graph_content(project_module)
            except ValueError as e:
                # we still stage/manifest so the workflow can clear its pending sources.
                logger.warning(
                    "event=execute_run_no_graph project_module=%s error=%s",
                    project_module,
                    e,
                )
                graph_content = None
            except (httpx.HTTPError, FileNotFoundError) as e:
                # Graph fetch failures (e.g. 404 from GitHub) should not be retried by ARQ.
                # immediately keep re-creating failing runs in a tight loop.
                graph_fetch_error = str(e)
                logger.warning(
                    "event=execute_run_graph_fetch_error project_module=%s error=%s",
                    project_module,
                    graph_fetch_error,
                    exc_info=True,
                )

            if graph_fetch_error:
                source_identifiers = [
                    str(spec.get("source_identifier"))
                    for spec in sources
                    if isinstance(spec, dict) and spec.get("source_identifier")
                ]
                await source_registry_service.clear_workflow_pending_for_sources(
                    db=db,
                    project_module=project_module,
                    source_identifiers=source_identifiers,
                    commit=False,
                )
                await run_ledger_service.update_run_status(
                    db=db,
                    run_id=run_id,
                    status=RunStatus.FAILED,
                    error=graph_fetch_error,
                )
                return {
                    "run_id": str(run_id),
                    "status": "failed",
                    "error": graph_fetch_error,
                    "manifest": manifest,
                }

            if graph_content:
                graph_json = json.loads(graph_content)
                inject_manifest_config_into_graph(graph_json, manifest)

                graph_path = get_graph_path(project_module)
                lg_name = f"{project_module}.graph"
                if graph_path:
                    lg_name = Path(graph_path).name

                profile = await _resolve_execution_profile(db, run)
                deployment_backend = profile.get("deployment_backend")

                if deployment_backend == "slurm_remote":
                   logger.info("event=execute_run_slurm_remote deployment_backend=%s", deployment_backend)
                   return
                # rest_dim: TM gen_pgt + gen_pg + DIM (requires tm_url, dim_*, deploy_*)
                tm_url = profile.get("tm_url")
                if not tm_url:
                    raise ValueError("rest_dim requires tm_url on the execution profile")
                dim_host = profile.get("dim_host_for_tm")
                dim_port = profile.get("dim_port_for_tm")
                if dim_host is None or dim_port is None:
                    raise ValueError(
                        "rest_dim requires dim_host_for_tm and dim_port_for_tm for gen_pg"
                    )

                translator = DaliugeTranslatorClient(
                    base_url=tm_url,
                    verify=profile["verify_ssl"],
                )
                try:
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
                finally:
                    translator.close()

                if not isinstance(pg_spec, list) or len(pg_spec) == 0:
                    raise ValueError("Empty physical graph from translator")

                drops = pg_spec[1:] if isinstance(pg_spec[0], str) else pg_spec
                specs = [x for x in drops if isinstance(x, dict) and x.get("oid")]
                roots = list(get_roots(specs))
                session_id = f"BeampipeRun_{run_id}"

                deploy_host = profile.get("deploy_host")
                deploy_port = profile.get("deploy_port")
                if deploy_host is None or deploy_port is None:
                    raise ValueError(
                        "REST DIM deploy requires deploy_host and deploy_port on the execution profile"
                    )
                dim_base = (
                    f"http://{deploy_host}:{deploy_port}"
                    if deploy_port != 80
                    else f"http://{deploy_host}"
                )
                deploy = DaliugeDeployClient(
                    base_url=dim_base,
                    verify=profile["verify_ssl"],
                )
                try:
                    deploy.deploy_session(session_id, pg_spec, roots)
                finally:
                    deploy.close()

                await run_ledger_service.update_run_status(
                    db=db,
                    run_id=run_id,
                    status=RunStatus.COMPLETED,
                    scheduler_name="daliuge",
                    scheduler_job_id=session_id,
                )
                return {
                    "run_id": str(run_id),
                    "status": "completed",
                    "scheduler_job_id": session_id,
                    "manifest": manifest,
                }

        source_identifiers = [
            str(spec.get("source_identifier"))
            for spec in sources
            if isinstance(spec, dict) and spec.get("source_identifier")
        ]
        await source_registry_service.clear_workflow_pending_for_sources(
            db=db,
            project_module=project_module,
            source_identifiers=source_identifiers,
            commit=False,
        )
        await run_ledger_service.update_run_status(
            db=db, run_id=run_id, status=RunStatus.COMPLETED
        )
        return {
            "run_id": str(run_id),
            "status": "completed",
            "manifest": manifest,
        }
    except Exception as e:
        logger.exception("event=execute_run_error run_id=%s error=%s", run_id, e)
        await run_ledger_service.update_run_status(
            db=db,
            run_id=run_id,
            status=RunStatus.FAILED,
            error=str(e),
        )
        raise
