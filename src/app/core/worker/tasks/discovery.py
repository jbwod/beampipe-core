import hashlib
import json
import logging
import time
from typing import Any

from arq.worker import Worker

from ...config import settings
from ...db.database import local_session
from ...registry.service import source_registry_service
from ...restate_invoke import invoke_restate_workflow
from .discovery_phases import run_discovery_persist_phase, run_discovery_tap_phase

logger = logging.getLogger(__name__)


async def discover_batch(
    ctx: Worker,
    project_module: str,
    source_identifiers: list[str],
    claim_token: str | None = None,
) -> dict[str, Any]:
    _ = ctx
    if (
        settings.WORKFLOW_ENGINE_DISCOVERY == "restate"
        and settings.RESTATE_INGRESS_BASE_URL
    ):
        if claim_token:
            discovery_id = claim_token
        else:
            material = json.dumps(
                {
                    "project_module": project_module,
                    "source_identifiers": sorted(source_identifiers),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            discovery_id = hashlib.sha256(material).hexdigest()

        return await invoke_restate_workflow(
            workflow_name=settings.RESTATE_DISCOVERY_WORKFLOW_NAME,
            workflow_id=discovery_id,
            handler_name=settings.RESTATE_DISCOVERY_WORKFLOW_HANDLER,
            payload={
                "discovery_id": discovery_id,
                "project_module": project_module,
                "source_identifiers": source_identifiers,
                "claim_token": claim_token,
            },
        )

    claim_released = False
    job_started_at = time.perf_counter()
    try:
        # - tap phase: concurrent discover+prepare, no DB (`run_discovery_tap_phase`)
        # - persist phase: apply results to Postgres + finalize marks (`run_discovery_persist_phase`)
        source_results = await run_discovery_tap_phase(
            project_module,
            source_identifiers,
            claim_token=claim_token,
        )
        async with local_session() as db:
            result, claim_released = await run_discovery_persist_phase(
                db,
                project_module=project_module,
                source_identifiers=source_identifiers,
                source_results=source_results,
                claim_token=claim_token,
                job_started_at=job_started_at,
            )
        return result
    finally:
        if claim_token and not claim_released:
            try:
                async with local_session() as cleanup_db:
                    await source_registry_service.release_discovery_claim(
                        db=cleanup_db,
                        project_module=project_module,
                        source_identifiers=source_identifiers,
                        claim_token=claim_token,
                        commit=True,
                    )
            except Exception as exc:
                logger.warning(
                    "event=discover_batch_release_claim_fallback_error "
                    "project_module=%s count=%s claim_token=%s error=%s",
                    project_module,
                    len(source_identifiers),
                    claim_token,
                    exc,
                    exc_info=True,
                )
