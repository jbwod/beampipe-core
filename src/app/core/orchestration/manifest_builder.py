"""Manifest builder: fetch metadata, apply staged URLs, produce project manifest format."""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..archive.service import archive_metadata_service
from ..exceptions.workflow_exceptions import WorkflowErrorCode, WorkflowFailure
from ..projects import load_project_module

logger = logging.getLogger(__name__)


def _get_sbids_for_source(spec: Any) -> list[str] | None:
    if isinstance(spec, dict):
        return spec.get("sbids")
    return getattr(spec, "sbids", None)


async def build_manifest(
    db: AsyncSession,
    project_module: str,
    sources: list,
    *,
    credentials_ini_url: str = "",
    staged_urls_by_scan_id: dict[str, str] | None = None,
    eval_urls_by_sbid: dict[str, str] | None = None,
    checksum_urls_by_scan_id: dict[str, str] | None = None,
    eval_checksum_urls_by_sbid: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    1. Loads project module and fetches metadata from archive_metadata.
    2. Calls project's build_manifest_sources (required).
    3. Returns manifest with inputs and sources.
    """
    module = load_project_module(project_module)
    metadata_by_source: dict[str, list[dict[str, Any]]] = {}

    for spec in sources:
        sid = spec.get("source_identifier") if isinstance(spec, dict) else getattr(spec, "source_identifier", None)
        if not sid:
            continue
        sbids = _get_sbids_for_source(spec)
        records = await archive_metadata_service.list_metadata_for_source(
            db=db,
            project_module=project_module,
            source_identifier=sid,
            sbids=sbids,
        )
        if records:
            metadata_by_source[sid] = records

    build_fn = getattr(module, "manifest", None)
    if not callable(build_fn):
        raise WorkflowFailure(
            WorkflowErrorCode.EXECUTION_PROJECT_MODULE_CONTRACT,
            f"Project module '{project_module}' must implement a callable manifest",
        )
    sources = build_fn(
        metadata_by_source,
        staged_urls_by_scan_id=staged_urls_by_scan_id or {},
        eval_urls_by_sbid=eval_urls_by_sbid or {},
        checksum_urls_by_scan_id=checksum_urls_by_scan_id or {},
        eval_checksum_urls_by_sbid=eval_checksum_urls_by_sbid or {},
    )

    manifest: dict[str, Any] = {
        "inputs": {},
        "sources": sources,
    }
    # if credentials_ini_url:
    #     manifest["inputs"]["credentials_ini_url"] = credentials_ini_url

    return manifest
