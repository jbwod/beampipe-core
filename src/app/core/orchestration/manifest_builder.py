"""Manifest builder: fetch metadata, apply staged URLs, produce project manifest format."""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..archive.service import archive_metadata_service
from ..projects import load_project_module

logger = logging.getLogger(__name__)


async def build_manifest(
    db: AsyncSession,
    project_module: str,
    source_identifiers: list[str],
    *,
    credentials_ini_url: str = "",
    staged_urls_by_scan_id: dict[str, str] | None = None,
    eval_urls_by_sbid: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    1. Loads project module and fetches metadata from archive_metadata.
    2. Calls project's build_manifest_sources (required).
    3. Returns manifest with inputs and sources.
    """
    module = load_project_module(project_module)
    metadata_by_source: dict[str, list[dict[str, Any]]] = {}

    for source_identifier in source_identifiers:
        records = await archive_metadata_service.list_metadata_for_source(
            db=db,
            project_module=project_module,
            source_identifier=source_identifier,
        )
        if records:
            metadata_by_source[source_identifier] = records

    build_fn = getattr(module, "build_manifest_sources", None)
    if not callable(build_fn):
        raise ValueError(
            f"Project module '{project_module}' must implement build_manifest_sources"
        )
    sources = build_fn(
        metadata_by_source,
        # staged_urls_by_scan_id=staged_urls_by_scan_id or {},
        # eval_urls_by_sbid=eval_urls_by_sbid or {},
    )

    manifest: dict[str, Any] = {
        "inputs": {},
        "sources": sources,
    }
    # if credentials_ini_url:
    #     manifest["inputs"]["credentials_ini_url"] = credentials_ini_url

    return manifest
