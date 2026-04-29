import logging
from typing import Any

from astropy.table import Table, vstack
from sqlalchemy.ext.asyncio import AsyncSession

from ..archive.adapters.casda import (
    metadata_records_to_eval_staging_table,
    metadata_records_to_staging_table,
)
from ..archive.adapters.casda import (
    stage_data as casda_stage_data,
)
from ..archive.adapters.casda import (
    stage_eval_data as casda_stage_eval_data,
)
from ..archive.adapters.casda.credentials import init_casda_client
from ..archive.service import archive_metadata_service
from ..config import settings
from ..projects import load_project_module
from ..worker.tasks.discovery_batch import resolve_module_adapters
from .manifest_builder import _get_sbids_for_source  # noqa: F401 — shared helper

logger = logging.getLogger(__name__)


def _effective_stage_by_sbid(stage_by_sbid: bool | None) -> bool:
    if stage_by_sbid is not None:
        return stage_by_sbid
    return settings.CASDA_STAGE_BY_SBID


def _sort_sbids(sbids: set[str]) -> list[str]:
    def key(s: str) -> tuple[int, str]:
        if s.isdigit():
            return (0, int(s))
        return (1, s)

    return sorted(sbids, key=key)


async def stage_sources_for_manifest(
    db: AsyncSession,
    project_module: str,
    sources: list,
    casda_username: str,
    *,
    adapters: dict[str, Any] | None = None,
    service_name: str = "async_service",
    stage_by_sbid: bool | None = None,
) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[str, str], set[str]]:
# returns all staged urls, eval urls, checksum urls, eval checksum urls, and failed sbids
    module = load_project_module(project_module)
    if adapters is None:
        adapters = resolve_module_adapters(module) or {}

    # Collect tables to stage
    tables_to_stage: list[Table] = []
    all_records: list[dict[str, Any]] = []
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
        all_records.extend(records)
        table = metadata_records_to_staging_table(records)
        tables_to_stage.append(table)

    if not all_records:
        logger.debug("event=stage_sources_no_datasets project_module=%s", project_module)
        return {}, {}, {}, {}, set()

    split_visibility = _effective_stage_by_sbid(stage_by_sbid)
    casda = init_casda_client(casda_username)
    # https://docs.astropy.org/en/latest/api/astropy.table.vstack.html
    all_staged: dict[str, str] = {}
    all_checksums: dict[str, str] = {}
    all_eval: dict[str, str] = {}
    all_eval_checksums: dict[str, str] = {}
    staging_failed_sbids: set[str] = set()

    try:
        if split_visibility:
            sbid_set = {str(r["sbid"]) for r in all_records if r.get("sbid") is not None}
            for sb in _sort_sbids(sbid_set):
                sub_records = [r for r in all_records if str(r.get("sbid")) == sb]
                vis_table = metadata_records_to_staging_table(sub_records)
                if len(vis_table) == 0:
                    continue
                logger.info(
                    "event=casda_stage_visibility_by_sbid project_module=%s sbid=%s rows=%s",
                    project_module,
                    sb,
                    len(vis_table),
                )
                try:
                    data_urls, checksum_urls = casda_stage_data(
                        casda,
                        vis_table,
                        verbose=True,
                        service_name=service_name,
                    )
                except ValueError as e:
                    if "do not have access to any of the requested data files" in str(e).lower():
                        logger.warning(
                            "event=casda_stage_visibility_access_denied project_module=%s sbid=%s error=%s",
                            project_module,
                            sb,
                            e,
                        )
                        staging_failed_sbids.add(str(sb))
                        continue
                    raise
                all_staged.update(data_urls)
                all_checksums.update(checksum_urls)

            orphans = [r for r in all_records if r.get("sbid") is None]
            if orphans:
                logger.warning(
                    "event=casda_stage_visibility_missing_sbid project_module=%s orphan_records=%s",
                    project_module,
                    len(orphans),
                )
                vis_table = metadata_records_to_staging_table(orphans)
                if len(vis_table) > 0:
                    data_urls, checksum_urls = casda_stage_data(
                        casda,
                        vis_table,
                        verbose=True,
                        service_name=service_name,
                    )
                    all_staged.update(data_urls)
                    all_checksums.update(checksum_urls)
        else:
            combined_table = vstack(tables_to_stage)
            data_urls, checksum_urls = casda_stage_data(
                casda,
                combined_table,
                verbose=True,
                service_name=service_name,
            )
            for scan_id, url in data_urls.items():
                all_staged[scan_id] = url
            for scan_id, url in checksum_urls.items():
                all_checksums[scan_id] = url
    except Exception as e:
        logger.error(
            "event=stage_sources_error project_module=%s error=%s",
            project_module,
            e,
            exc_info=True,
        )
        raise

    # Stage evaluation files

    eval_table = metadata_records_to_eval_staging_table(all_records)
    if len(eval_table) > 0:
        try:
            eval_urls, eval_checksum_urls = casda_stage_eval_data(
                casda, eval_table, verbose=True, service_name=service_name
            )
            for sbid, url in eval_urls.items():
                all_eval[sbid] = url
            for sbid, url in eval_checksum_urls.items():
                all_eval_checksums[sbid] = url
        except Exception as e:
            logger.warning(
                "event=stage_sources_eval_error project_module=%s error=%s",
                project_module,
                e,
            )

    for sb in staging_failed_sbids:
        all_eval.pop(sb, None)
        all_eval_checksums.pop(sb, None)

    return all_staged, all_eval, all_checksums, all_eval_checksums, staging_failed_sbids
