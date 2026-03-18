# TBD Move to a ARQ Job this is how we will init the staging job
import logging
from typing import Any

from astropy.table import Table
from astropy.table import vstack
from sqlalchemy.ext.asyncio import AsyncSession

from ..archive.adapters.casda import (
    metadata_records_to_staging_table,
    metadata_records_to_eval_staging_table,
    stage_data as casda_stage_data,
    stage_eval_data as casda_stage_eval_data,
)
from ..archive.adapters.casda.credentials import init_casda_client
from ..archive.service import archive_metadata_service
from ..projects import load_project_module
from ..worker.tasks.discovery_batch import resolve_module_adapters

logger = logging.getLogger(__name__)


def _get_sbids_for_source(spec: dict | Any) -> list[str] | None:
    """Extract sbids from a source spec (dict or RunSourceSpec)."""
    if isinstance(spec, dict):
        return spec.get("sbids")
    return getattr(spec, "sbids", None)


async def stage_sources_for_manifest(
    db: AsyncSession,
    project_module: str,
    sources: list,
    casda_username: str,
    *,
    adapters: dict[str, Any] | None = None,
    service_name: str = "async_service",
) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[str, str]]:
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
        # if len(table) == 0:
            # logger.debug(
            #     "event=stage_sources_no_metadata project_module=%s source=%s falling_back_to_discover",
            #     project_module,
            #     source_identifier,
            # )
            # discover_fn = getattr(module, "discover")
            # bundle = discover_fn(source_identifier, adapters=adapters or None)
            # if not isinstance(bundle, dict):
            #     logger.warning(
            #         "event=stage_sources_discover_invalid project_module=%s source=%s",
            #         project_module,
            #         source_identifier,
            #     )
            #     continue
            # query_results = bundle.get("query_results")
            # if query_results is None or (
            #     hasattr(query_results, "__len__") and len(query_results) == 0
            # ):
            #     logger.debug(
            #         "event=stage_sources_no_results project_module=%s source=%s",
            #         project_module,
            #         source_identifier,
            #     )
            #     continue
            # table = query_results
        tables_to_stage.append(table)

    if not tables_to_stage:
        logger.debug("event=stage_sources_no_datasets project_module=%s", project_module)
        return {}, {}, {}, {}

    # https://docs.astropy.org/en/latest/api/astropy.table.vstack.html
    combined_table = vstack(tables_to_stage)
    casda = init_casda_client(casda_username)

    # Stage visibilities (access_url from metadata)
    all_staged: dict[str, str] = {}
    all_checksums: dict[str, str] = {}
    all_eval: dict[str, str] = {}
    all_eval_checksums: dict[str, str] = {}
    try:
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

    return all_staged, all_eval, all_checksums, all_eval_checksums
