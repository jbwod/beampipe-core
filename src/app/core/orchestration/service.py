import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def prepare_run(
    db: AsyncSession,
    project_module: str,
    sources: list,
) -> dict:

# for spec in sources:
#         sid, registered, err = await validate_source_spec(db, project_module, spec)
#         if err:
#             errors.append(err)
#             continue

#         sbids = _get_sbids_for_source(spec)
#         records = await archive_metadata_service.list_metadata_for_source(
#             db=db,
#             project_module=project_module,
#             source_identifier=sid,
#             sbids=sbids,
#         )
#         sbid_count = len(records)
#         dataset_count = sum(
#             len((r.get("metadata_json") or {}).get("datasets") or [])
#             for r in records
#         )
#         total_datasets += dataset_count
#         sources_preview.append({
#             "source_identifier": sid,
#             "sbid_count": sbid_count,
#             "dataset_count": dataset_count,
#         })
    return None

async def execute_run(
    db: AsyncSession,
    run_id: UUID,
    *,
    casda_username: str | None = None,
    do_stage: bool = True,
    do_submit: bool = True,
) -> dict:
    return None
