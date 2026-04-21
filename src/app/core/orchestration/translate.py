"""Shared LG to PGT translation

Both REST/SLURM translate with identical inputs and its
their downstream handover differs for DIM vs PGT-body upload for SLURM.
"""

import logging
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from ...models.ledger import ExecutionPhase, ExecutionStatus
from ..ledger.service import execution_ledger_service
from ..ledger.source_readiness import source_identifiers_from_specs
from ..registry.service import source_registry_service
from .rest_client.translator_client import (
    DaliugeTranslatorClient,
    partitioned_pgt_for_dlg_deploy,
    pgt_handle_from_partitioned_payload,
)

logger = logging.getLogger(__name__)


async def translate_lg_to_pgt_artifact(
    *,
    translator: DaliugeTranslatorClient,
    lg_name: str,
    graph_json: Any,
    profile: dict[str, Any],
) -> tuple[str, Any]:
    """Run TM POST /unroll_and_partition and return partitioned PGT JSON.

    Returns (pgt_name, pgt_json) where pgt_jso` is [pgt_filename, drop_list] as dlg.deploy.create_dlg_job -P
    find_numislands expects - only this SLURM handover needs the full partitioned body.
    """
    raw = translator.unroll_and_partition_lg(
        lg_name,
        graph_json,
        algo=profile["algo"],
        num_par=profile["num_par"],
        num_islands=profile["num_islands"],
    )
    pgt_json = partitioned_pgt_for_dlg_deploy(raw, lg_name)
    pgt_id = pgt_handle_from_partitioned_payload(pgt_json, lg_name)
    return pgt_id, pgt_json


async def fail_execution_after_translate_error(
    db: AsyncSession,
    execution: dict,
    execution_id: UUID,
    project_module: str,
    error_message: str,
    session_id: str,
) -> dict[str, Any]:
    """Mark execution failed during a TM error and clear pending sources.

    Used by every translate-step backend so that a bad LG / TM 5xx surface as a
    terminal failure rather than re-queueing forever.
    """

    # Terminal: update ledger and clear registry pending sources.
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
        error=error_message,
        execution_phase=ExecutionPhase.SUBMIT,
    )
    return {"status": "terminal_failed", "session_id": session_id}


__all__ = [
    "fail_execution_after_translate_error",
    "translate_lg_to_pgt_artifact",
]
