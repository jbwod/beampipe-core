"""
Discover -> stage (optional) -> manifest. In-memory, no DB.

  uv run python src/app/core/orchestration/test_staging_e2e.py           # discover + manifest
  uv run python src/app/core/orchestration/test_staging_e2e.py --stage   # discover + stage + manifest
"""
import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

from _paths import ORCH_DIR, setup_sys_path

setup_sys_path()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DEFAULT_SOURCE = "HIPASSJ1318-21"
PROJECT_MODULE = "wallaby_hires"


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--sources", default=DEFAULT_SOURCE, help=f"Comma-separated sources (default: {DEFAULT_SOURCE})")
    p.add_argument("--stage", action="store_true", help="Run CASDA staging")
    return p.parse_args()


def _check_env(stage: bool) -> bool:
    try:
        from app.core.config import settings

        if stage:
            user_ok = bool(os.environ.get("CASDA_USERNAME") or getattr(settings, "CASDA_USERNAME", None))
            if not user_ok:
                logger.error("CASDA_USERNAME required for staging")
                return False
            pw = getattr(settings, "CASDA_PASSWORD", None)
            pass_ok = bool(os.environ.get("CASDA_PASSWORD") or (pw.get_secret_value() if pw else None))
            if not pass_ok:
                logger.error(
                    "CASDA_PASSWORD required for non-interactive staging. "
                    "Export CASDA_PASSWORD=your_opal_password or add to .env"
                )
                return False
        return True
    except Exception as e:
        logger.error("Config load failed: %s", e)
        return False


async def main() -> None:
    args = _parse_args()
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    do_stage = args.stage

    if not _check_env(do_stage):
        sys.exit(1)

    logger.info("Staging test: %s (stage=%s)", sources, do_stage)

    from app.core.projects import load_project_module
    from app.core.utils import group_metadata_by_sbid, metadata_payload_by_sbid

    module = load_project_module(PROJECT_MODULE)
    discover_fn = getattr(module, "discover")
    prepare_fn = getattr(module, "prepare_metadata")

    # 1. Discover and prepare metadata for each source
    all_records: list[dict] = []
    metadata_by_source: dict[str, list[dict]] = {}
    for source in sources:
        logger.info("Discover %s", source)
        bundle = discover_fn(source, adapters=None)
        query_results = bundle.get("query_results")
        if not query_results or len(query_results) == 0:
            logger.warning("  No datasets from discover for %s, skipping", source)
            continue
        logger.info("  Found %s visibility datasets", len(query_results))

        logger.info("Prepare metadata %s", source)
        metadata_list, discovery_flags = prepare_fn(
            source,
            bundle,
            data_url_by_scan_id=None,
            checksum_url_by_scan_id=None,
            include_evaluation_files=True,
            include_ra_dec_vsys=True,
            adapters=None,
        )
        if not metadata_list:
            logger.warning("  No metadata from prepare_metadata for %s, skipping", source)
            continue
        logger.info("  Prepared %s dataset records", len(metadata_list))

        grouped = group_metadata_by_sbid(metadata_list)
        payload_by_sbid = metadata_payload_by_sbid(grouped, discovery_flags)
        records = [{"metadata_json": metadata_json, "sbid": sbid} for sbid, metadata_json in payload_by_sbid.items()]
        all_records.extend(records)
        metadata_by_source[source] = records

    if not all_records:
        logger.error("No datasets from any source")
        sys.exit(1)

    if do_stage:
        logger.info("Stage")
        from app.core.archive.adapters.casda import (
            metadata_records_to_eval_staging_table,
            metadata_records_to_staging_table,
            stage_data as casda_stage_data,
            stage_eval_data as casda_stage_eval_data,
        )
        from app.core.archive.adapters.casda.credentials import init_casda_client

        vis_table = metadata_records_to_staging_table(all_records)
        if len(vis_table) == 0:
            logger.error("No visibility datasets to stage")
            sys.exit(1)
        from app.core.config import settings
        username = os.environ.get("CASDA_USERNAME") or getattr(settings, "CASDA_USERNAME", None) or ""
        if not username:
            logger.error("CASDA_USERNAME required for staging")
            sys.exit(1)
        casda = init_casda_client(username)
        staged, checksum_urls = casda_stage_data(casda, vis_table, verbose=True)
        eval_table = metadata_records_to_eval_staging_table(all_records)
        if len(eval_table) > 0:
            eval_urls, eval_checksum_urls = casda_stage_eval_data(casda, eval_table, verbose=True)
        else:
            eval_urls = {}
            eval_checksum_urls = {}
    else:
        staged = {}
        checksum_urls = {}
        eval_urls = {}
        eval_checksum_urls = {}

    logger.info("Build manifest")
    manifest_fn = getattr(module, "manifest", None) or getattr(module, "build_manifest_sources", None)
    if not callable(manifest_fn):
        logger.error("No manifest/build_manifest_sources in module")
        sys.exit(1)
    manifest_sources = manifest_fn(
        metadata_by_source,
        staged_urls_by_scan_id=staged,
        eval_urls_by_sbid=eval_urls,
        checksum_urls_by_scan_id=checksum_urls or {},
        eval_checksum_urls_by_sbid=eval_checksum_urls or {},
    )
    manifest = {"inputs": {}, "sources": manifest_sources}

    # Validate and report
    manifest_sources = manifest.get("sources") or []
    all_ok = True
    for src in manifest_sources:
        for sbid_group in src.get("sbids", []):
            if do_stage and sbid_group.get("evaluation_file") and not sbid_group.get("evaluation_file_url"):
                logger.warning("MISSING evaluation_file_url for SBID %s", sbid_group.get("sbid"))
                all_ok = False
            for ds in sbid_group.get("datasets", []):
                if do_stage and not ds.get("staged_url"):
                    logger.error("MISSING staged_url: %s", ds.get("name"))
                    all_ok = False
    if not all_ok:
        sys.exit(1)

    total = sum(len(sg.get("datasets", [])) for s in manifest_sources for sg in s.get("sbids", []))
    logger.info("Manifest: %s sources, %s datasets", len(manifest_sources), total)
    out_path = ORCH_DIR / "test_staging_e2e_manifest.json"
    with open(out_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    logger.info("Written: %s", out_path)


if __name__ == "__main__":
    asyncio.run(main())
