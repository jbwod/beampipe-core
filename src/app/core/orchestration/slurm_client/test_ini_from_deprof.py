"""
PYTHONPATH=src uv run python -m app.core.orchestration.slurm_client.test_ini_from_deprof src/app/core/orchestration/slurm_client/exampleprofile.json
"""

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any


def _load_deployment_config(data: dict[str, Any]) -> dict[str, Any]:
    if "deployment" in data:
        return dict(data["deployment"])
    if data.get("kind") == "slurm_remote":
        return dict(data)
    raise SystemExit(
        "Silly JSON moment"
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Preview ini parser."
    )
    p.add_argument(
        "profile_json",
        type=Path,
    )
    p.add_argument(
        "--execution-id",
        type=uuid.UUID,
        default=uuid.UUID("00000000-0000-0000-0000-000000000001"),    )
    p.add_argument(
        "--username",
        default=None,
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    path: Path = args.profile_json
    if not path.is_file():
        raise SystemExit(f"not a file: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    deployment_config = _load_deployment_config(data)

    if deployment_config.get("kind") not in (None, "slurm_remote"):
        print(
            "expected slurm_remote deployment.",
            file=sys.stderr,
        )

    dlg_root = str(deployment_config.get("dlg_root") or "").strip()
    if not dlg_root:
        raise SystemExit("deployment must include non-empty dlg_root")

    username = (args.username or deployment_config.get("remote_user") or "preview-user")
    username = str(username).strip() or "preview-user"

    staging_dir = f"{dlg_root.rstrip('/')}/staging"
    pgt_remote_path = f"{staging_dir}/BeampipeExecution_{args.execution_id}.pgt.graph"

    from app.core.orchestration.slurm import _render_generated_ini

    ini = _render_generated_ini(
        deployment_config=deployment_config,
        username=username,
        pgt_remote_path=pgt_remote_path,
        dlg_root=dlg_root,
    )
    sys.stdout.write(ini)
    if not ini.endswith("\n"):
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
