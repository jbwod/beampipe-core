"""SLURM job templates packaged with beampipe-core."""

from pathlib import Path

TEMPLATES_DIR = Path(__file__).resolve().parent
DEFAULT_TEMPLATE_NAME = "setonix"


def template_path(name: str) -> Path:
    return TEMPLATES_DIR / f"{name}.slurm"


def load_template(name: str | None = None) -> str:
    resolved = (name or DEFAULT_TEMPLATE_NAME).strip() or DEFAULT_TEMPLATE_NAME
    path = template_path(resolved)
    return path.read_text(encoding="utf-8")


__all__ = ["DEFAULT_TEMPLATE_NAME", "load_template", "template_path"]

# #!/bin/bash --login

# #SBATCH --account=$ACCOUNT
# #SBATCH --nodes=$NUM_NODES
# #SBATCH --job-name=DALiuGE-$SESSION_ID
# #SBATCH --time=$JOB_DURATION
# #SBATCH --error=logs/err-%j.log
# #SBATCH --mem=$SBATCH_MEM

# newgrp $ACCOUNT
# umask 002

# export DLG_ROOT=$DLG_ROOT
# $MODULES
# $VENV
