"""Workflow orchestration module.

Handles submission and management of workflows on HPC schedulers.
"""

from .manifest import prepare_manifest_embed

__all__ = ["prepare_manifest_embed"]
