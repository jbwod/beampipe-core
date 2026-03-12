"""Workflow orchestration module.

Handles submission and management of workflows on HPC schedulers.
"""

from .manifest import inject_manifest_config_into_graph

__all__ = ["inject_manifest_config_into_graph"]
