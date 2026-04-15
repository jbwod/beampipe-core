"""Workflow orchestration module.

Handles submission and management of workflows on HPC schedulers.
"""

from ..projects import resolve_graph_content
from .manifest import inject_manifest_config_into_graph
from .service import execute_execution, prepare_execution

__all__ = [
    "execute_execution",
    "inject_manifest_config_into_graph",
    "prepare_execution",
    "resolve_graph_content",
]
