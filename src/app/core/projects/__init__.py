"""Project modules (plugins) for domain-specific logic.

Survey-specific implementations, try to get this like a module system

Handles WALLABY-specific workflow generation and processing.
# - WALLABY dataset validation
# - DALiuGE workflow manifest generation
# - ASKAPsoft pipeline configuration
# - Result processing and validation
# https://github.com/ICRAR/wallaby-hires/blob/main/

so thinking perhaps an entry point?
    [project.entry-points."beampipe.projects"]
    wallaby_hires = "wallaby_hires.module"
"""

from .plugins import list_project_modules, load_project_module
from .service import (
    get_graph_github_url,
    get_graph_path,
    resolve_graph_content,
)


def debug_print_modules(target: str | None = None) -> None:
    modules = list_project_modules()
    print(f"beampipe.projects modules: {modules}")
    if target:
        module = load_project_module(target)
        name = getattr(module, "PROJECT_NAME", None)
        print(f"Loaded module '{target}', PROJECT_NAME={name}")


__all__ = [
    "debug_print_modules",
    "get_graph_github_url",
    "get_graph_path",
    "list_project_modules",
    "load_project_module",
    "resolve_graph_content",
]
