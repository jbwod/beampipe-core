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

from importlib.metadata import entry_points
from types import ModuleType

def _entry_points_for(group: str):
    eps = entry_points()
    if hasattr(eps, "select"):
        return eps.select(group=group)
    return eps.get(group, [])


def list_project_modules() -> list[str]:
    return [ep.name for ep in _entry_points_for("beampipe.projects")]


def load_project_module(name: str) -> ModuleType:
    for ep in _entry_points_for("beampipe.projects"):
        if ep.name == name:
            return ep.load()
    raise ValueError(
        f"Project module '{name}' not found. Available: {list_project_modules()}"
    )


def debug_print_modules(target: str | None = None) -> None:
    modules = list_project_modules()
    print(f"beampipe.projects modules: {modules}")
    if target:
        module = load_project_module(target)
        name = getattr(module, "PROJECT_NAME", None)
        print(f"Loaded module '{target}', PROJECT_NAME={name}")