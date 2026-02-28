"""Simple loader."""
from __future__ import annotations

from app.core.projects import debug_print_modules, load_project_module


def main() -> None:
    debug_print_modules("wallaby_hires")
    module = load_project_module("wallaby_hires")
    print("module", module)
    print("PROJECT_NAME", getattr(module, "PROJECT_NAME", None))
    if hasattr(module, "ping"):
        module.ping()


if __name__ == "__main__":
    main()
