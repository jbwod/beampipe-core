##### TEMP FILE FOR TESTING #####
import sys
from pathlib import Path

ORCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = ORCH_DIR.parent.parent.parent.parent
SRC = REPO_ROOT / "src"


def setup_sys_path() -> None:
    s = str(SRC)
    if s not in sys.path:
        sys.path.insert(0, s)
