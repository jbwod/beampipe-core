"""
  uv run python src/app/core/orchestration/test_deployment.py
  uv run python src/app/core/orchestration/test_deployment.py --sources HIPASSJ1318-21,HIPASSJ1317-21
"""
import argparse
import shutil
import subprocess
import sys

from _paths import ORCH_DIR, REPO_ROOT, setup_sys_path

setup_sys_path()

STAGING_MANIFEST = ORCH_DIR / "test_staging_e2e_manifest.json"
DEPLOY_MANIFEST = ORCH_DIR / "output_manifest.json"


def main() -> int:
    p = argparse.ArgumentParser(description="Staging + deployment e2e test")
    p.add_argument("--sources", default=None, help="Comma-separated sources (default: HIPASSJ1318-21)")
    args = p.parse_args()

    # 1. Run staging
    staging_cmd = [
        "uv", "run", "python", str(ORCH_DIR / "test_staging_e2e.py"),
        "--stage",
    ]
    if args.sources:
        staging_cmd.extend(["--sources", args.sources])
    result = subprocess.run(staging_cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        print("Staging failed", file=sys.stderr)
        return result.returncode

    if not STAGING_MANIFEST.exists():
        print(f"Staging manifest not found: {STAGING_MANIFEST}", file=sys.stderr)
        return 1

    print("")
    print("manifest for deployment")
    shutil.copy(STAGING_MANIFEST, DEPLOY_MANIFEST)
    print(f"  {STAGING_MANIFEST} -> {DEPLOY_MANIFEST}")

    print("(test-submit-rest.py)")
    deploy_cmd = [
        "uv", "run", "python", str(ORCH_DIR / "test-submit-rest.py"),
        "--proxied", "--insecure",
    ]
    result = subprocess.run(deploy_cmd, cwd=ORCH_DIR)
    if result.returncode != 0:
        print("Deployment failed", file=sys.stderr)
        return result.returncode

    print("")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
