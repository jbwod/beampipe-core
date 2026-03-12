"""
DIM REST deploy and session polling.

DIM: create session, append PG, deploy (dlg/clients.py create_session L59, append_graph L81, deploy_session L68)
REST: POST {dim_base}/api/sessions
  Body: JSON {"sessionId": "<id>"}
REST: POST {dim_base}/api/sessions/{sessionId}/graph/append
  Body: JSON PG spec (list of DROP specs)
REST: POST {dim_base}/api/sessions/{sessionId}/deploy
  Body: application/x-www-form-urlencoded  completed=<comma-separated root OIDs>

Poll: GET {dim_base}/api/sessions/{sessionId}/status  = JSON session status (e.g. 4 = FINISHED, 3 = ERROR)
      GET {dim_base}/api/sessions/{sessionId}/graph/status  = JSON dict { drop_uid: status, ... }
"""
import json
import sys
import time
from urllib.parse import quote

import httpx


def deploy_session(
    dim_base: str,
    session_id: str,
    pg_spec: list,
    roots: list[str],
    *,
    verify: bool = True,
    timeout_create: float = 30.0,
    timeout_append: float = 60.0,
    timeout_deploy: float = 30.0,
) -> None:
    """Create session, append graph, deploy. Raises on HTTP error."""
    base = dim_base.rstrip("/")
    sid = quote(session_id)
    with httpx.Client(verify=verify) as client:
        client.post(
            f"{base}/api/sessions",
            json={"sessionId": session_id},
            timeout=timeout_create,
        ).raise_for_status()
        client.post(
            f"{base}/api/sessions/{sid}/graph/append",
            json=pg_spec,
            timeout=timeout_append,
        ).raise_for_status()
        data = {"completed": ",".join(roots)} if roots else None
        client.post(
            f"{base}/api/sessions/{sid}/deploy",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=timeout_deploy,
        ).raise_for_status()


def wait_until_finished(
    dim_base: str,
    session_id: str,
    *,
    verify: bool = True,
    poll_interval: float = 3.0,
    timeout: float = 10.0,
) -> int:
    """Poll session status until FINISHED or ERROR. Prints status; returns 0 on success, 1 on error."""
    base = dim_base.rstrip("/")
    sid = quote(session_id)
    with httpx.Client(verify=verify) as client:
        while True:
            try:
                r = client.get(f"{base}/api/sessions/{sid}/status", timeout=timeout)
                r.raise_for_status()
                status = r.json()
            except Exception as e:
                print(f"status: {e}", file=sys.stderr)
                time.sleep(5)
                continue
            s = str(status)
            if "4" in s or "FINISHED" in s or "Finished" in s:
                print("Session finished.")
                try:
                    r = client.get(f"{base}/api/sessions/{sid}/graph/status", timeout=timeout)
                    r.raise_for_status()
                    print(json.dumps(r.json(), indent=2))
                except Exception as e:
                    print(f"(graph/status: {e})")
                return 0
            if "3" in s or "ERROR" in s or "Error" in s:
                print("Session error.", file=sys.stderr)
                print(json.dumps(status, indent=2), file=sys.stderr)
                return 1
            time.sleep(poll_interval)
