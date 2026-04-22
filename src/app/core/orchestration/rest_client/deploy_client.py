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
import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from ...utils.daliuge import classify_dim_session_status, dim_graph_status_error_uids

logger = logging.getLogger(__name__)


@dataclass
class DaliugeDeployClient:
    """Helper for DIM REST deploy and session polling."""

    base_url: str
    verify: bool = True
    timeout: float = 60.0
    timeout_create: float = 30.0
    timeout_append: float = 60.0
    timeout_deploy: float = 30.0

    def __post_init__(self) -> None:
        self._client = httpx.Client(
            base_url=self.base_url.rstrip("/"),
            verify=self.verify,
            timeout=self.timeout,
        )

    def close(self) -> None:
        self._client.close()

    def deploy_session(
        self,
        session_id: str,
        pg_spec: list[dict[str, Any]],
        roots: list[str],
    ) -> None:
        """Create session, append graph, deploy. Raises on HTTP error."""
        sid = quote(session_id)
        self._client.post(
            "/api/sessions",
            json={"sessionId": session_id},
            timeout=self.timeout_create,
        ).raise_for_status()
        self._client.post(
            f"/api/sessions/{sid}/graph/append",
            json=pg_spec,
            timeout=self.timeout_append,
        ).raise_for_status()
        data = {"completed": ",".join(roots)} if roots else None
        self._client.post(
            f"/api/sessions/{sid}/deploy",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.timeout_deploy,
        ).raise_for_status()

    def wait_until_finished(
        self,
        session_id: str,
        *,
        poll_interval: float = 3.0,
        timeout: float = 10.0,
    ) -> int:
        """Poll session status until FINISHED, FAILED, or CANCELLED."""
        sid = quote(session_id)
        while True:
            try:
                r = self._client.get(
                    f"/api/sessions/{sid}/status",
                    timeout=timeout,
                )
                r.raise_for_status()
                status = r.json()
            except Exception as e:
                logger.warning("event=dim_poll_status_error session_id=%s error=%s", session_id, e)
                time.sleep(5)
                continue

            state = classify_dim_session_status(status)

            if state == "finished":
                logger.info("event=dim_session_finished session_id=%s", session_id)
                try:
                    r = self._client.get(
                        f"/api/sessions/{sid}/graph/status",
                        timeout=timeout,
                    )
                    r.raise_for_status()
                    graph = r.json()
                    logger.debug("event=dim_graph_status session_id=%s graph=%s", session_id, json.dumps(graph))
                    if isinstance(graph, dict):
                        error_drops = dim_graph_status_error_uids(graph)
                        if error_drops:
                            logger.error(
                                "event=dim_session_drop_errors session_id=%s error_count=%s",
                                session_id,
                                len(error_drops),
                            )
                            return 1
                except Exception as e:
                    logger.warning("event=dim_graph_status_error session_id=%s error=%s", session_id, e)
                return 0

            if state in ("failed", "cancelled"):
                logger.error(
                    "event=dim_session_error session_id=%s status=%s",
                    session_id,
                    json.dumps(status),
                )
                return 1

            time.sleep(poll_interval)
