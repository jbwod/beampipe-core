import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List

import httpx

logger = logging.getLogger(__name__)


@dataclass
class DaliugeTranslatorClient:
    """Very small helper for DALiuGE translator REST calls."""

    base_url: str
    verify: bool = True
    timeout: float = 60.0

    def __post_init__(self) -> None:
        self._client = httpx.Client(
            base_url=self.base_url.rstrip("/"),
            verify=self.verify,
            timeout=self.timeout,
        )

    def close(self) -> None:
        self._client.close()

    def translate_lg_to_pgt(
        self,
        lg_name: str,
        lg_json: Dict[str, Any],
        *,
        algo: str = "metis",
        num_par: int = 1,
        num_islands: int = 0,
    ) -> str:
        data = {
            "lg_name": lg_name,
            "json_data": json.dumps(lg_json),
            "algo": algo,
            "num_par": str(num_par),
            "num_islands": str(num_islands),
        }
        resp = self._client.post(
            "/gen_pgt",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()

        # Turns out it increments the pgt_id by 1, it doesn't overwrite the previous one.
        match = re.search(r'var\s+pgtName\s*=\s*"([^"]+)";', resp.text or "")
        if match:
            return match.group(1)
        pgt_base = lg_name.rsplit(".", 1)[0]
        return f"{pgt_base}1_pgt.graph"

    def translate_pgt_to_pg(
        self,
        pgt_id: str,
        *,
        dim_host_for_tm: str,
        dim_port_for_tm: int,
    ) -> List[Dict[str, Any]]:
        params = {
            "pgt_id": pgt_id,
            "dlg_mgr_host": dim_host_for_tm,
            "dlg_mgr_port": str(dim_port_for_tm),
        }
        resp = self._client.get("/gen_pg", params=params)
        if resp.status_code >= 500:
            logger.error(
                "gen_pg failed: status=%s url=%s body=%s",
                resp.status_code,
                resp.url,
                resp.text[:500] if resp.text else "(empty)",
            )
        resp.raise_for_status()
        return resp.json()

