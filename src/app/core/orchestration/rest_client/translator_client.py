import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


def pgt_handle_from_partitioned_payload(pgt_json: Any, fallback_lg_name: str) -> str:
    if isinstance(pgt_json, list) and len(pgt_json) > 0 and isinstance(pgt_json[0], str):
        return pgt_json[0]
    base = fallback_lg_name.rsplit("/", 1)[-1]
    return base


def partitioned_pgt_for_dlg_deploy(pgt_json: Any, lg_name: str) -> list[Any]:
    if (
        isinstance(pgt_json, list)
        and len(pgt_json) == 2
        and isinstance(pgt_json[0], str)
        and isinstance(pgt_json[1], list)
    ):
        return pgt_json
    base = lg_name.rsplit("/", 1)[-1]
    # depnds on what we actually save with I suppose
    if base.endswith(".graph"):
        pgt_filename = base[: -len(".graph")] + "_pgt.graph"
    else:
        pgt_filename = f"{base}.pgt.graph"
    return [pgt_filename, pgt_json]


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
        lg_json: dict[str, Any],
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
    ) -> list[dict[str, Any]]:
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
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError(f"gen_pg expected JSON list, got {type(data).__name__}")
        out: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                raise ValueError(f"gen_pg list items must be objects, got {type(item).__name__}")
            out.append(item)
        return out

    def unroll_and_partition_lg(
        self,
        lg_name: str,
        lg_json: dict[str, Any],
        *,
        algo: str = "metis",
        num_par: int = 1,
        num_islands: int = 0,
    ) -> Any:
        """Call /unroll_and_partition and return the TM JSON
        Wrap with `partitioned_pgt_for_dlg_deploy`
        """
        np = int(num_par) if num_par is not None else 1
        if np < 1:
            np = 1
        ni = int(num_islands) if num_islands is not None else 1
        if ni < 1:
            ni = 1
        data = {
            "lg_content": json.dumps(lg_json),
            "num_partitions": str(np),
            "num_islands": str(ni),
            "algorithm": algo,
        }
        resp = self._client.post("/unroll_and_partition", data=data)
        if resp.status_code >= 500:
            logger.error(
                "unroll_and_partition failed: status=%s url=%s body=%s",
                resp.status_code,
                resp.url,
                resp.text[:500] if resp.text else "(empty)",
            )
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError as e:
            raise ValueError(f"unroll_and_partition invalid JSON for lg={lg_name!r}: {e}") from e

