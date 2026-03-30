from typing import Any
from uuid import UUID

from ...db.database import local_session
from .run_execution import execute_orchestration_run


async def execute_run_job(ctx: dict[str, Any], run_id: str) -> dict[str, Any]:
    _ = ctx
    run_uuid = UUID(str(run_id))
    async with local_session() as db:
        result = await execute_orchestration_run(
            db=db, run_id=run_uuid, do_stage=True, do_submit=True
        )
        return {"ok": True, "run_id": str(run_uuid), "result": result}
