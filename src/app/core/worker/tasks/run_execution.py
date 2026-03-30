from typing import Any
from uuid import UUID


async def execute_orchestration_run(
    db: Any,
    run_id: UUID,
    *,
    do_stage: bool = True,
    do_submit: bool = True,
) -> dict[str, Any]:
    from ...orchestration.service import execute_run as orchestration_execute_run

    return await orchestration_execute_run(
        db=db, run_id=run_id, do_stage=do_stage, do_submit=do_submit
    )
