"""Load shaping tbd."""

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from sqlalchemy import and_, distinct, func, select

from ...models.ledger import BatchExecutionRecord, ExecutionStatus
from ...models.registry import SourceRegistry

logger = logging.getLogger(__name__)

# https://redis.io/tutorials/howtos/ratelimiting/#4-token-bucket
# After some investigation seems like may not be scoped to the project, but certainly interesting.
_BUCKET_DISCOVERY = "beampipe:shape:bucket:discovery"
_BUCKET_EXECUTE = "beampipe:shape:bucket:execute"
_LUA_DIR = Path(__file__).with_name("lua")


def _read_lua_script(filename: str) -> str:
    return (_LUA_DIR / filename).read_text(encoding="utf-8")


_TOKEN_BUCKET_LUA = _read_lua_script("token_bucket.lua")
_LEAKY_BUCKET_LUA = _read_lua_script("leaky_bucket.lua")


def shaping_queue_max_depth(settings: Any) -> int | None:
    return cast(int | None, settings.SHAPING_ARQ_QUEUE_MAX_DEPTH)


def discovery_queue_max_depth(settings: Any) -> int | None:
    return shaping_queue_max_depth(settings)


def can_admit_by_in_flight(*, current: int, max_in_flight: int | None) -> bool:
    if max_in_flight is None:
        return True
    return int(current) < int(max_in_flight)


async def discovery_admission_budget(
    redis: Any,
    *,
    desired_sources: int,
    settings: Any | None = None,
) -> int:
    return max(0, int(desired_sources))


async def execute_admission_budget(
    redis: Any,
    *,
    desired_runs: int,
    settings: Any | None = None,
) -> int:
    return max(0, int(desired_runs))


async def count_execute_in_flight_runs(
    db: Any,
    *,
    settings: Any | None = None,
) -> int:
    from ..config import settings as global_settings

    s = settings or global_settings
    result = await db.execute(
        select(func.count(BatchExecutionRecord.uuid)).where(
            and_(
                BatchExecutionRecord.scheduler_name == s.WORKFLOW_AUTOMATION_SCHEDULER_NAME,
                BatchExecutionRecord.status.in_(
                    [ExecutionStatus.PENDING, ExecutionStatus.RUNNING, ExecutionStatus.RETRYING]
                ),
            )
        )
    )
    return int(result.scalar() or 0)


async def estimate_discovery_in_flight_batches(
    db: Any,
    redis: Any,
    *,
    queue_name: str,
    project_module: str | None = None,
) -> int:
    active_claims = 0
    active_claims_ok = False
    now = datetime.now(UTC)
    try:
        filters = [
            SourceRegistry.discovery_claim_token.is_not(None),
            SourceRegistry.discovery_claim_expires_at.is_not(None),
            SourceRegistry.discovery_claim_expires_at > now,
        ]
        if project_module is not None:
            filters.append(SourceRegistry.project_module == project_module)
        result = await db.execute(
            select(func.count(distinct(SourceRegistry.discovery_claim_token))).where(and_(*filters))
        )
        active_claims = int(result.scalar() or 0)
        active_claims_ok = True
    except Exception:
        active_claims = 0

    if active_claims_ok:
        return active_claims
    try:
        return int(await redis.zcard(queue_name))
    except Exception:
        return 0


async def arq_queue_depth_allows_enqueue(
    redis: Any,
    *,
    queue_name: str,
    max_depth: int | None,
) -> tuple[bool, int | None]:
    """If max_depth is None, always allow. Otherwise allow when zcard < max_depth."""
    if max_depth is None:
        try:
            depth = int(await redis.zcard(queue_name))
        except Exception:
            return True, None
        return True, depth

    try:
        depth = int(await redis.zcard(queue_name))
    except Exception as exc:
        logger.warning(
            "event=shaping_queue_depth_unavailable queue=%s error=%s",
            queue_name,
            exc,
            exc_info=True,
        )
        return True, None

    if depth >= max_depth:
        return False, depth
    return True, depth


async def shaping_enqueue_pace(settings: Any | None = None) -> None:
    """Optional delay after a successful enqueue to smooth bursts."""
    from ..config import settings as global_settings

    s = settings or global_settings
    ms = float(getattr(s, "SHAPING_ENQUEUE_PACING_MS", 0.0) or 0.0)
    if ms > 0:
        await asyncio.sleep(ms / 1000.0)
