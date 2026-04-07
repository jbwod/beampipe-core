"""Load shaping: queue backpressure, workflow ceilings, enqueue pacing."""
import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


def discovery_queue_max_depth(settings: Any) -> int | None:
    """Tightest queue depth cap for discovery scheduling (shaping + legacy discovery cap)."""
    shaping = settings.SHAPING_ARQ_QUEUE_MAX_DEPTH
    legacy = settings.DISCOVERY_MAX_QUEUE_DEPTH
    candidates = [x for x in (shaping, legacy) if x is not None]
    if not candidates:
        return None
    return min(candidates)


def apply_workflow_shaping_ceiling(
    policy: dict[str, Any],
    *,
    settings: Any | None = None,
) -> dict[str, Any]:
    """Copy module workflow policy and apply global SHAPING_* ceilings."""
    from ..config import settings as global_settings

    s = settings or global_settings
    out = dict(policy)

    ceiling_runs = s.SHAPING_WORKFLOW_MAX_RUNS_PER_TICK_CEILING
    if ceiling_runs is not None:
        out["max_runs_per_tick"] = min(
            max(1, int(out["max_runs_per_tick"])),
            max(1, int(ceiling_runs)),
        )

    ceiling_sources = s.SHAPING_WORKFLOW_MAX_SOURCES_PER_TICK_CEILING
    if ceiling_sources is not None:
        out["max_sources_per_tick"] = min(
            max(1, int(out["max_sources_per_tick"])),
            max(1, int(ceiling_sources)),
        )

    return out


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
