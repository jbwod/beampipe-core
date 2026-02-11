"""TAP endpoint health checks for discovery skip.
https://status.pawsey.org.au/incidents/hwmll6ylzvyv
- CASDA TAP was actually down - good excuse to put in a health check
"""
import logging

import httpx

from .adapters.casda import CASDA_TAP_URL
from .adapters.vizier import VIZIER_TAP_URL

logger = logging.getLogger(__name__)

# Vizier async endpoint for health check (UWS jobs list; returns 200 + XML when up)
VIZIER_TAP_HEALTH_URL = "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/async"

DISCOVERY_TAP_ENDPOINTS: list[tuple[str, str]] = [
    ("casda", CASDA_TAP_URL),
    ("vizier", VIZIER_TAP_HEALTH_URL),
]


async def is_tap_reachable(url: str, timeout_seconds: float = 10.0) -> bool:
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout_seconds,
        ) as client:
            response = await client.get(url)
            if response.status_code < 400 or response.status_code == 405:
                return True
            logger.warning(
                "event=tap_health_bad_status url=%s status_code=%s",
                url,
                response.status_code,
            )
            return False
    except (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError) as e:
        logger.warning(
            "event=tap_health_unreachable url=%s error=%s",
            url,
            e,
        )
        return False
    except Exception as e:
        logger.warning(
            "event=tap_health_error url=%s error=%s",
            url,
            e,
        )
        return False


async def get_tap_health(timeout_seconds: float = 10.0) -> dict[str, bool]:
    result: dict[str, bool] = {}
    for label, url in DISCOVERY_TAP_ENDPOINTS:
        result[label] = await is_tap_reachable(url, timeout_seconds=timeout_seconds)
    return result


def all_taps_reachable(health: dict[str, bool]) -> bool:
    """Return True if every endpoint in health is reachable."""
    return all(health.values())


def unreachable_taps(health: dict[str, bool]) -> list[str]:
    return [label for label, ok in health.items() if not ok]
