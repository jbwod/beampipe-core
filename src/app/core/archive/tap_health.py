"""TAP endpoint health checks for discovery skip.
https://status.pawsey.org.au/incidents/hwmll6ylzvyv
- CASDA TAP was actually down - good excuse to put in a health check
"""
import logging

import httpx

from .adapters import get_health_endpoints

logger = logging.getLogger(__name__)

async def is_tap_reachable(url: str, timeout_seconds: float = 10.0) -> bool:
    """Check if a TAP endpoint is reachable."""
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout_seconds,
            trust_env=False,
        ) as client:
            response = await client.get(url)
            if response.status_code < 400 or response.status_code == 405:
                logger.debug(
                    "event=tap_health_ok url=%s status_code=%s",
                    url,
                    response.status_code,
                )
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
    """Get the health of all TAP endpoints."""
    endpoints = get_health_endpoints()
    result: dict[str, bool] = {}
    for label, url in endpoints:
        result[label] = await is_tap_reachable(url, timeout_seconds=timeout_seconds)
    return result


def all_taps_reachable(health: dict[str, bool]) -> bool:
    """Return True if every endpoint in health is reachable."""
    return all(health.values())


def unreachable_taps(health: dict[str, bool]) -> list[str]:
    return [label for label, ok in health.items() if not ok]
