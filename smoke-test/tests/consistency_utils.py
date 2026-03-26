import logging
import time
from typing import List, Optional

import requests

from tests.utilities import env_vars

ELASTICSEARCH_REFRESH_INTERVAL_SECONDS: int = (
    env_vars.get_elasticsearch_refresh_interval_seconds()
)

logger = logging.getLogger(__name__)

# GMS Operations API endpoints for Kafka consumer lag
_KAFKA_LAG_ENDPOINTS = {
    "mcp": "/openapi/operations/kafka/mcp/consumer/offsets",
    "mcl": "/openapi/operations/kafka/mcl/consumer/offsets",
    "mcl_timeseries": "/openapi/operations/kafka/mcl-timeseries/consumer/offsets",
}


def _get_gms_url() -> str:
    return env_vars.get_gms_url() or "http://localhost:8080"


def _get_gms_token() -> Optional[str]:
    return env_vars.get_gms_token()


def _get_total_lag(gms_url: str, endpoint: str) -> Optional[int]:
    """Fetch total lag from a GMS Kafka Operations API endpoint.

    Returns the sum of totalLag across all consumer groups and topics,
    or None if the API call fails.
    """
    url = f"{gms_url}{endpoint}?skipCache=true"
    try:
        headers: dict = {}
        token = _get_gms_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = requests.get(url, headers=headers, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return 0
        total = 0
        for _group, topics in data.items():
            for _topic, info in topics.items():
                metrics = info.get("metrics")
                if metrics:
                    total += metrics.get("totalLag", 0)
        return total
    except Exception:
        return None


def _get_consumer_lag(gms_url: str, consumers: List[str]) -> Optional[int]:
    """Get combined lag across the requested consumer endpoints.

    Returns total lag, or None if any endpoint fails.
    """
    total = 0
    for consumer in consumers:
        endpoint = _KAFKA_LAG_ENDPOINTS.get(consumer)
        if not endpoint:
            continue
        lag = _get_total_lag(gms_url, endpoint)
        if lag is None:
            return None
        total += lag
    return total


def wait_for_writes_to_sync(
    max_timeout_in_sec: int = 120,
    mcp_only: bool = False,
    mae_only: bool = False,
    cdc_only: bool = False,
    consumer_group: str | None = None,  # Deprecated: for backward compatibility
) -> None:
    """Wait for Kafka consumer lag to reach zero using the GMS Operations API.

    Polls DataHub's built-in Kafka lag endpoints until all requested consumers
    have fully caught up, then waits an additional ES refresh interval for
    search index updates to become visible.

    Args:
        max_timeout_in_sec: Maximum time to wait in seconds (default: 120)
        mcp_only: If True, only wait for MCP consumer (for ingestion pipeline tests)
        mae_only: If True, only wait for MCL consumer (for entity update tests)
        cdc_only: Ignored (CDC has no dedicated lag endpoint; included for compat)
        consumer_group: (Deprecated) Ignored; kept for backward compatibility
    """
    # Determine which consumers to check
    if mcp_only:
        consumers = ["mcp"]
    elif mae_only:
        consumers = ["mcl"]
    else:
        # Default: wait for all consumers
        consumers = ["mcp", "mcl", "mcl_timeseries"]

    gms_url = _get_gms_url()
    start_time = time.time()
    lag_zero = False
    last_lag: Optional[int] = None

    while not lag_zero and (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)

        lag = _get_consumer_lag(gms_url, consumers)
        if lag is None:
            # API not available — fall back to static sleep
            logger.warning(
                "Kafka lag API unavailable, falling back to static sleep "
                f"({ELASTICSEARCH_REFRESH_INTERVAL_SECONDS}s)"
            )
            time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
            return

        last_lag = lag
        if lag == 0:
            lag_zero = True

    if not lag_zero:
        logger.warning(
            f"Timed out waiting for consumer lag to reach zero after "
            f"{max_timeout_in_sec}s. Last lag: {last_lag}"
        )
    else:
        logger.info(
            f"Consumer lag reached zero after "
            f"{time.time() - start_time:.1f}s, waiting {ELASTICSEARCH_REFRESH_INTERVAL_SECONDS}s "
            f"for ES refresh"
        )

    # Wait for Elasticsearch writes buffer to clear
    time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
