import logging
import subprocess
import time
from typing import List, Optional

import requests

from tests.utilities import env_vars

ELASTICSEARCH_REFRESH_INTERVAL_SECONDS: int = (
    env_vars.get_elasticsearch_refresh_interval_seconds()
)

logger = logging.getLogger(__name__)

_USAGE_EVENT_CONSUMER_GROUP = "datahub-usage-event-consumer-job-client"

# Transport-neutral messaging lag endpoints (preferred over deprecated /kafka/ endpoints)
_MESSAGING_LAG_ENDPOINTS = {
    "mcp": "/openapi/operations/messaging/mcp/consumer/lag",
    "mcl": "/openapi/operations/messaging/mcl/consumer/lag",
    "mcl_timeseries": "/openapi/operations/messaging/mcl-timeseries/consumer/lag",
    "usage_events": "/openapi/operations/messaging/usage-events/consumer/lag",
}


def _get_gms_url() -> str:
    return env_vars.get_gms_url() or "http://localhost:8080"


def _get_gms_token() -> Optional[str]:
    return env_vars.get_gms_token()


def _request_headers() -> dict:
    headers: dict = {}
    token = _get_gms_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _fetch_lag_envelope(gms_url: str, endpoint: str) -> Optional[dict]:
    url = f"{gms_url}{endpoint}?skipCache=true"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.debug("Lag fetch failed for %s: %s", endpoint, e)
        return None


def _sum_lag_from_envelope(
    data: dict, consumer_group: Optional[str] = None
) -> tuple[Optional[int], bool]:
    """Return (total_lag, group_found).

    When consumer_group is set, only lag for that group is summed and group_found
    indicates whether the group appeared in the response.
    """
    if not data:
        return 0, consumer_group is None
    consumer_groups = data.get("consumerGroups", {})
    if not consumer_groups:
        return 0, consumer_group is None

    total = 0
    group_found = consumer_group is None
    for group_name, topics in consumer_groups.items():
        if consumer_group is not None and group_name != consumer_group:
            continue
        if consumer_group is not None:
            group_found = True
        for _topic, info in topics.items():
            metrics = info.get("metrics")
            if metrics:
                total += metrics.get("totalLag", 0)
    if consumer_group is not None and not group_found:
        return None, False
    return total, group_found


def _get_total_lag(
    gms_url: str, endpoint: str, consumer_group: Optional[str] = None
) -> Optional[int]:
    """Fetch total lag from a GMS messaging consumer lag endpoint."""
    data = _fetch_lag_envelope(gms_url, endpoint)
    if data is None:
        return None
    lag, _group_found = _sum_lag_from_envelope(data, consumer_group)
    return lag


def _endpoints_for_consumer_group(consumer_group: str) -> List[str]:
    if consumer_group == _USAGE_EVENT_CONSUMER_GROUP:
        return ["usage_events"]
    if consumer_group.startswith("generic-mae-consumer-job-client"):
        return ["mcl", "mcl_timeseries"]
    if consumer_group.endswith("mce-consumer-job-client") or consumer_group.startswith(
        "mce-consumer"
    ):
        return ["mcp"]
    return ["mcp", "mcl", "mcl_timeseries", "usage_events"]


def _get_messaging_transport(gms_url: str) -> Optional[str]:
    try:
        resp = requests.get(
            f"{gms_url}/openapi/operations/messaging/transport",
            headers=_request_headers(),
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json().get("transport")
    except Exception as e:
        logger.debug("Failed to read messaging transport: %s", e)
        return None


def _get_consumer_lag(
    gms_url: str, consumers: List[str], consumer_group: Optional[str] = None
) -> tuple[Optional[int], bool, bool]:
    """Get combined lag across endpoints.

    Returns (lag, group_found, api_available).
    """
    total = 0
    group_found = consumer_group is None
    api_available = False
    for consumer in consumers:
        endpoint = _MESSAGING_LAG_ENDPOINTS.get(consumer)
        if not endpoint:
            continue
        data = _fetch_lag_envelope(gms_url, endpoint)
        if data is None:
            continue
        api_available = True
        lag, found = _sum_lag_from_envelope(data, consumer_group)
        if lag is None:
            continue
        if consumer_group is not None and found:
            group_found = True
        total += lag
    if not api_available:
        return None, False, False
    if consumer_group is not None and not group_found:
        return None, False, True
    return total, group_found, True


def _infer_kafka_broker_container() -> str:
    cmd = "docker ps --format '{{.Names}}' | grep broker"
    completed_process = subprocess.run(
        cmd,
        capture_output=True,
        shell=True,
        text=True,
        check=False,
    )
    lines = str(completed_process.stdout).splitlines()
    if not lines:
        raise ValueError("No Kafka broker containers found")
    return lines[0]


def _wait_for_kafka_consumer_group_lag(
    consumer_group: str, max_timeout_in_sec: int
) -> bool:
    """Poll kafka-consumer-groups for a single consumer group (Kafka transport fallback)."""
    kafka_bootstrap = env_vars.get_kafka_bootstrap_server()
    broker_container = (
        env_vars.get_kafka_broker_container() or _infer_kafka_broker_container()
    )
    start_time = time.time()
    lag_values: List[int] = []

    while (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)
        cmd = (
            f"docker exec {broker_container} /bin/kafka-consumer-groups "
            f"--bootstrap-server {kafka_bootstrap} --group '{consumer_group}' --describe "
            "| grep -v LAG | awk '{print $6}'"
        )
        try:
            completed_process = subprocess.run(
                cmd,
                capture_output=True,
                shell=True,
                text=True,
                check=False,
            )
            lines = [
                int(line)
                for line in str(completed_process.stdout).splitlines()
                if line.strip() != ""
            ]
            if not lines:
                continue
            lag_values = lines
            if max(lag_values) == 0:
                logger.info(
                    "Kafka consumer group %s lag reached zero via broker CLI",
                    consumer_group,
                )
                return True
        except ValueError:
            logger.warning(
                "Error reading kafka lag using command: %s", cmd, exc_info=True
            )

    logger.warning(
        "Timed out waiting for Kafka consumer group %s lag (last values: %s)",
        consumer_group,
        lag_values,
    )
    return False


def wait_for_writes_to_sync(
    max_timeout_in_sec: int = 120,
    mcp_only: bool = False,
    mae_only: bool = False,
    cdc_only: bool = False,
    consumer_group: str | None = None,
) -> None:
    """Wait for consumer lag to reach zero using the GMS messaging operations API.

    Polls the transport-neutral consumer lag endpoints until all requested
    consumers have fully caught up, then waits an additional ES refresh interval
    for search index updates to become visible. Works with both Kafka and pgQueue
    transports.

    Args:
        max_timeout_in_sec: Maximum time to wait in seconds (default: 120)
        mcp_only: If True, wait for MCP and MCL (ingestion pipeline: proposal + indexing)
        mae_only: If True, only wait for MCL versioned consumer (entity update tests)
        cdc_only: Ignored (CDC has no dedicated lag endpoint; included for compat)
        consumer_group: When set, wait only for this consumer group's lag (e.g.
            ``datahub-usage-event-consumer-job-client`` for audit-event indexing).
            Falls back to ``kafka-consumer-groups`` when the group is not exposed
            via the messaging lag API (Kafka usage-event consumer).
    """
    if env_vars.get_use_static_sleep():
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return

    if consumer_group:
        consumers = _endpoints_for_consumer_group(consumer_group)
    elif mcp_only:
        consumers = ["mcp", "mcl"]
    elif mae_only:
        consumers = ["mcl"]
    else:
        consumers = ["mcp", "mcl", "mcl_timeseries"]

    gms_url = _get_gms_url()

    # Usage events on Kafka are not exposed via trace readers; use broker CLI lag.
    if consumer_group == _USAGE_EVENT_CONSUMER_GROUP:
        transport = _get_messaging_transport(gms_url)
        if transport == "kafka":
            _wait_for_kafka_consumer_group_lag(consumer_group, max_timeout_in_sec)
            time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
            return

    start_time = time.time()
    lag_zero = False
    last_lag: Optional[int] = None
    used_kafka_fallback = False

    while not lag_zero and (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)

        lag, group_found, api_available = _get_consumer_lag(
            gms_url, consumers, consumer_group
        )
        if (
            consumer_group
            and api_available
            and not group_found
            and _wait_for_kafka_consumer_group_lag(
                consumer_group,
                max(1, int(max_timeout_in_sec - (time.time() - start_time))),
            )
        ):
            used_kafka_fallback = True
            lag_zero = True
            break

        if not api_available:
            has_token = _get_gms_token() is not None
            logger.warning(
                "Messaging lag API unavailable (gms_url=%s, has_token=%s), "
                "falling back to static sleep (%ds)",
                gms_url,
                has_token,
                ELASTICSEARCH_REFRESH_INTERVAL_SECONDS,
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
            f"{time.time() - start_time:.1f}s"
            f"{' (kafka CLI fallback)' if used_kafka_fallback else ''}, "
            f"waiting {ELASTICSEARCH_REFRESH_INTERVAL_SECONDS}s for ES refresh"
        )

    time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
