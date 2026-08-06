import logging
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Protocol

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


class _AuthenticatedSession(Protocol):
    def get(self, url: str, **kwargs: object) -> requests.Response: ...


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


def _fetch_lag_envelope(
    gms_url: str,
    endpoint: str,
    auth_session: Optional[_AuthenticatedSession] = None,
) -> Optional[dict]:
    url = f"{gms_url}{endpoint}?skipCache=true"
    try:
        if auth_session is not None:
            resp = auth_session.get(url, timeout=5)
        else:
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
    gms_url: str,
    endpoint: str,
    consumer_group: Optional[str] = None,
    auth_session: Optional[_AuthenticatedSession] = None,
) -> Optional[int]:
    """Fetch total lag from a GMS messaging consumer lag endpoint."""
    data = _fetch_lag_envelope(gms_url, endpoint, auth_session)
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


def _get_messaging_transport(
    gms_url: str, auth_session: Optional[_AuthenticatedSession] = None
) -> Optional[str]:
    try:
        url = f"{gms_url}/openapi/operations/messaging/transport"
        if auth_session is not None:
            resp = auth_session.get(url, timeout=5)
        else:
            resp = requests.get(url, headers=_request_headers(), timeout=5)
        resp.raise_for_status()
        return resp.json().get("transport")
    except Exception as e:
        logger.debug("Failed to read messaging transport: %s", e)
        return None


def _get_consumer_lag(
    gms_url: str,
    consumers: List[str],
    consumer_group: Optional[str] = None,
    auth_session: Optional[_AuthenticatedSession] = None,
) -> tuple[Optional[int], bool, bool]:
    """Get combined lag across endpoints.

    Fetches each consumer's lag endpoint concurrently rather than sequentially,
    since a broad wait (mcp+mcl+mcl_timeseries) would otherwise pay three
    round-trips per poll tick instead of one.

    Returns (lag, group_found, api_available).
    """
    endpoints = [
        (consumer, _MESSAGING_LAG_ENDPOINTS[consumer])
        for consumer in consumers
        if consumer in _MESSAGING_LAG_ENDPOINTS
    ]

    with ThreadPoolExecutor(max_workers=max(len(endpoints), 1)) as executor:
        futures = {
            executor.submit(
                _fetch_lag_envelope, gms_url, endpoint, auth_session
            ): consumer
            for consumer, endpoint in endpoints
        }
        envelopes = {futures[future]: future.result() for future in futures}

    total = 0
    group_found = consumer_group is None
    api_available = False
    for consumer, _endpoint in endpoints:
        data = envelopes[consumer]
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
    consumer_group: str,
    max_timeout_in_sec: int,
    topic: Optional[str] = None,
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
        cmd = [
            "docker",
            "exec",
            broker_container,
            "/bin/kafka-consumer-groups",
            "--bootstrap-server",
            kafka_bootstrap,
            "--group",
            consumer_group,
            "--describe",
        ]
        try:
            completed_process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
            )
            if completed_process.returncode != 0:
                logger.warning(
                    "Kafka lag command failed for consumer group %s: %s",
                    consumer_group,
                    completed_process.stderr.strip(),
                )
                return False

            lag_values = []
            for line in completed_process.stdout.splitlines():
                columns = line.split()
                if (
                    len(columns) >= 6
                    and columns[0] != "GROUP"
                    and (topic is None or columns[1] == topic)
                ):
                    lag_values.append(int(columns[5]))
            if not lag_values:
                continue
            if max(lag_values) == 0:
                logger.info(
                    "Kafka consumer group %s lag reached zero via broker CLI",
                    consumer_group,
                )
                return True
        except ValueError:
            logger.warning(
                "Error reading Kafka lag for consumer group %s",
                consumer_group,
                exc_info=True,
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
    auth_session: Optional[_AuthenticatedSession] = None,
    legacy_wait: bool = False,
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
        auth_session: Base authenticated test session used for GMS operations.
        legacy_wait: If True, use the old aggregate consumer-group-lag polling.
            Defaults to False, which instead captures a fixed offset checkpoint
            at call time and waits only for the consumer to pass it -- immune to
            the "lag never reaches zero under concurrent writers" problem that
            aggregate-lag polling has (see wait_for_offsets_to_be_consumed).
            Always uses the legacy path when consumer_group is set, since the
            offset-checkpoint path doesn't cover the usage-event consumer group.
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
        transport = _get_messaging_transport(gms_url, auth_session)
        if transport == "kafka":
            _wait_for_kafka_consumer_group_lag(
                consumer_group,
                max_timeout_in_sec,
                topic=env_vars.get_datahub_usage_event_topic(),
            )
            time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
            return

    # The offset-checkpoint path only covers mcp/mcl/mcl_timeseries; fall back
    # to legacy aggregate-lag polling for any consumer_group-scoped call.
    if not legacy_wait and not consumer_group:
        wait_for_offsets_to_be_consumed(
            consumers,
            max_timeout_in_sec=max_timeout_in_sec,
            auth_session=auth_session,
        )
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return

    start_time = time.time()
    lag_zero = False
    last_lag: Optional[int] = None
    used_kafka_fallback = False

    while not lag_zero and (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)

        lag, group_found, api_available = _get_consumer_lag(
            gms_url, consumers, consumer_group, auth_session
        )
        if (
            consumer_group
            and api_available
            and not group_found
            and _wait_for_kafka_consumer_group_lag(
                consumer_group,
                max(1, int(max_timeout_in_sec - (time.time() - start_time))),
                topic=(
                    env_vars.get_datahub_usage_event_topic()
                    if consumer_group == _USAGE_EVENT_CONSUMER_GROUP
                    else None
                ),
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


# ============================================================================
# Offset-checkpoint based wait, used by wait_for_writes_to_sync() by default
# (legacy_wait=False) for the mcp/mcl/mcl_timeseries case.
#
# The legacy path above polls *aggregate* consumer-group lag, which is a
# moving target under concurrent xdist workers: any other worker's in-flight
# write keeps lag non-zero, so a caller has to wait for everyone to be caught
# up, not just its own write -- under sustained concurrent load this can fail
# to converge at all (a synthetic benchmark timed out the legacy path in 2 of
# 3 runs under continuous 3-way concurrent writes; this approach resolved in
# ~0.28s every time). Under real pytest-xdist load the gain is smaller and
# bimodal: calls that don't land during a genuine concurrent write burst
# still resolve in ~0.3s, but calls whose checkpoint is captured mid-burst
# still have to wait for that already-queued backlog to drain (~7-9s
# observed), since a fixed checkpoint captured during a burst already
# includes it. The real win here is eliminating the "never converges" tail
# risk, not eliminating genuine backlog-drain time. This instead captures
# each relevant
# topic-partition's current end-offset ONCE at call time (a fixed checkpoint
# that already includes this call's own write, since the write happened
# before this function was called) and polls only until the consumer's
# committed offset has passed that fixed checkpoint. Later writes from other
# workers don't move the target, so this is immune to the "everyone else's
# writes" pollution that inflates the legacy wait.
#
# Uses the existing /openapi/operations/messaging/{type}/consumer/lag
# endpoint with detailed=true, which already returns per-partition
# {offset (consumer's committed offset), lag} -- offset + lag gives the
# topic's current end-offset. No GMS-side changes needed. This avoids the
# ~1.4s-per-call JVM startup cost of shelling out to kafka-consumer-groups
# (an earlier version of this prototype did that; the HTTP endpoint responds
# in ~20-50ms instead).
# ============================================================================


def _fetch_detailed_lag_envelope(
    gms_url: str,
    endpoint: str,
    auth_session: Optional[_AuthenticatedSession] = None,
) -> Optional[dict]:
    url = f"{gms_url}{endpoint}?skipCache=true&detailed=true"
    try:
        if auth_session is not None:
            resp = auth_session.get(url, timeout=5)
        else:
            resp = requests.get(url, headers=_request_headers(), timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.debug("Detailed lag fetch failed for %s: %s", endpoint, e)
        return None


def _fetch_detailed_partitions(
    gms_url: str,
    endpoint: str,
    auth_session: Optional[_AuthenticatedSession] = None,
) -> dict:
    """Returns {partition: (current_offset, lag)} from the detailed lag envelope.

    Assumes a single consumer group per topic (true for mcp/mcl/mcl_timeseries
    today); takes the first group found if there happen to be more.
    """
    data = _fetch_detailed_lag_envelope(gms_url, endpoint, auth_session)
    if not data:
        return {}
    consumer_groups = data.get("consumerGroups", {})
    for _group_name, topics in consumer_groups.items():
        for _topic_name, topic_info in topics.items():
            partitions = topic_info.get("partitions") or {}
            return {
                int(p): (info["offset"], info.get("lag") or 0)
                for p, info in partitions.items()
            }
    return {}


def wait_for_offsets_to_be_consumed(
    consumers: List[str],
    max_timeout_in_sec: int = 60,
    poll_interval_sec: float = 0.25,
    auth_session: Optional[_AuthenticatedSession] = None,
) -> None:
    """PROTOTYPE. Wait for consumer offsets to pass a fixed checkpoint captured
    at call time, instead of polling a continuously-refreshed aggregate lag.

    Args:
        consumers: subset of "mcp", "mcl", "mcl_timeseries" to wait on.
        max_timeout_in_sec: safety ceiling.
        poll_interval_sec: how often to re-check (much finer than the 1s
            granularity of wait_for_writes_to_sync's poll loop, since each
            check is now a ~20-50ms HTTP call rather than a ~1.4s CLI spawn).
        auth_session: base authenticated test session used for GMS operations.
    """
    start_time = time.time()
    gms_url = _get_gms_url()

    targets: dict = {}
    for consumer_type in consumers:
        endpoint = _MESSAGING_LAG_ENDPOINTS[consumer_type]
        partitions = _fetch_detailed_partitions(gms_url, endpoint, auth_session)
        for partition, (offset, lag) in partitions.items():
            targets[(consumer_type, partition)] = offset + lag

    remaining = dict(targets)
    while remaining and (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(poll_interval_sec)
        for consumer_type in {c for (c, _p) in remaining}:
            endpoint = _MESSAGING_LAG_ENDPOINTS[consumer_type]
            partitions = _fetch_detailed_partitions(gms_url, endpoint, auth_session)
            for partition, (offset, _lag) in partitions.items():
                key = (consumer_type, partition)
                if key in remaining and offset >= remaining[key]:
                    del remaining[key]

    elapsed = time.time() - start_time
    if remaining:
        logger.warning(
            f"Timed out after {elapsed:.1f}s waiting for offsets to be consumed: {remaining}"
        )
    else:
        logger.info(
            f"All {len(targets)} target offset(s) consumed after {elapsed:.2f}s"
        )
