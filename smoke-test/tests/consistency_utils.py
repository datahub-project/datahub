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


class LagAuthDeniedError(Exception):
    """Lag or transport endpoint returned HTTP 401/403."""


def _get_gms_url() -> str:
    return env_vars.get_gms_url() or "http://localhost:8080"


def _get_gms_token() -> Optional[str]:
    return env_vars.get_gms_token()


def _lag_monitor_token() -> str:
    """Bootstrap admin PAT from DATAHUB_GMS_TOKEN (set by conftest)."""
    token = _get_gms_token()
    if not token:
        raise RuntimeError(
            "wait_for_writes_to_sync requires DATAHUB_GMS_TOKEN with access to "
            "messaging lag endpoints (VIEW_SYSTEM_STATUS or MANAGE_SYSTEM_OPERATIONS)."
        )
    return token


def _request_headers() -> dict:
    return {"Authorization": f"Bearer {_lag_monitor_token()}"}


def _raise_if_auth_denied(resp: requests.Response, endpoint: str) -> None:
    if resp.status_code in (401, 403):
        raise LagAuthDeniedError(f"{endpoint} returned HTTP {resp.status_code}")


def _fetch_lag_envelope(
    gms_url: str,
    endpoint: str,
) -> Optional[dict]:
    url = f"{gms_url}{endpoint}?skipCache=true"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=5)
        _raise_if_auth_denied(resp, endpoint)
        resp.raise_for_status()
        return resp.json()
    except LagAuthDeniedError:
        raise
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
        url = f"{gms_url}/openapi/operations/messaging/transport"
        resp = requests.get(url, headers=_request_headers(), timeout=5)
        _raise_if_auth_denied(resp, "/openapi/operations/messaging/transport")
        resp.raise_for_status()
        return resp.json().get("transport")
    except LagAuthDeniedError:
        raise
    except Exception as e:
        logger.debug("Failed to read messaging transport: %s", e)
        return None


def _get_consumer_lag(
    gms_url: str,
    consumers: List[str],
    consumer_group: Optional[str] = None,
) -> tuple[Optional[int], bool, bool, bool]:
    """Get combined lag across endpoints.

    Fetches each consumer's lag endpoint concurrently rather than sequentially,
    since a broad wait (mcp+mcl+mcl_timeseries) would otherwise pay three
    round-trips per poll tick instead of one.

    Returns (lag, group_found, api_available, auth_denied).
    """
    endpoints = [
        (consumer, _MESSAGING_LAG_ENDPOINTS[consumer])
        for consumer in consumers
        if consumer in _MESSAGING_LAG_ENDPOINTS
    ]

    auth_denied = False
    envelopes: dict = {}
    with ThreadPoolExecutor(max_workers=max(len(endpoints), 1)) as executor:
        futures = {
            executor.submit(_fetch_lag_envelope, gms_url, endpoint): consumer
            for consumer, endpoint in endpoints
        }
        for future in futures:
            consumer = futures[future]
            try:
                envelopes[consumer] = future.result()
            except LagAuthDeniedError as e:
                logger.debug("Lag fetch auth denied for %s: %s", consumer, e)
                auth_denied = True
                envelopes[consumer] = None

    if auth_denied:
        return None, False, False, True

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
        return None, False, False, False
    if consumer_group is not None and not group_found:
        return None, False, True, False
    return total, group_found, True, False


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


class _LagAuthTracker:
    """Retry 401/403 for a short window, then fail with a grant hint."""

    def __init__(self) -> None:
        self.timeout = env_vars.get_lag_auth_timeout_seconds()
        self.denied_since: Optional[float] = None

    def note_denied(self) -> None:
        now = time.time()
        if self.denied_since is None:
            self.denied_since = now
            logger.warning(
                "Messaging lag API returned 401/403; retrying for up to %ss. "
                "Grant VIEW_SYSTEM_STATUS or MANAGE_SYSTEM_OPERATIONS if this persists.",
                self.timeout,
            )
        if now - self.denied_since >= self.timeout:
            raise RuntimeError(
                "Messaging lag API returned 401/403 for "
                f"{self.timeout:g}s. Grant VIEW_SYSTEM_STATUS or "
                "MANAGE_SYSTEM_OPERATIONS to the DATAHUB_GMS_TOKEN user."
            )

    def note_ok(self) -> None:
        self.denied_since = None


def _select_wait_consumers(
    consumer_group: Optional[str], mcp_only: bool, mae_only: bool
) -> List[str]:
    if consumer_group:
        return _endpoints_for_consumer_group(consumer_group)
    if mcp_only:
        return ["mcp", "mcl"]
    if mae_only:
        return ["mcl"]
    return ["mcp", "mcl", "mcl_timeseries"]


def _try_kafka_usage_event_wait(
    gms_url: str,
    consumer_group: str,
    max_timeout_in_sec: int,
    auth: _LagAuthTracker,
) -> bool:
    try:
        transport = _get_messaging_transport(gms_url)
    except LagAuthDeniedError:
        auth.note_denied()
        return False
    if transport != "kafka":
        return False
    _wait_for_kafka_consumer_group_lag(
        consumer_group,
        max_timeout_in_sec,
        topic=env_vars.get_datahub_usage_event_topic(),
    )
    time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
    return True


def _try_offset_checkpoint_wait(
    consumers: List[str],
    max_timeout_in_sec: int,
    auth: _LagAuthTracker,
) -> bool:
    try:
        checkpoint_established = wait_for_offsets_to_be_consumed(
            consumers,
            max_timeout_in_sec=max_timeout_in_sec,
        )
    except LagAuthDeniedError:
        auth.note_denied()
        return False
    if not checkpoint_established:
        return False
    time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
    return True


def _poll_aggregate_lag(
    gms_url: str,
    consumers: List[str],
    consumer_group: Optional[str],
    max_timeout_in_sec: int,
    auth: _LagAuthTracker,
) -> None:
    start_time = time.time()
    lag_zero = False
    last_lag: Optional[int] = None
    used_kafka_fallback = False

    while not lag_zero and (time.time() - start_time) < max_timeout_in_sec:
        time.sleep(1)

        lag, group_found, api_available, auth_denied = _get_consumer_lag(
            gms_url, consumers, consumer_group
        )
        if auth_denied:
            auth.note_denied()
            continue
        auth.note_ok()
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
            logger.debug(
                "Messaging lag API unavailable this poll (gms_url=%s); retrying",
                gms_url,
            )
            continue

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
        mcp_only: If True, wait for MCP and MCL -- the proposal being consumed
            and the resulting change log being indexed. Skips the timeseries MCL
            consumer, so it is not sufficient for timeseries aspects.
        mae_only: If True, only wait for MCL versioned consumer (entity update tests)
        cdc_only: Ignored (CDC has no dedicated lag endpoint; included for compat)
        consumer_group: When set, wait only for this consumer group's lag (e.g.
            ``datahub-usage-event-consumer-job-client`` for audit-event indexing).
            Falls back to ``kafka-consumer-groups`` when the group is not exposed
            via the messaging lag API (Kafka usage-event consumer).
        auth_session: Ignored. Lag polls always use DATAHUB_GMS_TOKEN.
            Restricted-user sessions 403 VIEW_SYSTEM_STATUS / MANAGE_SYSTEM_OPERATIONS.
        legacy_wait: If True, use the old aggregate consumer-group-lag polling.
            Defaults to False, which instead captures offset checkpoints and
            waits for the consumers to pass them -- immune to the "lag never
            reaches zero under concurrent writers" problem that aggregate-lag
            polling has (see wait_for_offsets_to_be_consumed).
            Always uses the legacy path when consumer_group is set, since the
            offset-checkpoint path doesn't cover the usage-event consumer group,
            and when DATAHUB_TEST_FORCE_LEGACY_WAIT is set (CI retry attempts).
    """
    if env_vars.get_use_static_sleep():
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return

    # Lag polls require a token. Missing auth is not replaced by a sleep; the
    # ES refresh interval still runs after lag has been awaited (or the wait
    # times out).
    _lag_monitor_token()
    del auth_session

    auth = _LagAuthTracker()

    # CI sets this on retry attempts: a batch that failed once re-runs with the
    # more conservative wait, so a wait-related flake self-recovers.
    if env_vars.get_force_legacy_wait():
        legacy_wait = True

    consumers = _select_wait_consumers(consumer_group, mcp_only, mae_only)
    gms_url = _get_gms_url()

    # Usage events on Kafka are not exposed via trace readers; use broker CLI lag.
    if consumer_group == _USAGE_EVENT_CONSUMER_GROUP and _try_kafka_usage_event_wait(
        gms_url, consumer_group, max_timeout_in_sec, auth
    ):
        return

    # The offset-checkpoint path only covers mcp/mcl/mcl_timeseries; fall back
    # to legacy aggregate-lag polling for any consumer_group-scoped call, and
    # also if the checkpoint fetch itself fails (transient connection
    # error) rather than trusting a false-empty checkpoint.
    if (
        not legacy_wait
        and not consumer_group
        and _try_offset_checkpoint_wait(consumers, max_timeout_in_sec, auth)
    ):
        return

    _poll_aggregate_lag(gms_url, consumers, consumer_group, max_timeout_in_sec, auth)
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
) -> Optional[dict]:
    url = f"{gms_url}{endpoint}?skipCache=true&detailed=true"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=5)
        _raise_if_auth_denied(resp, endpoint)
        resp.raise_for_status()
        return resp.json()
    except LagAuthDeniedError:
        raise
    except Exception as e:
        logger.debug("Detailed lag fetch failed for %s: %s", endpoint, e)
        return None


def _fetch_detailed_partitions(
    gms_url: str,
    endpoint: str,
) -> Optional[dict]:
    """Returns {partition: (current_offset, lag)} from the detailed lag envelope,
    or None if the fetch itself failed (as opposed to a genuinely empty result).

    Assumes a single consumer group per topic (true for mcp/mcl/mcl_timeseries
    today); takes the first group found if there happen to be more.
    """
    data = _fetch_detailed_lag_envelope(gms_url, endpoint)
    if data is None:
        return None
    consumer_groups = data.get("consumerGroups", {})
    for _group_name, topics in consumer_groups.items():
        for _topic_name, topic_info in topics.items():
            partitions = topic_info.get("partitions") or {}
            result = {}
            for p, info in partitions.items():
                offset = info.get("offset")
                if offset is None:
                    # Treat a malformed partition entry like a failed fetch, so
                    # the caller falls back to the legacy wait rather than
                    # raising out of the middle of a sync.
                    logger.warning(
                        "Partition %s in %s has no offset; falling back to legacy wait",
                        p,
                        endpoint,
                    )
                    return None
                result[int(p)] = (offset, info.get("lag") or 0)
            return result
    return {}


def _capture_offset_targets(
    gms_url: str,
    consumer_types: List[str],
) -> Optional[dict]:
    """Capture the current log-end-offset per (consumer_type, partition).

    Returns None if any fetch failed, so callers can distinguish "nothing to
    wait for" from "couldn't look".
    """
    targets: dict = {}
    for consumer_type in consumer_types:
        endpoint = _MESSAGING_LAG_ENDPOINTS[consumer_type]
        partitions = _fetch_detailed_partitions(gms_url, endpoint)
        if partitions is None:
            logger.warning(
                "Could not establish an offset checkpoint for %s (lag endpoint "
                "unreachable); falling back to legacy wait",
                consumer_type,
            )
            return None
        for partition, (offset, lag) in partitions.items():
            targets[(consumer_type, partition)] = offset + lag
    return targets


def _await_offset_targets(
    gms_url: str,
    targets: dict,
    deadline: float,
    poll_interval_sec: float,
) -> dict:
    """Poll until every target offset has been passed, or the deadline hits.

    Returns the targets still outstanding (empty dict means fully consumed).
    """
    remaining = dict(targets)
    while remaining and time.time() < deadline:
        time.sleep(poll_interval_sec)
        for consumer_type in {c for (c, _p) in remaining}:
            endpoint = _MESSAGING_LAG_ENDPOINTS[consumer_type]
            partitions = _fetch_detailed_partitions(gms_url, endpoint)
            if partitions is None:
                continue
            for partition, (offset, _lag) in partitions.items():
                key = (consumer_type, partition)
                if key in remaining and offset >= remaining[key]:
                    del remaining[key]
    return remaining


def wait_for_offsets_to_be_consumed(
    consumers: List[str],
    max_timeout_in_sec: int = 60,
    poll_interval_sec: float = 0.25,
) -> bool:
    """Wait for consumer offsets to pass a checkpoint, instead of polling a
    continuously-refreshed aggregate lag (which never converges while other
    xdist workers keep writing).

    Runs in two phases, because an MCL does not necessarily exist yet when this
    is called:

      1. Wait for the MCP checkpoint. On the async ingest path (the Python REST
         sink's ASYNC/ASYNC_BATCH modes, i.e. ingest_file_via_rest), GMS's
         ingestProposalAsync produces *only* the MCP and returns -- no SQL write
         and no MCL. The MCL is produced later, by the mce-consumer.
      2. Only then capture the MCL checkpoint and wait for it. By this point the
         MCL is guaranteed to be in the topic: the mce-consumer joins the MCL
         send futures inside its listener (MCLEmitResult.isProduced() calls
         mclFuture.get(), evaluated by a terminal collect() in produceMCLAsync),
         so its own offset cannot advance until the MCL is broker-acked.

    Capturing both checkpoints up front instead would be wrong for that path:
    the MCL checkpoint would be the *pre-write* end-offset, already satisfied on
    the first tick, so the wait would return without ever covering indexing.
    Synchronous writes (GraphQL mutations, SYNC_PRIMARY emits) are unaffected --
    they produce and ack their MCL in-request, so phase 1 is a no-op for them
    and phase 2 sees their MCL immediately.

    Two carve-outs where phase 2 is not a guarantee: CDC mode
    (CDC_MCL_PROCESSING_ENABLED=true, default off) produces no MCL inline at all,
    and an MCL produce failure is swallowed by the consumer, which advances its
    offset with the SQL written but no MCL.

    Args:
        consumers: subset of "mcp", "mcl", "mcl_timeseries" to wait on.
        max_timeout_in_sec: safety ceiling across both phases.
        poll_interval_sec: how often to re-check (much finer than the 1s
            granularity of wait_for_writes_to_sync's poll loop, since each
            check is now a ~20-50ms HTTP call rather than a ~1.4s CLI spawn).

    Returns:
        True if checkpoints were established (whether or not every offset was
        consumed within the timeout). False if a checkpoint fetch itself
        failed (e.g. a transient connection error) -- the caller should
        fall back to the legacy lag-polling path rather than trust an empty
        checkpoint, which would otherwise return near-instantly as if there
        were nothing to wait for.
    """
    start_time = time.time()
    deadline = start_time + max_timeout_in_sec
    gms_url = _get_gms_url()

    mcp_consumers = [c for c in consumers if c == "mcp"]
    mcl_consumers = [c for c in consumers if c != "mcp"]

    total_targets = 0
    outstanding: dict = {}

    for phase_consumers in (mcp_consumers, mcl_consumers):
        if not phase_consumers:
            continue
        targets = _capture_offset_targets(gms_url, phase_consumers)
        if targets is None:
            return False
        total_targets += len(targets)
        outstanding.update(
            _await_offset_targets(gms_url, targets, deadline, poll_interval_sec)
        )

    elapsed = time.time() - start_time
    if outstanding:
        logger.warning(
            f"Timed out after {elapsed:.1f}s waiting for offsets to be consumed: {outstanding}"
        )
    else:
        logger.info(
            f"All {total_targets} target offset(s) consumed after {elapsed:.2f}s"
        )
    return True
