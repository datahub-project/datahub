"""
Smoke test: Dashboard.usageStats (daily buckets + rolled-up aggregations) via GraphQL.

Seeds daily dashboardUsageStatistics for 3 dashboards (plus out-of-window records that must be
excluded), then queries all three in a single multi-alias request so the DataLoader batches them
into one batchGetAggregatedStats when timeseriesAspectAggBatchLoadEnabled=true, and falls back to
per-URN getAggregatedStats when false.

Each dashboard is seeded with DISTINCT values (unique view/execution totals and unique-user count),
so a batch that mis-keys one dashboard's result onto another fails an exact equality assertion
instead of slipping past a loose "> 0" check. The batched and per-URN paths compute identical
values, so the same assertions verify parity under either flag state.

A separate single-dashboard test exercises the loader's single-URN guard (which deliberately skips
the batch path) end-to-end.
"""

import logging
import time
from typing import Any, Dict, List, Tuple
from urllib.parse import quote

import pytest
import requests as _requests

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import (
    execute_graphql,
    get_timestampmillis_at_start_of_day,
    with_test_retry,
)

logger = logging.getLogger(__name__)

_DASHBOARD_URNS = [f"urn:li:dashboard:(looker,usageStatsTest{i})" for i in range(3)]
_OLD_USER_URN = "urn:li:corpuser:usageStatsTestOldUser"

# In-window days (covered by the query window below) and out-of-window days (must be excluded).
_RECENT_DAYS = [-20, -10, -3]
_OLD_DAYS = [-60, -55, -50]

# Query window: [start_of_day(-40), start_of_day(+1)) — includes the recent days, excludes the old.
_WINDOW_START_OFFSET = -40
_WINDOW_END_OFFSET = 1

# Inflated views/executions on the out-of-window records: if the window filter regressed, the
# rolled-up totals (and bucket count) would jump far above the expected in-window values.
_OLD_VIEWS = 1000
_OLD_EXECUTIONS = 1000

# Per-dashboard DISTINCT fixtures. Per-day values are seeded on each of _RECENT_DAYS, so:
#   aggregations.viewsCount      = views_per_recent_day * len(_RECENT_DAYS)
#   aggregations.executionsCount = executions_per_recent_day * len(_RECENT_DAYS)
#   aggregations.uniqueUserCount = len(recent_users)
#   len(buckets)                 = len(_RECENT_DAYS)   (one daily bucket per in-window day)
_DASHBOARD_FIXTURES: List[Dict[str, Any]] = [
    {
        "urn": _DASHBOARD_URNS[0],
        "views_per_recent_day": 10,
        "executions_per_recent_day": 2,
        "recent_users": ["urn:li:corpuser:usageStatsDs0UserA"],
    },
    {
        "urn": _DASHBOARD_URNS[1],
        "views_per_recent_day": 20,
        "executions_per_recent_day": 5,
        "recent_users": [
            "urn:li:corpuser:usageStatsDs1UserA",
            "urn:li:corpuser:usageStatsDs1UserB",
        ],
    },
    {
        "urn": _DASHBOARD_URNS[2],
        "views_per_recent_day": 30,
        "executions_per_recent_day": 9,
        "recent_users": [
            "urn:li:corpuser:usageStatsDs2UserA",
            "urn:li:corpuser:usageStatsDs2UserB",
            "urn:li:corpuser:usageStatsDs2UserC",
        ],
    },
]


def _encode(urn: str) -> str:
    return quote(urn, safe="")


def _batch_post(gms_url: str, token: str, items: List[Tuple[str, Dict]]) -> None:
    """Fire all POSTs in one burst, incurring a single Kafka lag wait for the batch."""
    auth_header = {"Authorization": f"Bearer {token}"}
    params = {"createIfNotExists": "false", "async": "false"}
    for url, payload in items:
        resp = _requests.post(url, params=params, json=payload, headers=auth_header)
        assert resp.status_code in (200, 201, 202), (
            f"Ingest failed {url}: {resp.status_code} {resp.text}"
        )
    wait_for_writes_to_sync()


def _day_payload(
    urn: str, day_offset: int, views: int, executions: int, users: List[str]
) -> Tuple[str, Dict]:
    # One record per (dashboard, day) with the whole day's userCounts array — a single record per
    # day keeps the per-day SUM unambiguous and avoids same-day timeseries doc-id collisions.
    url = f"{{GMS_URL}}/openapi/v3/entity/dashboard/{_encode(urn)}/dashboardUsageStatistics"
    payload = {
        "value": {
            "timestampMillis": get_timestampmillis_at_start_of_day(day_offset),
            "eventGranularity": {"unit": "DAY", "multiple": 1},
            "viewsCount": views,
            "executionsCount": executions,
            "uniqueUserCount": len(users),
            "userCounts": [{"user": u, "usageCount": 1} for u in users],
        }
    }
    return url, payload


def _build_ingest_batch(gms_url: str) -> List[Tuple[str, Dict]]:
    items: List[Tuple[str, Dict]] = []
    for fixture in _DASHBOARD_FIXTURES:
        urn = fixture["urn"]
        for day in _RECENT_DAYS:
            url, payload = _day_payload(
                urn,
                day,
                fixture["views_per_recent_day"],
                fixture["executions_per_recent_day"],
                fixture["recent_users"],
            )
            items.append((url.replace("{GMS_URL}", gms_url), payload))
        # Out-of-window records: inflated views + a distinct old user, all of which must be excluded.
        for day in _OLD_DAYS:
            url, payload = _day_payload(
                urn, day, _OLD_VIEWS, _OLD_EXECUTIONS, [_OLD_USER_URN]
            )
            items.append((url.replace("{GMS_URL}", gms_url), payload))
    return items


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session: Any):
    """Ingests 3 dashboards × (3 recent + 3 old) = 18 records as a single batch."""
    gms_url = auth_session.gms_url()
    token = auth_session.gms_token()
    items = _build_ingest_batch(gms_url)
    logger.info("Batch-ingesting %d dashboardUsageStatistics records", len(items))
    _batch_post(gms_url, token, items)
    logger.info("Batch ingest complete, waiting for indexing to settle")
    time.sleep(3)
    yield

    logger.info("Cleaning up dashboard usageStats test entities")
    auth_header = {"Authorization": f"Bearer {token}"}
    for urn in _DASHBOARD_URNS:
        _requests.delete(
            f"{gms_url}/openapi/v3/entity/dashboard/{_encode(urn)}",
            headers=auth_header,
        )


_USAGE_FIELDS = """
        usageStats(startTimeMillis: $start, endTimeMillis: $end) {
            buckets { bucket }
            aggregations { uniqueUserCount viewsCount executionsCount }
        }
"""

_MULTI_QUERY = (
    """
query GetDashboardUsageStats($urn0: String!, $urn1: String!, $urn2: String!, $start: Long!, $end: Long!) {
    d0: dashboard(urn: $urn0) {"""
    + _USAGE_FIELDS
    + """    }
    d1: dashboard(urn: $urn1) {"""
    + _USAGE_FIELDS
    + """    }
    d2: dashboard(urn: $urn2) {"""
    + _USAGE_FIELDS
    + """    }
}
"""
)

_SINGLE_QUERY = (
    """
query GetOneDashboardUsageStats($urn0: String!, $start: Long!, $end: Long!) {
    d0: dashboard(urn: $urn0) {"""
    + _USAGE_FIELDS
    + """    }
}
"""
)


def _window() -> Dict[str, int]:
    return {
        "start": get_timestampmillis_at_start_of_day(_WINDOW_START_OFFSET),
        "end": get_timestampmillis_at_start_of_day(_WINDOW_END_OFFSET),
    }


def _assert_dashboard(
    key: str, fixture: Dict[str, Any], summary: Dict[str, Any]
) -> None:
    assert summary is not None, f"{key}: usageStats is null"

    expected_views = fixture["views_per_recent_day"] * len(_RECENT_DAYS)
    expected_executions = fixture["executions_per_recent_day"] * len(_RECENT_DAYS)
    expected_unique_users = len(fixture["recent_users"])

    agg = summary["aggregations"]
    assert agg is not None, f"{key}: aggregations is null"
    assert agg.get("viewsCount") == expected_views, (
        f"{key}: expected viewsCount == {expected_views}, got {agg.get('viewsCount')} "
        f"(a wrong value means out-of-window records leaked or the batch mis-keyed)"
    )
    assert agg.get("executionsCount") == expected_executions, (
        f"{key}: expected executionsCount == {expected_executions}, got {agg.get('executionsCount')}"
    )
    assert agg.get("uniqueUserCount") == expected_unique_users, (
        f"{key}: expected uniqueUserCount == {expected_unique_users}, "
        f"got {agg.get('uniqueUserCount')}"
    )

    # The daily date-histogram gap-fills empty days and aligns buckets to UTC midnight, so neither
    # the bucket count nor exact keys are stable. Assert buckets exist and all fall within the
    # queried window — the precise out-of-window exclusion is already proven by the exact totals
    # above (leaked old records would have inflated viewsCount/executionsCount well past 30/60/90).
    window = _window()
    bucket_ts = [b["bucket"] for b in (summary.get("buckets") or [])]
    assert bucket_ts, f"{key}: expected usage buckets, got none"
    assert all(window["start"] <= t <= window["end"] for t in bucket_ts), (
        f"{key}: found buckets outside the queried window {window}: {bucket_ts}"
    )


def test_dashboard_usage_stats_batched(auth_session: Any) -> None:
    """
    Queries 3 dashboards in one request (>1 URN sharing a window → the DataLoader batches them into
    one batchGetAggregatedStats). Distinct per-dashboard data + exact assertions catch mis-keying
    or mis-aggregation; the inflated out-of-window records must be excluded.
    """
    variables: Dict[str, Any] = {
        "urn0": _DASHBOARD_URNS[0],
        "urn1": _DASHBOARD_URNS[1],
        "urn2": _DASHBOARD_URNS[2],
        **_window(),
    }

    @with_test_retry()
    def assert_summaries() -> None:
        data = execute_graphql(auth_session, _MULTI_QUERY, variables)["data"]
        for key, fixture in zip(("d0", "d1", "d2"), _DASHBOARD_FIXTURES, strict=True):
            _assert_dashboard(key, fixture, data[key]["usageStats"])

    assert_summaries()


def test_dashboard_usage_stats_single_urn(auth_session: Any) -> None:
    """
    A single dashboard exercises the loader's single-URN guard (which skips the batch path). The
    result must be identical to the batched path — same exact aggregations for dashboard 0.
    """
    fixture = _DASHBOARD_FIXTURES[0]
    variables: Dict[str, Any] = {"urn0": fixture["urn"], **_window()}

    @with_test_retry()
    def assert_summary() -> None:
        data = execute_graphql(auth_session, _SINGLE_QUERY, variables)["data"]
        _assert_dashboard("d0", fixture, data["d0"]["usageStats"])

    assert_summary()
