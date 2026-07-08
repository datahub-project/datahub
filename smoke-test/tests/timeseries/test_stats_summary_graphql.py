"""
Smoke test: Dashboard.statsSummary and Dataset.statsSummary via GraphQL.

Seeds timeseries aspects (3 old-window + 3 recent-window buckets per entity, plus one
absolute record per dashboard) for 3 dashboards and 3 datasets, then queries all entities
in a single multi-alias GraphQL request.

All writes are fired as a single batch — one Kafka lag wait at the end instead of one
per write — so total fixture setup time is ~7s regardless of datapoint count.

The test is agnostic to whether the batch aggregation path is active — run it with
TIMESERIES_ASPECT_AGG_BATCH_LOAD_ENABLED=true and =false to verify parity.

Time-window correctness is verified by using two distinct user URNs:
  OLD_USER    — days -40, -37, -34 (outside the 30-day window)
  RECENT_USER — days -20, -10, -3  (inside the 30-day window)
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

# ---------------------------------------------------------------------------
# Test entities
# ---------------------------------------------------------------------------

_DASHBOARD_URNS = [f"urn:li:dashboard:(looker,statsSummaryTest{i})" for i in range(3)]
_DATASET_URNS = [
    f"urn:li:dataset:(urn:li:dataPlatform:smokeTest,statsSummaryTest{i},PROD)"
    for i in range(3)
]

_OLD_USER_URN = "urn:li:corpuser:statsSummaryTestUserOld"
_RECENT_USER_URN = "urn:li:corpuser:statsSummaryTestUserRecent"

# Three days clearly outside the 30-day window
_OLD_DAYS = [-40, -37, -34]
# Three days clearly inside the 30-day window
_RECENT_DAYS = [-20, -10, -3]


def _encode(urn: str) -> str:
    return quote(urn, safe="")


# ---------------------------------------------------------------------------
# Batch ingest helper
# All requests are fired using raw HTTP (bypassing TestSessionWrapper's per-call
# Kafka lag wait), then one wait_for_writes_to_sync() is called at the end.
# ---------------------------------------------------------------------------


def _batch_post(
    gms_url: str,
    token: str,
    items: List[Tuple[str, Dict]],
) -> None:
    """Fire all POSTs in one burst, incurring a single Kafka lag wait for the batch."""
    auth_header = {"Authorization": f"Bearer {token}"}
    params = {"createIfNotExists": "false", "async": "false"}
    for url, payload in items:
        resp = _requests.post(url, params=params, json=payload, headers=auth_header)
        assert resp.status_code in (200, 201, 202), (
            f"Ingest failed {url}: {resp.status_code} {resp.text}"
        )
    wait_for_writes_to_sync()


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------


def _dashboard_absolute_payload(urn: str) -> Tuple[str, Dict]:
    url = f"{{GMS_URL}}/openapi/v3/entity/dashboard/{_encode(urn)}/dashboardUsageStatistics"
    payload = {
        "value": {
            "timestampMillis": get_timestampmillis_at_start_of_day(0),
            "viewsCount": 100,
            "favoritesCount": 5,
        }
    }
    return url, payload


def _dashboard_bucket_payload(
    urn: str, day_offset: int, user_urn: str
) -> Tuple[str, Dict]:
    url = f"{{GMS_URL}}/openapi/v3/entity/dashboard/{_encode(urn)}/dashboardUsageStatistics"
    payload = {
        "value": {
            "timestampMillis": get_timestampmillis_at_start_of_day(day_offset),
            "eventGranularity": {"unit": "DAY", "multiple": 1},
            "userCounts": [{"user": user_urn, "usageCount": 3}],
        }
    }
    return url, payload


def _dataset_bucket_payload(
    urn: str, day_offset: int, user_urn: str
) -> Tuple[str, Dict]:
    url = f"{{GMS_URL}}/openapi/v3/entity/dataset/{_encode(urn)}/datasetUsageStatistics"
    payload = {
        "value": {
            "timestampMillis": get_timestampmillis_at_start_of_day(day_offset),
            "eventGranularity": {"unit": "DAY", "multiple": 1},
            "totalSqlQueries": 2,
            "uniqueUserCount": 1,
            "userCounts": [{"user": user_urn, "count": 1}],
        }
    }
    return url, payload


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


def _build_ingest_batch(gms_url: str) -> List[Tuple[str, Dict]]:
    """Build all (url, payload) pairs — substitute GMS_URL at call time."""
    items: List[Tuple[str, Dict]] = []

    for urn in _DASHBOARD_URNS:
        url, payload = _dashboard_absolute_payload(urn)
        items.append((url.replace("{GMS_URL}", gms_url), payload))
        for day in _OLD_DAYS:
            url, payload = _dashboard_bucket_payload(urn, day, _OLD_USER_URN)
            items.append((url.replace("{GMS_URL}", gms_url), payload))
        for day in _RECENT_DAYS:
            url, payload = _dashboard_bucket_payload(urn, day, _RECENT_USER_URN)
            items.append((url.replace("{GMS_URL}", gms_url), payload))

    for urn in _DATASET_URNS:
        for day in _OLD_DAYS:
            url, payload = _dataset_bucket_payload(urn, day, _OLD_USER_URN)
            items.append((url.replace("{GMS_URL}", gms_url), payload))
        for day in _RECENT_DAYS:
            url, payload = _dataset_bucket_payload(urn, day, _RECENT_USER_URN)
            items.append((url.replace("{GMS_URL}", gms_url), payload))

    return items


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session: Any):
    """
    Ingests 39 records as a single batch (one Kafka lag wait):
      3 dashboards × (1 absolute + 3 old + 3 recent) = 21
      3 datasets  × (3 old + 3 recent) = 18
    """
    gms_url = auth_session.gms_url()
    token = auth_session.gms_token()
    items = _build_ingest_batch(gms_url)
    logger.info(
        "Batch-ingesting %d timeseries records for statsSummary test", len(items)
    )
    _batch_post(gms_url, token, items)
    logger.info("Batch ingest complete, waiting for indexing to settle")
    time.sleep(3)
    yield

    logger.info("Cleaning up statsSummary test entities")
    auth_header = {"Authorization": f"Bearer {token}"}
    for urn in _DASHBOARD_URNS:
        _requests.delete(
            f"{gms_url}/openapi/v3/entity/dashboard/{_encode(urn)}",
            headers=auth_header,
        )
    for urn in _DATASET_URNS:
        _requests.delete(
            f"{gms_url}/openapi/v3/entity/dataset/{_encode(urn)}",
            headers=auth_header,
        )


# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

_DASHBOARD_QUERY = """
query GetDashboardStatsSummary($urn0: String!, $urn1: String!, $urn2: String!) {
    d0: dashboard(urn: $urn0) {
        statsSummary {
            viewCount
            uniqueUserCountLast30Days
            topUsersLast30Days { urn }
        }
    }
    d1: dashboard(urn: $urn1) {
        statsSummary {
            viewCount
            uniqueUserCountLast30Days
            topUsersLast30Days { urn }
        }
    }
    d2: dashboard(urn: $urn2) {
        statsSummary {
            viewCount
            uniqueUserCountLast30Days
            topUsersLast30Days { urn }
        }
    }
}
"""

_DATASET_QUERY = """
query GetDatasetStatsSummary($urn0: String!, $urn1: String!, $urn2: String!) {
    d0: dataset(urn: $urn0) {
        statsSummary {
            queryCountLast30Days
            uniqueUserCountLast30Days
            topUsersLast30Days { urn }
        }
    }
    d1: dataset(urn: $urn1) {
        statsSummary {
            queryCountLast30Days
            uniqueUserCountLast30Days
            topUsersLast30Days { urn }
        }
    }
    d2: dataset(urn: $urn2) {
        statsSummary {
            queryCountLast30Days
            uniqueUserCountLast30Days
            topUsersLast30Days { urn }
        }
    }
}
"""


def _top_user_urns(summary: Dict[str, Any]) -> List[str]:
    return [u["urn"] for u in (summary.get("topUsersLast30Days") or [])]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_dashboard_stats_summary(auth_session: Any) -> None:
    """
    Queries 3 dashboards in a single multi-alias request (exercises the DataLoader
    batch path when timeseriesAspectAggBatchLoadEnabled=true, falls back to per-URN
    when false). Asserts time-window correctness: only RECENT_USER appears in top users.
    """
    variables: Dict[str, Any] = {
        "urn0": _DASHBOARD_URNS[0],
        "urn1": _DASHBOARD_URNS[1],
        "urn2": _DASHBOARD_URNS[2],
    }

    @with_test_retry()
    def assert_dashboard_summaries() -> None:
        res = execute_graphql(auth_session, _DASHBOARD_QUERY, variables)
        data = res["data"]
        for key in ("d0", "d1", "d2"):
            summary = data[key]["statsSummary"]
            assert summary is not None, f"{key}: statsSummary is null"

            assert (summary.get("viewCount") or 0) > 0, (
                f"{key}: expected viewCount > 0, got {summary.get('viewCount')}"
            )
            assert (summary.get("uniqueUserCountLast30Days") or 0) > 0, (
                f"{key}: expected uniqueUserCountLast30Days > 0"
            )

            top_urns = _top_user_urns(summary)
            assert len(top_urns) > 0, f"{key}: topUsersLast30Days is empty"
            assert _RECENT_USER_URN in top_urns, (
                f"{key}: expected {_RECENT_USER_URN} in topUsersLast30Days, got {top_urns}"
            )
            assert _OLD_USER_URN not in top_urns, (
                f"{key}: {_OLD_USER_URN} (>30 days ago) must not appear in topUsersLast30Days"
            )

    assert_dashboard_summaries()


def test_dataset_stats_summary(auth_session: Any) -> None:
    """
    Queries 3 datasets in a single multi-alias request. Asserts time-window correctness:
    only RECENT_USER appears in top users for the last-30-days window.
    """
    variables: Dict[str, Any] = {
        "urn0": _DATASET_URNS[0],
        "urn1": _DATASET_URNS[1],
        "urn2": _DATASET_URNS[2],
    }

    @with_test_retry()
    def assert_dataset_summaries() -> None:
        res = execute_graphql(auth_session, _DATASET_QUERY, variables)
        data = res["data"]
        for key in ("d0", "d1", "d2"):
            summary = data[key]["statsSummary"]
            assert summary is not None, f"{key}: statsSummary is null"

            assert (summary.get("queryCountLast30Days") or 0) > 0, (
                f"{key}: expected queryCountLast30Days > 0, got {summary.get('queryCountLast30Days')}"
            )
            assert (summary.get("uniqueUserCountLast30Days") or 0) > 0, (
                f"{key}: expected uniqueUserCountLast30Days > 0"
            )

            top_urns = _top_user_urns(summary)
            assert len(top_urns) > 0, f"{key}: topUsersLast30Days is empty"
            assert _RECENT_USER_URN in top_urns, (
                f"{key}: expected {_RECENT_USER_URN} in topUsersLast30Days, got {top_urns}"
            )
            assert _OLD_USER_URN not in top_urns, (
                f"{key}: {_OLD_USER_URN} (>30 days ago) must not appear in topUsersLast30Days"
            )

    assert_dataset_summaries()
