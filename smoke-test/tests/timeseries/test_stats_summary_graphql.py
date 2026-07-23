"""
Smoke test: Dashboard.statsSummary and Dataset.statsSummary via GraphQL.

Seeds timeseries aspects (3 old-window + 3 recent-window buckets per entity, plus one
absolute record per dashboard) for 3 dashboards and 3 datasets, then queries all entities
in a single multi-alias GraphQL request.

All writes are fired as a single batch — one Kafka lag wait at the end instead of one
per write — so total fixture setup time is ~7s regardless of datapoint count.

The batched and per-URN paths compute identical values, so the same exact-value assertions
verify parity under either setting — run with TIMESERIES_ASPECT_AGG_BATCH_LOAD_ENABLED
(dashboards) and DATASET_STATS_SUMMARY_BATCH_LOAD_ENABLED (datasets) set to true and false.

Each dataset is seeded with DISTINCT data (see _DATASET_FIXTURES) — a unique query count,
unique-user count, and top-user set — so a batch that mis-keys one dataset's summary onto
another fails a concrete equality assertion instead of slipping past a loose "> 0" check.

Time-window correctness is verified with out-of-window records that must be filtered out:
  OLD_USER    — days -40, -37, -34 (outside the 30-day window)
  RECENT_USER / per-dataset users — days -20, -10, -3 (inside the 30-day window)
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

# Per-dataset DISTINCT fixtures so a mis-keyed batch (one dataset's summary returned for another)
# fails an exact equality assertion. The seeded values map to the summary as:
#   queryCountLast30Days      = sql_queries_per_recent_day * len(_RECENT_DAYS)  (per-day LATEST, summed)
#   uniqueUserCountLast30Days = len(recent_user_counts_per_day)
#   topUsersLast30Days        = users ordered by summed count desc, then urn asc
# (No ties here — tie-break ordering is covered by the unit test DatasetStatsSummaryBatchLoaderTest.)
_DATASET_FIXTURES: List[Dict[str, Any]] = [
    {
        "urn": _DATASET_URNS[0],
        "sql_queries_per_recent_day": 2,  # -> queryCountLast30Days == 6
        "recent_user_counts_per_day": {"urn:li:corpuser:statsSummaryDs0UserA": 1},
    },
    {
        "urn": _DATASET_URNS[1],
        "sql_queries_per_recent_day": 5,  # -> 15
        "recent_user_counts_per_day": {
            "urn:li:corpuser:statsSummaryDs1UserA": 2,
            "urn:li:corpuser:statsSummaryDs1UserB": 1,
        },
    },
    {
        "urn": _DATASET_URNS[2],
        "sql_queries_per_recent_day": 10,  # -> 30
        "recent_user_counts_per_day": {
            "urn:li:corpuser:statsSummaryDs2UserA": 3,
            "urn:li:corpuser:statsSummaryDs2UserB": 2,
            "urn:li:corpuser:statsSummaryDs2UserC": 1,
        },
    },
]

# totalSqlQueries on the out-of-window dataset records; deliberately large so that if the 30-day
# window filter regressed, the summed query count would jump far above the expected in-window total.
_OLD_WINDOW_SQL_QUERIES = 100

# A dataset whose ONLY usage predates the 30-day window. Its 30-day summary must come back empty
# (present, but every stat field falsy) — exercising an empty slice assembled inside a batch
# alongside populated datasets, and confirming the window filter can exclude an entity entirely.
_OUT_OF_WINDOW_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:smokeTest,statsSummaryOutOfWindow,PROD)"
)


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


def _dataset_day_payload(
    urn: str, day_offset: int, total_sql_queries: int, user_counts: Dict[str, int]
) -> Tuple[str, Dict]:
    # One record per (dataset, day) carrying the whole day's userCounts array — keeping a single
    # record per day means the per-day LATEST totalSqlQueries is unambiguous and avoids same-day
    # timeseries records colliding on their document id.
    url = f"{{GMS_URL}}/openapi/v3/entity/dataset/{_encode(urn)}/datasetUsageStatistics"
    payload = {
        "value": {
            "timestampMillis": get_timestampmillis_at_start_of_day(day_offset),
            "eventGranularity": {"unit": "DAY", "multiple": 1},
            "totalSqlQueries": total_sql_queries,
            "uniqueUserCount": len(user_counts),
            "userCounts": [
                {"user": user, "count": count} for user, count in user_counts.items()
            ],
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

    for fixture in _DATASET_FIXTURES:
        urn = fixture["urn"]
        # In-window days: this dataset's distinct query count and top users.
        for day in _RECENT_DAYS:
            url, payload = _dataset_day_payload(
                urn,
                day,
                fixture["sql_queries_per_recent_day"],
                fixture["recent_user_counts_per_day"],
            )
            items.append((url.replace("{GMS_URL}", gms_url), payload))
        # Out-of-window days: OLD_USER + an inflated query count, all of which must be filtered out.
        for day in _OLD_DAYS:
            url, payload = _dataset_day_payload(
                urn, day, _OLD_WINDOW_SQL_QUERIES, {_OLD_USER_URN: 50}
            )
            items.append((url.replace("{GMS_URL}", gms_url), payload))

    # A dataset whose only usage is out of window — entity exists (seeded here) but its 30-day
    # summary must be empty even when queried in the same batch as populated datasets.
    for day in _OLD_DAYS:
        url, payload = _dataset_day_payload(
            _OUT_OF_WINDOW_DATASET_URN,
            day,
            _OLD_WINDOW_SQL_QUERIES,
            {_OLD_USER_URN: 50},
        )
        items.append((url.replace("{GMS_URL}", gms_url), payload))

    return items


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session: Any):
    """
    Ingests 42 records as a single batch (one Kafka lag wait):
      3 dashboards            × (1 absolute + 3 old + 3 recent) = 21
      3 datasets              × (3 recent + 3 old)              = 18
      1 out-of-window dataset × 3 old                           = 3
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
    for urn in _DATASET_URNS + [_OUT_OF_WINDOW_DATASET_URN]:
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

# Two-alias variant used by the out-of-window and duplicate-URN batching tests.
_DATASET_PAIR_QUERY = """
query GetTwoDatasetStatsSummary($urn0: String!, $urn1: String!) {
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
}
"""


def _top_user_urns(summary: Dict[str, Any]) -> List[str]:
    return [u["urn"] for u in (summary.get("topUsersLast30Days") or [])]


def _expected_query_count(fixture: Dict[str, Any]) -> int:
    return fixture["sql_queries_per_recent_day"] * len(_RECENT_DAYS)


def _expected_top_users(fixture: Dict[str, Any]) -> List[str]:
    # Summed count = per-day count * len(_RECENT_DAYS); order by count desc, then urn asc — the same
    # ordering the loader applies. Multiplying by a constant preserves order, so per-day count works.
    counts: Dict[str, int] = fixture["recent_user_counts_per_day"]
    return sorted(counts, key=lambda user: (-counts[user], user))


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
    Queries 3 datasets in a single multi-alias request (one GraphQL request → the DataLoader
    batches all three into two aggregations). Each dataset carries DISTINCT data, so the exact
    equality assertions fail if the batch mis-keys one dataset's summary onto another, or if
    per-day query counts / user counts are mis-aggregated. Out-of-window (OLD_USER, inflated
    query count) records must be excluded by the 30-day window.
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
        for key, fixture in zip(("d0", "d1", "d2"), _DATASET_FIXTURES, strict=True):
            summary = data[key]["statsSummary"]
            assert summary is not None, f"{key}: statsSummary is null"

            expected_query_count = _expected_query_count(fixture)
            assert summary.get("queryCountLast30Days") == expected_query_count, (
                f"{key}: expected queryCountLast30Days == {expected_query_count}, "
                f"got {summary.get('queryCountLast30Days')}"
            )

            expected_unique_users = len(fixture["recent_user_counts_per_day"])
            assert summary.get("uniqueUserCountLast30Days") == expected_unique_users, (
                f"{key}: expected uniqueUserCountLast30Days == {expected_unique_users}, "
                f"got {summary.get('uniqueUserCountLast30Days')}"
            )

            expected_top_users = _expected_top_users(fixture)
            top_urns = _top_user_urns(summary)
            assert top_urns == expected_top_users, (
                f"{key}: expected topUsersLast30Days {expected_top_users}, got {top_urns}"
            )
            assert _OLD_USER_URN not in top_urns, (
                f"{key}: {_OLD_USER_URN} (>30 days ago) must not appear in topUsersLast30Days"
            )

    assert_dataset_summaries()


def test_dataset_stats_summary_out_of_window_only(auth_session: Any) -> None:
    """
    Corner case + batching flow: a dataset whose only usage predates the 30-day window is queried
    in the SAME batched request as a populated dataset. The populated one must resolve to its exact
    values while the out-of-window one resolves to a present-but-empty summary — proving the batch
    assembles an empty slice correctly (no neighbour bleed) and the window filter can exclude an
    entity entirely.
    """
    populated = _DATASET_FIXTURES[0]
    variables: Dict[str, Any] = {
        "urn0": populated["urn"],
        "urn1": _OUT_OF_WINDOW_DATASET_URN,
    }

    @with_test_retry()
    def assert_summaries() -> None:
        res = execute_graphql(auth_session, _DATASET_PAIR_QUERY, variables)
        data = res["data"]

        populated_summary = data["d0"]["statsSummary"]
        assert populated_summary is not None, "d0: statsSummary is null"
        expected_query_count = _expected_query_count(populated)
        assert populated_summary.get("queryCountLast30Days") == expected_query_count, (
            f"d0: expected queryCountLast30Days == {expected_query_count}, "
            f"got {populated_summary.get('queryCountLast30Days')}"
        )

        # Present but empty: every stat field falsy (null on the batch path; null/0/[] per-URN).
        empty_summary = data["d1"]["statsSummary"]
        assert empty_summary is not None, "d1: statsSummary should be present, not null"
        assert not empty_summary.get("queryCountLast30Days"), (
            f"d1: out-of-window dataset must have no query count, "
            f"got {empty_summary.get('queryCountLast30Days')}"
        )
        assert not empty_summary.get("uniqueUserCountLast30Days"), (
            f"d1: out-of-window dataset must have no unique users, "
            f"got {empty_summary.get('uniqueUserCountLast30Days')}"
        )
        assert not _top_user_urns(empty_summary), (
            f"d1: out-of-window dataset must have no top users, "
            f"got {_top_user_urns(empty_summary)}"
        )

    assert_summaries()


def test_dataset_stats_summary_duplicate_urn_in_request(auth_session: Any) -> None:
    """
    Batching flow corner case: the same dataset URN requested twice in one GraphQL request (two
    aliases) must resolve both aliases to the same correct summary — the DataLoader de-duplicates
    the key and fans the single batched result back to both fields.
    """
    fixture = _DATASET_FIXTURES[0]
    variables: Dict[str, Any] = {
        "urn0": fixture["urn"],
        "urn1": fixture["urn"],
    }

    @with_test_retry()
    def assert_summaries() -> None:
        res = execute_graphql(auth_session, _DATASET_PAIR_QUERY, variables)
        data = res["data"]

        expected_query_count = _expected_query_count(fixture)
        expected_top_users = _expected_top_users(fixture)
        for key in ("d0", "d1"):
            summary = data[key]["statsSummary"]
            assert summary is not None, f"{key}: statsSummary is null"
            assert summary.get("queryCountLast30Days") == expected_query_count, (
                f"{key}: expected queryCountLast30Days == {expected_query_count}, "
                f"got {summary.get('queryCountLast30Days')}"
            )
            assert _top_user_urns(summary) == expected_top_users, (
                f"{key}: expected topUsersLast30Days {expected_top_users}, "
                f"got {_top_user_urns(summary)}"
            )

    assert_summaries()
