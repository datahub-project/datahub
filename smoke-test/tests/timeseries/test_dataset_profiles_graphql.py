"""
Smoke test: fetch datasetProfiles via GraphQL for multiple datasets.

Ingests datasetProfile timeseries aspects for several datasets via the OpenAPI
v3 endpoint, then queries them together via a single GraphQL request. The test
validates correctness of the returned profiles regardless of which internal
batch path is active.
"""

import logging
import time
from typing import Any, Dict
from urllib.parse import quote

import pytest

from tests.utils import (
    execute_graphql,
    get_timestampmillis_at_start_of_day,
    with_test_retry,
)

logger = logging.getLogger(__name__)

_DATASET_URNS = [
    f"urn:li:dataset:(urn:li:dataPlatform:test,profilesGraphqlDataset{i},PROD)"
    for i in range(3)
]

# Ingest profiles for the last 3 days; latest (day 0) is the one asserted.
_DAYS = 3
_ROW_COUNT_BASE = 100  # day 0 gets _DAYS * _ROW_COUNT_BASE


def _encode_urn(urn: str) -> str:
    return quote(urn, safe="")


def _ingest_profile(auth_session: Any, urn: str, day_offset: int) -> None:
    timestamp = get_timestampmillis_at_start_of_day(day_offset)
    row_count = (_DAYS + day_offset) * _ROW_COUNT_BASE  # day 0 → _DAYS * base
    url = f"{auth_session.gms_url()}/openapi/v3/entity/dataset/{_encode_urn(urn)}/datasetProfile"
    payload = {
        "value": {
            "timestampMillis": timestamp,
            "messageId": f"{urn}-day{day_offset}",
            "rowCount": row_count,
            "columnCount": 5,
        }
    }
    resp = auth_session.post(
        url, params={"createIfNotExists": "false", "async": "false"}, json=payload
    )
    assert resp.status_code in (200, 201, 202), (
        f"Failed to ingest profile for {urn}: {resp.text}"
    )


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session: Any):
    logger.info("Ingesting datasetProfile aspects for %d datasets", len(_DATASET_URNS))
    for urn in _DATASET_URNS:
        for day in range(-(_DAYS - 1), 1):  # e.g. -2, -1, 0
            _ingest_profile(auth_session, urn, day)
    # Allow async indexing to settle
    time.sleep(3)
    yield
    logger.info("Cleaning up datasetProfile test datasets")
    for urn in _DATASET_URNS:
        auth_session.delete(
            f"{auth_session.gms_url()}/openapi/v3/entity/dataset/{_encode_urn(urn)}"
        )


_QUERY = """
query GetMultiDatasetProfiles($urn0: String!, $urn1: String!, $urn2: String!) {
    d0: dataset(urn: $urn0) { datasetProfiles(limit: 1) { rowCount timestampMillis } }
    d1: dataset(urn: $urn1) { datasetProfiles(limit: 1) { rowCount timestampMillis } }
    d2: dataset(urn: $urn2) { datasetProfiles(limit: 1) { rowCount timestampMillis } }
}
"""


def test_dataset_profiles_graphql(auth_session: Any) -> None:
    variables: Dict[str, Any] = {
        "urn0": _DATASET_URNS[0],
        "urn1": _DATASET_URNS[1],
        "urn2": _DATASET_URNS[2],
    }
    expected_row_count = _DAYS * _ROW_COUNT_BASE  # latest day (offset 0)
    expected_timestamp = get_timestampmillis_at_start_of_day(0)

    @with_test_retry()
    def assert_profiles() -> None:
        res = execute_graphql(auth_session, _QUERY, variables)
        data = res["data"]
        for key in ("d0", "d1", "d2"):
            profiles = data[key]["datasetProfiles"]
            assert len(profiles) == 1, f"{key}: expected 1 profile, got {len(profiles)}"
            assert profiles[0]["rowCount"] == expected_row_count, (
                f"{key}: rowCount mismatch (got {profiles[0]['rowCount']})"
            )
            assert profiles[0]["timestampMillis"] == expected_timestamp, (
                f"{key}: timestampMillis mismatch"
            )

    assert_profiles()
