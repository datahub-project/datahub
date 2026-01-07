"""
Tests for OpenAPI v2 timeseries scroll pagination.

This test verifies that:
1. Multiple timeseries aspects can be ingested
2. The scroll API returns scrollId when there are more results
3. Pagination works correctly to retrieve all results
4. No duplicates occur during pagination
"""

import logging
import time
from typing import Any, Dict, List, Optional, Set

import pytest

logger = logging.getLogger(__name__)

# Test configuration
TEST_ENTITY_NAME = "dataset"
TEST_ASPECT_NAME = "datasetProfile"
TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:test,timeseriesScrollTest,PROD)"
NUM_PROFILES = 5  # Create 5 profiles to test pagination
PAGE_SIZE = 2  # Small page size to force multiple pages


def create_dataset_profile(
    timestamp_millis: int, row_count: int, message_id: str
) -> Dict:
    """Create a dataset profile aspect payload."""
    return {
        "value": {
            "timestampMillis": timestamp_millis,
            "messageId": message_id,
            "rowCount": row_count,
            "columnCount": 10,
        }
    }


def ingest_profile(
    auth_session, timestamp_millis: int, row_count: int, message_id: str
) -> None:
    """Ingest a single dataset profile via OpenAPI v3."""
    url = f"{auth_session.gms_url()}/openapi/v3/entity/dataset/{TEST_DATASET_URN.replace(':', '%3A').replace('(', '%28').replace(')', '%29').replace(',', '%2C')}/datasetProfile"
    params = {"createIfNotExists": "false", "async": "false"}
    payload = create_dataset_profile(timestamp_millis, row_count, message_id)

    response = auth_session.post(url, params=params, json=payload)
    assert response.status_code in [200, 201, 202], (
        f"Failed to ingest profile: {response.text}"
    )


def cleanup_test_data(auth_session) -> None:
    """Clean up test dataset and profiles."""
    url = f"{auth_session.gms_url()}/openapi/v3/entity/dataset/{TEST_DATASET_URN.replace(':', '%3A').replace('(', '%28').replace(')', '%29').replace(',', '%2C')}"
    auth_session.delete(url)
    # Wait for deletion to propagate
    time.sleep(2)


def scroll_timeseries(
    auth_session,
    count: int = 10,
    scroll_id: Optional[str] = None,
    start_time_millis: Optional[int] = None,
    end_time_millis: Optional[int] = None,
) -> Dict:
    """Call the OpenAPI v2 timeseries scroll endpoint."""
    url = f"{auth_session.gms_url()}/openapi/v2/timeseries/{TEST_ENTITY_NAME}/{TEST_ASPECT_NAME}"

    params: Dict[str, Any] = {"count": count}
    if scroll_id:
        params["scrollId"] = scroll_id
    if start_time_millis:
        params["startTimeMillis"] = start_time_millis
    if end_time_millis:
        params["endTimeMillis"] = end_time_millis

    response = auth_session.get(url, params=params)
    assert response.status_code == 200, f"Scroll request failed: {response.text}"
    return response.json()


@pytest.fixture(scope="function")
def setup_timeseries_data(auth_session):
    """Setup: Clean up and create test timeseries data."""
    # Cleanup any existing test data
    cleanup_test_data(auth_session)

    # Create multiple profiles with distinct timestamps
    base_timestamp = 1700000000000  # Fixed base timestamp for reproducibility
    created_message_ids: List[str] = []

    for i in range(NUM_PROFILES):
        timestamp = base_timestamp + (i * 1000)  # 1 second apart
        message_id = f"scroll_test_msg_{i}"
        row_count = (i + 1) * 100
        ingest_profile(auth_session, timestamp, row_count, message_id)
        created_message_ids.append(message_id)
        logger.info(
            f"Created profile {i}: timestamp={timestamp}, messageId={message_id}"
        )

    # Wait for ES to index
    time.sleep(3)

    yield {
        "base_timestamp": base_timestamp,
        "message_ids": created_message_ids,
        "num_profiles": NUM_PROFILES,
    }

    # Cleanup after test
    cleanup_test_data(auth_session)


def test_timeseries_scroll_returns_scrollid(auth_session, setup_timeseries_data):
    """Test that scrollId is returned when there are more results than the page size."""
    test_data = setup_timeseries_data

    # Request with small page size
    result = scroll_timeseries(
        auth_session,
        count=PAGE_SIZE,
        start_time_millis=test_data["base_timestamp"] - 1000,
        end_time_millis=test_data["base_timestamp"] + (NUM_PROFILES * 1000) + 1000,
    )

    # Verify we got results
    assert "results" in result, f"Expected 'results' in response: {result}"
    results = result["results"]
    assert len(results) == PAGE_SIZE, (
        f"Expected {PAGE_SIZE} results, got {len(results)}"
    )

    # Verify scrollId is present (indicating more results)
    assert "scrollId" in result and result["scrollId"], (
        f"Expected scrollId when results ({len(results)}) equals page size ({PAGE_SIZE}). "
        f"Response: {result}"
    )

    logger.info(
        f"First page: {len(results)} results, scrollId present: {bool(result.get('scrollId'))}"
    )


def test_timeseries_scroll_pagination_no_duplicates(
    auth_session, setup_timeseries_data
):
    """Test that pagination retrieves all results without duplicates."""
    test_data = setup_timeseries_data

    all_message_ids: Set[str] = set()
    all_results: List[Dict] = []
    scroll_id = None
    page_count = 0
    max_pages = 10  # Safety limit

    # Paginate through all results
    while page_count < max_pages:
        result = scroll_timeseries(
            auth_session,
            count=PAGE_SIZE,
            scroll_id=scroll_id,
            start_time_millis=test_data["base_timestamp"] - 1000,
            end_time_millis=test_data["base_timestamp"] + (NUM_PROFILES * 1000) + 1000,
        )

        results = result.get("results", [])
        if not results:
            break

        page_count += 1
        logger.info(f"Page {page_count}: {len(results)} results")

        for r in results:
            msg_id = r.get("messageId")
            # Check for duplicates
            assert msg_id not in all_message_ids, (
                f"Duplicate messageId found: {msg_id}. "
                f"This indicates pagination is not working correctly."
            )
            all_message_ids.add(msg_id)
            all_results.append(r)

        # Check for next page
        scroll_id = result.get("scrollId")
        if not scroll_id or len(results) < PAGE_SIZE:
            break

    # Verify we got all expected profiles
    logger.info(f"Total pages: {page_count}, Total results: {len(all_results)}")
    logger.info(f"Retrieved messageIds: {sorted(all_message_ids)}")

    # We should have retrieved all our test profiles
    expected_ids = set(test_data["message_ids"])
    retrieved_test_ids = {
        mid for mid in all_message_ids if mid.startswith("scroll_test_msg_")
    }

    assert retrieved_test_ids == expected_ids, (
        f"Expected to retrieve all test profiles. "
        f"Expected: {expected_ids}, Got: {retrieved_test_ids}"
    )


def test_timeseries_scroll_no_scrollid_on_last_page(
    auth_session, setup_timeseries_data
):
    """Test that scrollId is not returned when we've retrieved all results."""
    test_data = setup_timeseries_data

    # Request with page size larger than total results
    large_page_size = NUM_PROFILES + 10
    result = scroll_timeseries(
        auth_session,
        count=large_page_size,
        start_time_millis=test_data["base_timestamp"] - 1000,
        end_time_millis=test_data["base_timestamp"] + (NUM_PROFILES * 1000) + 1000,
    )

    results = result.get("results", [])
    logger.info(f"Requested {large_page_size}, got {len(results)} results")

    # Since we got fewer results than requested, scrollId should be null/absent
    scroll_id = result.get("scrollId")
    if len(results) < large_page_size:
        assert not scroll_id, (
            f"scrollId should be null when results ({len(results)}) < page size ({large_page_size}). "
            f"Got scrollId: {scroll_id}"
        )
