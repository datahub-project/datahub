"""
Test for dataset assertions query that powers the observe/datasets UI view.

This test verifies that the searchAcrossEntities query with assertion-related filters
works correctly without errors. This is a read-only test that can run against any
deployment, including empty ones.
"""

import logging

import pytest

from tests.test_result_msg import add_datahub_stats
from tests.utilities.metadata_operations import search_datasets_by_assertions

logger = logging.getLogger(__name__)


@pytest.mark.read_only
def test_dataset_assertions_query(auth_session):
    """
    Test the dataset assertions query used by the observe/datasets UI.

    This query filters datasets by various assertion states:
    - hasFailingAssertions
    - hasPassingAssertions
    - hasErroredAssertions
    - hasInitializingAssertions

    And sorts by lastAssertionResultAt in descending order.

    Note: This test only verifies the query executes without errors.
    It does not require datasets to exist (works on empty deployments).
    """
    logger.info("Testing dataset assertions query from observe/datasets UI")

    # Execute the query using the helper
    search_data = search_datasets_by_assertions(auth_session)

    # Verify response structure (but allow empty results)
    assert search_data is not None, "Received None response"
    assert "start" in search_data, "start field not found in response"
    assert "count" in search_data, "count field not found in response"
    assert "total" in search_data, "total field not found in response"
    assert "searchResults" in search_data, "searchResults not found in response"

    # Log results
    total_results = search_data["total"]
    results_count = len(search_data["searchResults"])
    logger.info(
        f"Dataset assertions query returned {results_count} results "
        f"(total matching: {total_results})"
    )
    add_datahub_stats("num-datasets-with-assertions", total_results)

    # If there are results, verify they are datasets
    for result in search_data["searchResults"]:
        assert "entity" in result, "entity not found in search result"
        entity = result["entity"]
        assert "urn" in entity, "urn not found in entity"
        assert "type" in entity, "type not found in entity"
        assert entity["type"] == "DATASET", (
            f"Expected type DATASET, got {entity['type']}"
        )

    logger.info("Dataset assertions query test passed successfully")
