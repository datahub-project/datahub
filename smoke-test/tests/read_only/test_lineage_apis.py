# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# ABOUTME: Read-only tests for lineage APIs (searchAcrossLineage, scrollAcrossLineage).
# ABOUTME: These tests query existing lineage data without modifying any state.

import logging

import pytest

from tests.utilities.metadata_operations import (
    get_search_results,
    scroll_across_lineage,
    search_across_lineage,
)
from tests.utils import execute_graphql

logger = logging.getLogger(__name__)


def _find_urn_with_lineage(auth_session) -> str:
    """Find a URN that has upstream lineage, or any dataset URN as fallback."""
    # Try to find datasets with upstream lineage using hasUpstreams filter
    query = """
        query search($input: SearchInput!) {
            search(input: $input) {
                total
                searchResults {
                    entity {
                        urn
                    }
                }
            }
        }
    """
    variables = {
        "input": {
            "type": "DATASET",
            "query": "*",
            "start": 0,
            "count": 10,
            "orFilters": [
                {
                    "and": [
                        {
                            "field": "hasUpstreams",
                            "values": ["true"],
                            "condition": "EQUAL",
                        }
                    ]
                }
            ],
        }
    }

    res_data = execute_graphql(auth_session, query, variables)
    search_result = res_data["data"]["search"]

    if search_result["total"] > 0:
        urn = search_result["searchResults"][0]["entity"]["urn"]
        logger.info(f"Found URN with lineage: {urn}")
        return urn

    # Fallback: get any dataset URN
    logger.warning("No datasets with lineage found, using any dataset as fallback")
    fallback_result = get_search_results(auth_session, "dataset")
    if fallback_result["total"] == 0:
        pytest.skip("No datasets available for lineage API testing")

    urn = fallback_result["searchResults"][0]["entity"]["urn"]
    logger.info(f"Using fallback URN: {urn}")
    return urn


@pytest.mark.read_only
def test_search_across_lineage_api(auth_session):
    """Test searchAcrossLineage API for basic functionality."""
    urn = _find_urn_with_lineage(auth_session)

    # Call searchAcrossLineage with UPSTREAM direction
    result = search_across_lineage(
        auth_session, urn=urn, direction="UPSTREAM", count=10
    )

    # Validate response structure
    assert "searchResults" in result, "Response should contain searchResults"
    assert "total" in result, "Response should contain total"
    assert "count" in result, "Response should contain count"
    assert "start" in result, "Response should contain start"

    # Validate searchResults structure (if any results exist)
    if result["total"] > 0:
        first_result = result["searchResults"][0]
        assert "entity" in first_result, "Result should contain entity"
        assert "urn" in first_result["entity"], "Entity should contain urn"
        assert "degree" in first_result, "Result should contain degree"
        logger.info(f"searchAcrossLineage returned {result['total']} results for {urn}")
    else:
        logger.info(f"searchAcrossLineage returned 0 results for {urn} (expected)")


@pytest.mark.read_only
def test_scroll_across_lineage_api(auth_session):
    """Test scrollAcrossLineage API with pagination."""
    urn = _find_urn_with_lineage(auth_session)

    # First call: get initial results without scrollId
    result = scroll_across_lineage(
        auth_session, urn=urn, direction="UPSTREAM", count=10
    )

    # Validate response structure
    assert "searchResults" in result, "Response should contain searchResults"
    assert "total" in result, "Response should contain total"
    assert "count" in result, "Response should contain count"
    assert "nextScrollId" in result, "Response should contain nextScrollId"

    # Validate searchResults structure (if any results exist)
    if result["total"] > 0:
        first_result = result["searchResults"][0]
        assert "entity" in first_result, "Result should contain entity"
        assert "urn" in first_result["entity"], "Entity should contain urn"
        assert "degree" in first_result, "Result should contain degree"
        logger.info(f"scrollAcrossLineage returned {result['total']} results for {urn}")
    else:
        logger.info(f"scrollAcrossLineage returned 0 results for {urn} (expected)")

    # Test pagination if nextScrollId exists
    next_scroll_id = result.get("nextScrollId")
    if next_scroll_id:
        logger.info(f"Testing pagination with scrollId: {next_scroll_id}")
        next_result = scroll_across_lineage(
            auth_session,
            urn=urn,
            direction="UPSTREAM",
            scroll_id=next_scroll_id,
            count=10,
        )

        # Validate second page response
        assert "searchResults" in next_result, "Next page should contain searchResults"
        assert "total" in next_result, "Next page should contain total"
        assert "count" in next_result, "Next page should contain count"
        logger.info("Pagination test successful")
    else:
        logger.info("No nextScrollId returned, skipping pagination test")
