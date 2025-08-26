import json
import logging
import os
import tempfile
import urllib.parse
from typing import Any, Dict, List, Optional

import pytest
import tenacity

from tests.utils import delete_urns_from_file, ingest_file_via_rest

logger = logging.getLogger(__name__)

# Test constants
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"
TEST_USER_1 = "urn:li:corpuser:user1"
TEST_USER_2 = "urn:li:corpuser:user2"
NONEXISTENT_USER = "urn:li:corpuser:nonexistent_user"


def create_test_mcp_data() -> Dict[str, Any]:
    """Create the MCP data for testing top users last 30 days functionality."""
    return {
        "entityType": "dataset",
        "entityUrn": DATASET_URN,
        "changeType": "UPSERT",
        "aspectName": "usageFeatures",
        "aspect": {"json": {"topUsersLast30Days": [TEST_USER_1, TEST_USER_2]}},
    }


def create_single_user_mcp_data() -> Dict[str, Any]:
    """Create the MCP data with only one user for testing top users last 30 days functionality."""
    return {
        "entityType": "dataset",
        "entityUrn": DATASET_URN,
        "changeType": "UPSERT",
        "aspectName": "usageFeatures",
        "aspect": {"json": {"topUsersLast30Days": [TEST_USER_1]}},
    }


def create_search_query(user_urns: Optional[List[str]] = None) -> Dict[str, Any]:
    """Create the search query to test top users last 30 days filtering."""
    if user_urns is None:
        user_urns = [TEST_USER_1]

    return {
        "types": [],
        "query": "*",
        "start": 0,
        "count": 10,
        "filters": [],
        "orFilters": [
            {
                "and": [
                    {
                        "field": "topUsersLast30Days",
                        "condition": "EQUAL",
                        "values": user_urns,
                        "negated": False,
                    }
                ]
            }
        ],
        "searchFlags": {
            "getSuggestions": True,
            "includeStructuredPropertyFacets": True,
        },
    }


def get_dataset_graphql_query(urn: str = DATASET_URN) -> Dict[str, Any]:
    """Create GraphQL query to get dataset with statsSummary."""
    return {
        "query": """
        query getDataset($urn: String!) {
            dataset(urn: $urn) {
                urn
                type
                name
                properties {
                    name
                    description
                }
                statsSummary {
                    topUsersLast30Days {
                        urn
                        type
                        username
                        properties {
                            displayName
                            firstName
                            lastName
                            fullName
                        }
                    }
                }
            }
        }
        """,
        "variables": {"urn": urn},
    }


def get_search_graphql_query(
    search_input: Dict[str, Any], include_stats: bool = True
) -> Dict[str, Any]:
    """Create GraphQL query for search across entities."""
    stats_summary_fragment = (
        """
                            statsSummary {
                                topUsersLast30Days {
                                    urn
                                    type
                                    username
                                    properties {
                                        displayName
                                        firstName
                                        lastName
                                        fullName
                                    }
                                }
                            }
    """
        if include_stats
        else ""
    )

    return {
        "query": f"""
        query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {{
            searchAcrossEntities(input: $input) {{
                start
                count
                total
                searchResults {{
                    entity {{
                        urn
                        type
                        ... on Dataset {{
                            name
                            properties {{
                                name
                                description
                            }}
                            {stats_summary_fragment}
                        }}
                    }}
                }}
            }}
        }}
        """,
        "variables": {"input": search_input},
    }


def execute_graphql_query(auth_session, query: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a GraphQL query and return the result."""
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=query
    )
    response.raise_for_status()
    return response.json()


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=10),
    retry=tenacity.retry_if_exception_type(AssertionError),
)
def verify_dataset_exists_and_has_top_users(
    auth_session,
    dataset_urn: str = DATASET_URN,
    expected_users: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Verify that a dataset exists and has the expected top users using REST API."""
    if expected_users is None:
        expected_users = [TEST_USER_1, TEST_USER_2]

    # Check the raw aspect data directly via REST API
    logger.info("Checking raw usageFeatures aspect via REST API...")
    try:
        encoded_urn = urllib.parse.quote(dataset_urn)
        entity_url = f"{auth_session.gms_url()}/entities/{encoded_urn}"
        response = auth_session.get(entity_url)
        response.raise_for_status()
        entity_data = response.json()
        logger.info(f"Raw entity data: {entity_data}")

        # Parse the nested structure to find usageFeatures aspect
        aspects = (
            entity_data.get("value", {})
            .get("com.linkedin.metadata.snapshot.DatasetSnapshot", {})
            .get("aspects", [])
        )

        usage_features_aspect = None
        for aspect in aspects:
            if "com.linkedin.metadata.search.features.UsageFeatures" in aspect:
                usage_features_aspect = aspect[
                    "com.linkedin.metadata.search.features.UsageFeatures"
                ]
                break

        if not usage_features_aspect:
            raise AssertionError("usageFeatures aspect not found in entity data")

        raw_top_users = usage_features_aspect.get("topUsersLast30Days", [])
        logger.info(f"Raw topUsersLast30Days from REST API: {raw_top_users}")

        # Also check the specific aspect via the aspect endpoint
        aspect_url = (
            f"{auth_session.gms_url()}/entities/{encoded_urn}/aspects/usageFeatures"
        )
        aspect_response = auth_session.get(aspect_url)
        if aspect_response.status_code == 200:
            aspect_data = aspect_response.json()
            logger.info(f"Raw usageFeatures aspect data: {aspect_data}")
        else:
            logger.warning(
                f"Could not fetch usageFeatures aspect: {aspect_response.status_code}"
            )

        # Validate the data from REST API
        actual_user_urns = raw_top_users

        # Add debugging output to show the data
        logger.info(f"Expected users: {expected_users}")
        logger.info(f"Actual users returned by REST API: {actual_user_urns}")

        assert set(actual_user_urns) == set(expected_users), (
            f"Expected users {expected_users}, got {actual_user_urns}"
        )

        logger.info(f"Entity verification successful via REST API: {dataset_urn}")
        return {"urn": dataset_urn, "topUsersLast30Days": actual_user_urns}

    except Exception as e:
        logger.error(f"Could not fetch raw entity data: {e}")
        raise


def execute_search_and_verify_results(
    auth_session,
    search_query: Dict[str, Any],
    expected_dataset_urn: str = DATASET_URN,
    should_find_dataset: bool = True,
    include_stats: bool = True,
) -> Dict[str, Any]:
    """Execute a search query and verify the results."""
    query = get_search_graphql_query(search_query, include_stats)
    search_result = execute_graphql_query(auth_session, query)

    # Verify search results structure
    assert search_result["data"], f"Search response missing data: {search_result}"
    assert search_result["data"]["searchAcrossEntities"], (
        "searchAcrossEntities not found"
    )

    search_data = search_result["data"]["searchAcrossEntities"]

    if should_find_dataset:
        assert search_data["total"] >= 1, (
            f"Expected at least 1 result, got {search_data['total']}"
        )

        # Verify our dataset is in the search results
        search_results = search_data["searchResults"]
        found_dataset = False

        for result in search_results:
            entity = result["entity"]
            if entity["urn"] == expected_dataset_urn:
                found_dataset = True
                if include_stats:
                    # Verify the dataset has the expected statsSummary
                    assert entity["statsSummary"], (
                        "statsSummary not found in search result"
                    )
                    assert entity["statsSummary"]["topUsersLast30Days"], (
                        "topUsersLast30Days not found in search result"
                    )

                    top_users_in_search = entity["statsSummary"]["topUsersLast30Days"]
                    user_urns_in_search = [user["urn"] for user in top_users_in_search]
                    assert TEST_USER_1 in user_urns_in_search, (
                        "user1 not found in search result topUsersLast30Days"
                    )
                break

        assert found_dataset, (
            f"Dataset {expected_dataset_urn} not found in search results"
        )
        logger.info(
            f"Search test successful: found {search_data['total']} results including our dataset"
        )
    else:
        # Verify that the dataset is NOT found
        search_results = search_data["searchResults"]
        found_dataset = False
        for result in search_results:
            if result["entity"]["urn"] == expected_dataset_urn:
                found_dataset = True
                break

        assert not found_dataset, (
            f"Dataset {expected_dataset_urn} should not be found in search results"
        )
        logger.info(
            "Search filter test successful: dataset correctly excluded from search results"
        )

    return search_data


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    """Fixture to ingest test data and clean up after tests."""
    # Create temporary file for MCP data
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        mcp_data = [create_test_mcp_data()]
        json.dump(mcp_data, f, indent=2)
        temp_file_path = f.name

    try:
        logger.info("ingesting top users last 30 days test data")
        ingest_file_via_rest(auth_session, temp_file_path)
        yield
        logger.info("removing top users last 30 days test data")
        delete_urns_from_file(graph_client, temp_file_path)
    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


def test_top_users_last_30_days_ingestion_and_search(auth_session):
    """
    Test that ingests MCP data with usageFeatures aspect containing topUsersLast30Days,
    verifies the entity exists, and tests search functionality with the specified filters.
    """
    # Step 1: Verify the entity exists using DataHub get functionality
    verify_dataset_exists_and_has_top_users(auth_session)

    # Step 2: Test search functionality with the specified filters
    search_query = create_search_query()
    execute_search_and_verify_results(auth_session, search_query)


def test_top_users_last_30_days_search_with_different_user(auth_session):
    """
    Test search functionality with a different user to ensure the filter works correctly.
    """
    # Test search with a user that doesn't exist in topUsersLast30Days
    search_query = create_search_query([NONEXISTENT_USER])
    execute_search_and_verify_results(
        auth_session, search_query, should_find_dataset=False, include_stats=False
    )


def test_top_users_last_30_days_update_to_single_user(auth_session):
    """
    Test that updates the top users to only one user and verifies that GraphQL returns only that single user.
    """
    # Step 1: Verify initial state has 2 users
    verify_dataset_exists_and_has_top_users(
        auth_session, expected_users=[TEST_USER_1, TEST_USER_2]
    )

    # Step 2: Create temporary file with single user MCP data
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        mcp_data = [create_single_user_mcp_data()]
        json.dump(mcp_data, f, indent=2)
        temp_file_path = f.name

    try:
        # Step 3: Ingest the updated data with only one user
        logger.info("Updating top users last 30 days to single user")
        ingest_file_via_rest(auth_session, temp_file_path)

        # Step 4: Verify that the dataset now has only one user (with tenacity retry)
        verify_dataset_exists_and_has_top_users(
            auth_session, expected_users=[TEST_USER_1]
        )

        # Step 5: Test search functionality still works with the single user
        search_query = create_search_query([TEST_USER_1])
        execute_search_and_verify_results(auth_session, search_query)

        logger.info(
            "Successfully updated and verified single user in topUsersLast30Days"
        )

    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
