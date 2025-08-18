import json
import logging
import os
import tempfile
import urllib.parse
from typing import Any, Dict, List

import pytest
import tenacity

from tests.utils import delete_urns_from_file, execute_gql, ingest_file_via_rest

logger = logging.getLogger(__name__)

# Test constants
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,test_fetch_extra_fields,PROD)"


def create_test_dataset_mcp_data() -> Dict[str, Any]:
    """Create the MCP data for testing fetchExtraFields functionality."""
    return {
        "entityType": "dataset",
        "entityUrn": DATASET_URN,
        "changeType": "UPSERT",
        "aspectName": "datasetProperties",
        "aspect": {
            "json": {
                "name": "test_fetch_extra_fields",
                "description": "Dataset for testing fetchExtraFields functionality",
                "customProperties": {"platform": "hive", "env": "PROD"},
            }
        },
    }


def create_test_dataset_profile_mcp_data() -> Dict[str, Any]:
    """Create the MCP data with dataset profile containing rowCount and sizeInBytes for fetchExtraFields."""
    return {
        "entityType": "dataset",
        "entityUrn": DATASET_URN,
        "changeType": "UPSERT",
        "aspectName": "datasetProfile",
        "aspect": {
            "json": {
                "columnCount": 5,
                "partitionSpec": {
                    "partition": "FULL_TABLE_SNAPSHOT",
                    "type": "FULL_TABLE",
                },
                "rowCount": 10000,
                "sizeInBytes": 5242880,
                "timestampMillis": 0,
            }
        },
    }


def get_search_across_entities_query_with_fetch_extra_fields() -> Dict[str, Any]:
    """Create GraphQL query for searchAcrossEntities with fetchExtraFields."""
    return {
        "query": """
        query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
            searchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                        ... on Dataset {
                            name
                            properties {
                                name
                                description
                            }
                        }
                    }
                    extraProperties {
                        name
                        value
                    }
                }
            }
        }
        """,
        "variables": {
            "input": {
                "types": ["DATASET"],
                "query": "test_fetch_extra_fields",
                "start": 0,
                "count": 10,
                "searchFlags": {
                    "fetchExtraFields": [
                        "rowCount",
                        "sizeInBytes",
                    ]
                },
            }
        },
    }


def get_scroll_across_entities_query_with_fetch_extra_fields() -> Dict[str, Any]:
    """Create GraphQL query for scrollAcrossEntities with fetchExtraFields using orFilters."""
    return {
        "query": """
        query scrollAcrossEntities($input: ScrollAcrossEntitiesInput!) {
            scrollAcrossEntities(input: $input) {
                nextScrollId
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                        ... on Dataset {
                            name
                            properties {
                                name
                                description
                            }
                        }
                    }
                    extraProperties {
                        name
                        value
                    }
                }
            }
        }
        """,
        "variables": {
            "input": {
                "query": "*",
                "count": 10,
                "orFilters": [{"and": [{"field": "urn", "values": [DATASET_URN]}]}],
                "searchFlags": {
                    "fetchExtraFields": [
                        "rowCount",
                        "sizeInBytes",
                    ]
                },
            }
        },
    }


def execute_graphql_query(auth_session, query: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a GraphQL query and return the result."""
    return execute_gql(auth_session, query["query"], query.get("variables"))


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=10),
    retry=tenacity.retry_if_exception_type(AssertionError),
)
def verify_dataset_exists_with_aspects(
    auth_session, dataset_urn: str = DATASET_URN
) -> None:
    """Verify that the dataset exists and has the expected aspects using GMS REST API."""
    logger.info(f"Verifying dataset exists with aspects: {dataset_urn}")

    # Use GMS REST API to get entity aspects directly
    encoded_urn = urllib.parse.quote(dataset_urn)
    aspects_param = "datasetProperties"
    url = f"{auth_session.gms_url()}/entitiesV2?ids=List({encoded_urn})&aspects=List({aspects_param})"

    # Add RestLi headers as used in test_e2e.py
    restli_headers = {
        "X-RestLi-Protocol-Version": "2.0.0",
        "X-RestLi-Method": "batch_get",
    }

    response = auth_session.get(url, headers=restli_headers)
    if not response.ok:
        logger.error(
            f"GMS API error: Status {response.status_code}, Response: {response.text}"
        )
        response.raise_for_status()

    res_data = response.json()
    assert "results" in res_data, f"Results not found in response: {res_data}"
    assert dataset_urn in res_data["results"], (
        f"Dataset {dataset_urn} not found in results"
    )

    entity_data = res_data["results"][dataset_urn]
    assert "aspects" in entity_data, f"Aspects not found for dataset {dataset_urn}"

    aspects = entity_data["aspects"]
    aspect_names = set(aspects.keys())

    logger.info(f"Found aspects for dataset {dataset_urn}: {aspect_names}")

    # Check for required aspects
    required_aspects = {"datasetProperties"}
    missing_aspects = required_aspects - aspect_names

    assert len(missing_aspects) == 0, (
        f"Dataset {dataset_urn} is missing required aspects: {missing_aspects}. "
        f"Found aspects: {aspect_names}"
    )

    logger.info(f"Dataset {dataset_urn} verified with all required aspects and fields")


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=10),
    retry=tenacity.retry_if_exception_type(AssertionError),
)
def verify_extra_properties_in_results(
    search_results: List[Dict[str, Any]],
    expected_extra_fields: List[str],
    dataset_urn: str = DATASET_URN,
) -> None:
    """Verify that search results contain the expected extra properties."""
    # Find our test dataset in the results
    dataset_result = None
    for result in search_results:
        if result["entity"]["urn"] == dataset_urn:
            dataset_result = result
            break

    assert dataset_result is not None, (
        f"Dataset {dataset_urn} not found in search results"
    )

    # Verify extraProperties exist
    assert "extraProperties" in dataset_result, (
        "extraProperties field not found in search result"
    )
    extra_properties = dataset_result["extraProperties"]

    # Extract property names from the results
    actual_property_names = {prop["name"] for prop in extra_properties}

    # Check that we have some of the expected extra fields (profile fields should be present)
    expected_profile_fields = {
        "rowCount",
        "sizeInBytes",
    }
    found_profile_fields = expected_profile_fields.intersection(actual_property_names)

    logger.info(f"Found extra properties: {actual_property_names}")
    logger.info(f"Expected profile fields: {expected_profile_fields}")
    logger.info(f"Found profile fields: {found_profile_fields}")

    # We should find at least some of the profile fields we ingested
    assert len(found_profile_fields) > 0, (
        f"Expected to find some profile fields {expected_profile_fields} in extra properties, "
        f"but found: {actual_property_names}"
    )


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    """Fixture to ingest test data and clean up after tests."""
    # Create temporary file for MCP data
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        mcp_data = [
            create_test_dataset_mcp_data(),
            create_test_dataset_profile_mcp_data(),
        ]
        json.dump(mcp_data, f, indent=2)
        temp_file_path = f.name

    try:
        logger.info("Ingesting fetchExtraFields test data")
        ingest_file_via_rest(auth_session, temp_file_path)
        yield
        logger.info("Removing fetchExtraFields test data")
        delete_urns_from_file(graph_client, temp_file_path)
    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=8),
    retry=tenacity.retry_if_exception_type(AssertionError),
)
def _execute_search_with_retry(auth_session, expected_extra_fields):
    """Execute search query with retry logic."""
    # Execute searchAcrossEntities with fetchExtraFields
    query = get_search_across_entities_query_with_fetch_extra_fields()
    response = execute_graphql_query(auth_session, query)

    # Verify response structure
    assert "data" in response, f"Response missing data: {response}"
    assert "searchAcrossEntities" in response["data"], (
        "searchAcrossEntities not found in response"
    )

    search_data = response["data"]["searchAcrossEntities"]
    assert "searchResults" in search_data, "searchResults not found in response"
    assert search_data["total"] >= 1, (
        f"Expected at least 1 result, got {search_data['total']}"
    )

    search_results = search_data["searchResults"]
    logger.info(f"Found {len(search_results)} search results")

    # Verify that extra properties are returned
    verify_extra_properties_in_results(search_results, expected_extra_fields)
    return search_results


def test_search_across_entities_with_fetch_extra_fields(auth_session):
    """
    Test that searchAcrossEntities properly handles fetchExtraFields and returns extra properties.
    """
    logger.info("Testing searchAcrossEntities with fetchExtraFields")

    # First verify the dataset exists with the required aspects
    verify_dataset_exists_with_aspects(auth_session)

    expected_extra_fields = [
        "rowCount",
        "sizeInBytes",
    ]
    _execute_search_with_retry(auth_session, expected_extra_fields)

    logger.info("searchAcrossEntities with fetchExtraFields test passed")


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=8),
    retry=tenacity.retry_if_exception_type(AssertionError),
)
def _execute_scroll_with_retry(auth_session, expected_extra_fields):
    """Execute scroll query with retry logic."""
    # Execute scrollAcrossEntities with fetchExtraFields
    query = get_scroll_across_entities_query_with_fetch_extra_fields()
    response = execute_graphql_query(auth_session, query)

    # Verify response structure
    assert "data" in response, f"Response missing data: {response}"
    assert "scrollAcrossEntities" in response["data"], (
        "scrollAcrossEntities not found in response"
    )

    scroll_data = response["data"]["scrollAcrossEntities"]
    assert "searchResults" in scroll_data, "searchResults not found in response"
    assert scroll_data["total"] >= 1, (
        f"Expected at least 1 result, got {scroll_data['total']}"
    )

    search_results = scroll_data["searchResults"]
    logger.info(f"Found {len(search_results)} scroll results")

    # Verify that extra properties are returned
    verify_extra_properties_in_results(search_results, expected_extra_fields)
    return search_results


def test_scroll_across_entities_with_fetch_extra_fields(auth_session):
    """
    Test that scrollAcrossEntities properly handles fetchExtraFields and returns extra properties.
    """
    logger.info("Testing scrollAcrossEntities with fetchExtraFields")

    # First verify the dataset exists with the required aspects
    verify_dataset_exists_with_aspects(auth_session)

    expected_extra_fields = [
        "rowCount",
        "sizeInBytes",
    ]
    _execute_scroll_with_retry(auth_session, expected_extra_fields)

    logger.info("scrollAcrossEntities with fetchExtraFields test passed")


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=8),
    retry=tenacity.retry_if_exception_type(AssertionError),
)
def _execute_search_without_extra_fields_with_retry(auth_session):
    """Execute search query without fetchExtraFields with retry logic."""
    # Create query without fetchExtraFields
    query = {
        "query": """
        query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
            searchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                    }
                    extraProperties {
                        name
                        value
                    }
                }
            }
        }
        """,
        "variables": {
            "input": {
                "types": ["DATASET"],
                "query": "test_fetch_extra_fields",
                "start": 0,
                "count": 10,
                # No searchFlags or fetchExtraFields specified
            }
        },
    }

    response = execute_graphql_query(auth_session, query)
    search_results = response["data"]["searchAcrossEntities"]["searchResults"]

    # Find our test dataset
    dataset_result = None
    for result in search_results:
        if result["entity"]["urn"] == DATASET_URN:
            dataset_result = result
            break

    assert dataset_result is not None, (
        f"Dataset {DATASET_URN} not found in search results"
    )

    # Verify that extraProperties is empty or contains only default fields
    extra_properties = dataset_result.get("extraProperties", [])
    logger.info(f"Extra properties without fetchExtraFields: {extra_properties}")

    # The profile fields should not be present when fetchExtraFields is not specified
    property_names = {prop["name"] for prop in extra_properties}
    profile_fields = {"rowCount", "sizeInBytes"}
    found_profile_fields = profile_fields.intersection(property_names)

    assert len(found_profile_fields) == 0, (
        f"Expected no profile fields when fetchExtraFields is not specified, "
        f"but found: {found_profile_fields}"
    )


def test_search_without_fetch_extra_fields_returns_no_extra_properties(auth_session):
    """
    Test that when fetchExtraFields is not specified, no extra properties are returned.
    """
    logger.info("Testing searchAcrossEntities without fetchExtraFields")

    _execute_search_without_extra_fields_with_retry(auth_session)

    logger.info("Test without fetchExtraFields passed - no profile fields returned")
