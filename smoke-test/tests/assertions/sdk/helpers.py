"""Helper functions and constants for SDK assertion tests."""

import logging
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from tests.consistency_utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)


def wait_for_assertion_sync() -> None:
    """Faster wait suitable for single-entity assertion operations.

    Uses a 30-second timeout instead of the default 120 seconds,
    which is sufficient for assertion create/update/delete operations.
    """
    wait_for_writes_to_sync(max_timeout_in_sec=30)


def generate_unique_test_id() -> str:
    """Generate a unique identifier for test isolation.

    Returns a short UUID string suitable for use in display names and URNs.
    """
    return str(uuid.uuid4())[:8]


def get_nested_value(data: dict, path: str, default: Any = None) -> Any:
    """Get a value from a nested dictionary using dot notation.

    Args:
        data: The dictionary to search
        path: Dot-separated path (e.g., "info.source.type")
        default: Default value if path not found

    Returns:
        The value at the path, or default if not found

    Examples:
        >>> get_nested_value({"info": {"source": {"type": "NATIVE"}}}, "info.source.type")
        "NATIVE"
        >>> get_nested_value({"info": {}}, "info.source.type", None)
        None
    """
    keys = path.split(".")
    current = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


GRAPHQL_GET_ASSERTION_MONITOR = """
    query getAssertionMonitor($assertionUrn: String!) {
        assertion(urn: $assertionUrn) {
            monitor {
                urn
            }
        }
    }
"""


def cleanup_assertion(graph_client: DataHubGraph, assertion_urn: str) -> None:
    """Helper to clean up an assertion and its associated monitor after test.

    Uses a shorter 10-second timeout since cleanup doesn't need
    to verify the deletion is visible in search indexes.

    Args:
        graph_client: The DataHub graph client
        assertion_urn: The URN of the assertion to delete
    """
    # First, try to find and delete the associated monitor
    try:
        result = graph_client.execute_graphql(
            query=GRAPHQL_GET_ASSERTION_MONITOR,
            variables={"assertionUrn": assertion_urn},
        )
        monitor = result.get("assertion", {}).get("monitor")
        if monitor and monitor.get("urn"):
            monitor_urn = monitor["urn"]
            graph_client.delete_entity(monitor_urn, hard=True)
            logger.debug(f"Deleted monitor: {monitor_urn}")
    except Exception as e:
        logger.debug(
            f"Could not find/delete monitor for assertion {assertion_urn}: {e}"
        )

    # Delete the assertion
    graph_client.delete_entity(assertion_urn, hard=True)
    wait_for_writes_to_sync(max_timeout_in_sec=10)


# =============================================================================
# Assertion Result Helpers
# =============================================================================


def assert_result_type(result: Dict, expected_type: str, context: str) -> None:
    """Assert that the result has the expected type (SUCCESS, FAILURE, etc.)."""
    actual_type = result.get("type")
    assert actual_type == expected_type, (
        f"{context}: Expected result type '{expected_type}', got '{actual_type}'. "
        f"Full result: {result}"
    )


def assert_no_error(result: Dict, context: str) -> None:
    """Assert that the result does not have an error."""
    error = result.get("error")
    assert error is None, (
        f"{context}: Unexpected error in result: {error}. Full result: {result}"
    )


def assert_error_type(result: Dict, expected_type: str, context: str) -> None:
    """Assert that the result has the expected error type."""
    assert "error" in result and result["error"] is not None, (
        f"{context}: Expected error in result but got none. Result: {result}"
    )
    actual_type = result["error"].get("type")
    assert actual_type == expected_type, (
        f"{context}: Expected error type '{expected_type}', got '{actual_type}'. "
        f"Error: {result['error']}"
    )


def assert_error_type_in(result: Dict, expected_types: List[str], context: str) -> None:
    """Assert that the result has one of the expected error types."""
    assert "error" in result and result["error"] is not None, (
        f"{context}: Expected error in result but got none. Result: {result}"
    )
    actual_type = result["error"].get("type")
    assert actual_type in expected_types, (
        f"{context}: Expected error type in {expected_types}, got '{actual_type}'. "
        f"Error: {result['error']}"
    )


# =============================================================================
# Assertion Lifecycle Context Managers
# =============================================================================


@contextmanager
def managed_sql_assertion(
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
    dataset_urn: str,
    display_name: str,
    statement: str,
    criteria_condition: str = "IS_GREATER_THAN",
    criteria_parameters: Any = 0,
    schedule: str = "0 * * * *",
) -> Generator[str, None, None]:
    """Context manager for SQL assertion lifecycle.

    Creates the assertion, yields its URN, and cleans up on exit.
    """
    assertion = datahub_client.assertions.sync_sql_assertion(
        dataset_urn=dataset_urn,
        display_name=display_name,
        statement=statement,
        criteria_condition=criteria_condition,
        criteria_parameters=criteria_parameters,
        schedule=schedule,
    )
    assertion_urn = str(assertion.urn)
    logger.info(f"Created SQL assertion: {assertion_urn}")
    wait_for_assertion_sync()
    try:
        yield assertion_urn
    finally:
        cleanup_assertion(graph_client, assertion_urn)


@contextmanager
def managed_volume_assertion(
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
    dataset_urn: str,
    display_name: str,
    criteria_condition: str,
    criteria_parameters: Any,
    detection_mechanism: Any,
    schedule: str = "0 * * * *",
) -> Generator[str, None, None]:
    """Context manager for volume assertion lifecycle.

    Creates the assertion, yields its URN, and cleans up on exit.
    """
    assertion = datahub_client.assertions.sync_volume_assertion(
        dataset_urn=dataset_urn,
        display_name=display_name,
        criteria_condition=criteria_condition,
        criteria_parameters=criteria_parameters,
        detection_mechanism=detection_mechanism,
        schedule=schedule,
    )
    assertion_urn = str(assertion.urn)
    logger.info(f"Created volume assertion: {assertion_urn}")
    wait_for_assertion_sync()
    try:
        yield assertion_urn
    finally:
        cleanup_assertion(graph_client, assertion_urn)


@contextmanager
def managed_freshness_assertion(
    graph_client: DataHubGraph,
    datahub_client: DataHubClient,
    dataset_urn: str,
    display_name: str,
    lookback_window: Dict[str, Any],
    freshness_schedule_check_type: str,
    detection_mechanism: Any,
    schedule: str = "0 * * * *",
) -> Generator[str, None, None]:
    """Context manager for freshness assertion lifecycle.

    Creates the assertion, yields its URN, and cleans up on exit.
    """
    assertion = datahub_client.assertions.sync_freshness_assertion(
        dataset_urn=dataset_urn,
        display_name=display_name,
        lookback_window=lookback_window,
        freshness_schedule_check_type=freshness_schedule_check_type,
        detection_mechanism=detection_mechanism,
        schedule=schedule,
    )
    assertion_urn = str(assertion.urn)
    logger.info(f"Created freshness assertion: {assertion_urn}")
    wait_for_assertion_sync()
    try:
        yield assertion_urn
    finally:
        cleanup_assertion(graph_client, assertion_urn)


@contextmanager
def managed_assertions(
    graph_client: DataHubGraph,
    assertion_urns: List[str],
) -> Generator[List[str], None, None]:
    """Context manager for cleaning up multiple assertions.

    Takes a list of URNs and ensures cleanup on exit.
    Use this when you need to create multiple assertions manually.
    """
    try:
        yield assertion_urns
    finally:
        for urn in assertion_urns:
            cleanup_assertion(graph_client, urn)


# =============================================================================
# GraphQL Queries
# =============================================================================


# GraphQL query to retrieve assertions for a dataset
GRAPHQL_GET_ASSERTIONS = """
    query dataset($datasetUrn: String!) {
        dataset(urn: $datasetUrn) {
            assertions(start: 0, count: 1000) {
                total
                assertions {
                    urn
                    info {
                        type
                        description
                        externalUrl
                        source {
                            type
                        }
                        freshnessAssertion {
                            entityUrn
                            type
                            schedule {
                                type
                                cron {
                                    cron
                                    timezone
                                }
                                fixedInterval {
                                    unit
                                    multiple
                                }
                            }
                        }
                        volumeAssertion {
                            entityUrn
                            type
                            rowCountTotal {
                                operator
                                parameters {
                                    value {
                                        value
                                        type
                                    }
                                    minValue {
                                        value
                                        type
                                    }
                                    maxValue {
                                        value
                                        type
                                    }
                                }
                            }
                            rowCountChange {
                                type
                                operator
                                parameters {
                                    value {
                                        value
                                        type
                                    }
                                }
                            }
                        }
                        sqlAssertion {
                            entityUrn
                            statement
                            operator
                            parameters {
                                value {
                                    value
                                    type
                                }
                                minValue {
                                    value
                                    type
                                }
                                maxValue {
                                    value
                                    type
                                }
                            }
                        }
                        schemaAssertion {
                            entityUrn
                            compatibility
                            fields {
                                path
                                type
                                nativeType
                            }
                        }
                        fieldAssertion {
                            entityUrn
                            type
                            fieldValuesAssertion {
                                field {
                                    path
                                    type
                                    nativeType
                                }
                                operator
                                transform {
                                    type
                                }
                                parameters {
                                    value {
                                        value
                                        type
                                    }
                                    minValue {
                                        value
                                        type
                                    }
                                    maxValue {
                                        value
                                        type
                                    }
                                }
                                failThreshold {
                                    type
                                    value
                                }
                                excludeNulls
                            }
                            fieldMetricAssertion {
                                field {
                                    path
                                    type
                                    nativeType
                                }
                                metric
                                operator
                                parameters {
                                    value {
                                        value
                                        type
                                    }
                                    minValue {
                                        value
                                        type
                                    }
                                    maxValue {
                                        value
                                        type
                                    }
                                }
                            }
                        }
                    }
                    tags {
                        tags {
                            tag {
                                urn
                                properties {
                                    name
                                }
                            }
                        }
                    }
                    actions {
                        onSuccess {
                            type
                        }
                        onFailure {
                            type
                        }
                    }
                    monitor {
                        urn
                        info {
                            status {
                                mode
                            }
                            assertionMonitor {
                                assertions {
                                    schedule {
                                        cron
                                        timezone
                                    }
                                    parameters {
                                        type
                                        datasetFreshnessParameters {
                                            sourceType
                                            field {
                                                path
                                                type
                                                nativeType
                                                kind
                                            }
                                        }
                                        datasetVolumeParameters {
                                            sourceType
                                        }
                                        datasetFieldParameters {
                                            sourceType
                                            changedRowsField {
                                                path
                                                type
                                                nativeType
                                            }
                                        }
                                    }
                                }
                                settings {
                                    inferenceSettings {
                                        sensitivity {
                                            level
                                        }
                                        trainingDataLookbackWindowDays
                                        exclusionWindows {
                                            type
                                            displayName
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
"""


def get_assertion_by_urn(
    graph_client: DataHubGraph, dataset_urn: str, assertion_urn: str
) -> Optional[dict]:
    """Helper to fetch a specific assertion from a dataset."""
    result = graph_client.execute_graphql(
        query=GRAPHQL_GET_ASSERTIONS,
        variables={"datasetUrn": dataset_urn},
    )
    assertions = result["dataset"]["assertions"]["assertions"]
    for assertion in assertions:
        if assertion["urn"] == assertion_urn:
            return assertion
    return None
