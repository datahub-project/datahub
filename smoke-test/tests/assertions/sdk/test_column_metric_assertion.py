"""
Integration tests for column metric assertion SDK methods.

Tests the following methods:
- sync_column_metric_assertion
- sync_smart_column_metric_assertion
"""

from typing import Any

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import InferenceSensitivity

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.conftest import TEST_DATASET_URN
from tests.assertions.sdk.helpers import cleanup_assertion, get_assertion_by_urn
from tests.consistency_utils import wait_for_writes_to_sync

# Test constants for column metric assertion thresholds
INITIAL_THRESHOLD = 10
UPDATED_THRESHOLD = 20


def test_sync_column_metric_assertion(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test create, update, and delete of column metric assertion."""
    # Create column metric assertion
    assertion = datahub_client.assertions.sync_column_metric_assertion(
        dataset_urn=TEST_DATASET_URN,
        display_name="Test Column Metric Assertion",
        column_name="user_id",
        metric_type="NULL_COUNT",
        operator="LESS_THAN",
        criteria_parameters=INITIAL_THRESHOLD,
        schedule="0 * * * *",
    )

    assertion_urn = str(assertion.urn)
    assert assertion_urn

    wait_for_writes_to_sync()

    # Verify creation via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["type"] == "FIELD"
    assert fetched["info"]["fieldAssertion"] is not None
    assert fetched["info"]["fieldAssertion"]["entityUrn"] == TEST_DATASET_URN

    # Verify creation values on returned assertion object
    assert assertion.column_name == "user_id"
    # GraphQL serialization may return numeric values as strings; cast to float for comparison
    assert float(assertion.criteria_parameters) == INITIAL_THRESHOLD
    assert str(assertion.dataset_urn) == TEST_DATASET_URN

    # Update assertion - update the operator and criteria parameters together
    # Note: LESS_THAN is allowed for NULL_COUNT on STRING columns
    updated = datahub_client.assertions.sync_column_metric_assertion(
        dataset_urn=TEST_DATASET_URN,
        urn=assertion_urn,
        operator="LESS_THAN",
        criteria_parameters=UPDATED_THRESHOLD,
    )
    assert str(updated.urn) == assertion_urn

    wait_for_writes_to_sync()

    # Verify update was applied correctly
    # GraphQL serialization may return numeric values as strings; cast to float for comparison
    assert float(updated.criteria_parameters) == UPDATED_THRESHOLD
    # Verify other fields weren't corrupted
    assert updated.column_name == "user_id"
    assert str(updated.dataset_urn) == TEST_DATASET_URN
    assert updated.display_name == "Test Column Metric Assertion"

    # Cleanup
    cleanup_assertion(graph_client, assertion_urn)

    # Verify deletion
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is None


def test_sync_smart_column_metric_assertion(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test create, update, and delete of smart column metric assertion."""
    # Create smart column metric assertion
    assertion = datahub_client.assertions.sync_smart_column_metric_assertion(
        dataset_urn=TEST_DATASET_URN,
        display_name="Test Smart Column Metric Assertion",
        column_name="price",
        metric_type="MEAN",
        sensitivity="medium",
        schedule="0 * * * *",
    )

    assertion_urn = str(assertion.urn)
    assert assertion_urn

    wait_for_writes_to_sync()

    # Verify creation via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["type"] == "FIELD"
    assert fetched["info"]["fieldAssertion"] is not None

    # Verify creation values on returned assertion object
    assert assertion.column_name == "price"
    assert assertion.sensitivity == InferenceSensitivity.MEDIUM
    assert str(assertion.dataset_urn) == TEST_DATASET_URN

    # Update assertion
    updated = datahub_client.assertions.sync_smart_column_metric_assertion(
        dataset_urn=TEST_DATASET_URN,
        urn=assertion_urn,
        sensitivity="low",
    )
    assert str(updated.urn) == assertion_urn

    wait_for_writes_to_sync()

    # Verify update was applied correctly
    assert updated.sensitivity == InferenceSensitivity.LOW
    # Verify other fields weren't corrupted
    assert updated.column_name == "price"
    assert str(updated.dataset_urn) == TEST_DATASET_URN
    assert updated.display_name == "Test Smart Column Metric Assertion"

    # Cleanup
    cleanup_assertion(graph_client, assertion_urn)

    # Verify deletion
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is None
