"""
Integration tests for volume assertion SDK methods.

Tests the following methods:
- sync_volume_assertion
- sync_smart_volume_assertion
"""

from typing import Any

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import InferenceSensitivity
from acryl_datahub_cloud.sdk.assertion_input.volume_assertion_input import (
    VolumeAssertionCondition,
)

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.conftest import TEST_DATASET_URN
from tests.assertions.sdk.helpers import cleanup_assertion, get_assertion_by_urn
from tests.consistency_utils import wait_for_writes_to_sync

# Test constants for volume assertion row count thresholds
INITIAL_ROW_COUNT_THRESHOLD = 100
UPDATED_ROW_COUNT_THRESHOLD = 200


def test_sync_volume_assertion(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test create, update, and delete of volume assertion."""
    # Create volume assertion
    assertion = datahub_client.assertions.sync_volume_assertion(
        dataset_urn=TEST_DATASET_URN,
        display_name="Test Volume Assertion",
        schedule="0 * * * *",
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=INITIAL_ROW_COUNT_THRESHOLD,
    )

    assertion_urn = str(assertion.urn)
    assert assertion_urn

    wait_for_writes_to_sync()

    # Verify creation via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["type"] == "VOLUME"
    assert fetched["info"]["volumeAssertion"] is not None
    assert fetched["info"]["volumeAssertion"]["entityUrn"] == TEST_DATASET_URN

    # Verify creation values on returned assertion object
    assert (
        assertion.criteria.condition
        == VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO
    )
    assert assertion.criteria.parameters == INITIAL_ROW_COUNT_THRESHOLD
    assert str(assertion.dataset_urn) == TEST_DATASET_URN

    # Update assertion - must provide both criteria_condition and criteria_parameters
    updated = datahub_client.assertions.sync_volume_assertion(
        dataset_urn=TEST_DATASET_URN,
        urn=assertion_urn,
        criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        criteria_parameters=UPDATED_ROW_COUNT_THRESHOLD,
    )
    assert str(updated.urn) == assertion_urn

    wait_for_writes_to_sync()

    # Verify update was applied correctly
    assert updated.criteria.parameters == UPDATED_ROW_COUNT_THRESHOLD
    # Verify condition wasn't changed
    assert (
        updated.criteria.condition
        == VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO
    )
    # Verify other fields weren't corrupted
    assert str(updated.dataset_urn) == TEST_DATASET_URN
    assert updated.display_name == "Test Volume Assertion"

    # Cleanup
    cleanup_assertion(graph_client, assertion_urn)

    # Verify deletion
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is None


def test_sync_smart_volume_assertion(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test create, update, and delete of smart volume assertion."""
    # Create smart volume assertion
    assertion = datahub_client.assertions.sync_smart_volume_assertion(
        dataset_urn=TEST_DATASET_URN,
        display_name="Test Smart Volume Assertion",
        detection_mechanism="information_schema",
        sensitivity="medium",
        schedule="0 * * * *",
    )

    assertion_urn = str(assertion.urn)
    assert assertion_urn

    wait_for_writes_to_sync()

    # Verify creation via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["type"] == "VOLUME"
    assert fetched["info"]["volumeAssertion"] is not None

    # Verify creation values on returned assertion object
    assert assertion.sensitivity == InferenceSensitivity.MEDIUM
    assert str(assertion.dataset_urn) == TEST_DATASET_URN

    # Update assertion
    updated = datahub_client.assertions.sync_smart_volume_assertion(
        dataset_urn=TEST_DATASET_URN,
        urn=assertion_urn,
        sensitivity="low",
    )
    assert str(updated.urn) == assertion_urn

    wait_for_writes_to_sync()

    # Verify update was applied correctly
    assert updated.sensitivity == InferenceSensitivity.LOW
    # Verify other fields weren't corrupted
    assert str(updated.dataset_urn) == TEST_DATASET_URN
    assert updated.display_name == "Test Smart Volume Assertion"

    # Cleanup
    cleanup_assertion(graph_client, assertion_urn)

    # Verify deletion
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is None
