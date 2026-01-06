"""
Integration tests for freshness assertion SDK methods.

Tests the following methods:
- sync_freshness_assertion
- sync_smart_freshness_assertion
"""

from typing import Any

from acryl_datahub_cloud.sdk.assertion.assertion_base import AssertionMode
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import InferenceSensitivity

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.conftest import TEST_DATASET_URN
from tests.assertions.sdk.helpers import cleanup_assertion, get_assertion_by_urn
from tests.consistency_utils import wait_for_writes_to_sync


def test_sync_freshness_assertion(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test create, update, and delete of freshness assertion."""
    # Create freshness assertion
    assertion = datahub_client.assertions.sync_freshness_assertion(
        dataset_urn=TEST_DATASET_URN,
        display_name="Test Freshness Assertion",
        schedule="0 * * * *",
        lookback_window={"unit": "HOUR", "multiple": 1},
        freshness_schedule_check_type="fixed_interval",
    )

    assertion_urn = str(assertion.urn)
    assert assertion_urn

    wait_for_writes_to_sync()

    # Verify creation via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["type"] == "FRESHNESS"
    assert fetched["info"]["freshnessAssertion"] is not None
    assert fetched["info"]["freshnessAssertion"]["entityUrn"] == TEST_DATASET_URN

    # Verify creation values on returned assertion object
    assert assertion.mode == AssertionMode.ACTIVE
    assert assertion.schedule.cron == "0 * * * *"
    assert str(assertion.dataset_urn) == TEST_DATASET_URN

    # Update assertion
    updated = datahub_client.assertions.sync_freshness_assertion(
        dataset_urn=TEST_DATASET_URN,
        urn=assertion_urn,
        enabled=False,
        schedule="0 */2 * * *",
    )
    assert str(updated.urn) == assertion_urn

    wait_for_writes_to_sync()

    # Verify update was applied correctly
    assert updated.mode == AssertionMode.INACTIVE
    assert updated.schedule.cron == "0 */2 * * *"
    # Verify other fields weren't corrupted
    assert str(updated.dataset_urn) == TEST_DATASET_URN
    assert updated.display_name == "Test Freshness Assertion"

    # Cleanup
    cleanup_assertion(graph_client, assertion_urn)

    # Verify deletion
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is None


def test_sync_smart_freshness_assertion(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test create, update, and delete of smart freshness assertion."""
    # Create smart freshness assertion
    assertion = datahub_client.assertions.sync_smart_freshness_assertion(
        dataset_urn=TEST_DATASET_URN,
        display_name="Test Smart Freshness Assertion",
        detection_mechanism="information_schema",
        sensitivity="medium",
    )

    assertion_urn = str(assertion.urn)
    assert assertion_urn

    wait_for_writes_to_sync()

    # Verify creation via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["type"] == "FRESHNESS"
    assert fetched["info"]["freshnessAssertion"] is not None

    # Verify creation values on returned assertion object
    assert assertion.sensitivity == InferenceSensitivity.MEDIUM
    assert str(assertion.dataset_urn) == TEST_DATASET_URN

    # Update assertion
    updated = datahub_client.assertions.sync_smart_freshness_assertion(
        dataset_urn=TEST_DATASET_URN,
        urn=assertion_urn,
        sensitivity="high",
    )
    assert str(updated.urn) == assertion_urn

    wait_for_writes_to_sync()

    # Verify update was applied correctly
    assert updated.sensitivity == InferenceSensitivity.HIGH
    # Verify other fields weren't corrupted
    assert str(updated.dataset_urn) == TEST_DATASET_URN
    assert updated.display_name == "Test Smart Freshness Assertion"

    # Cleanup
    cleanup_assertion(graph_client, assertion_urn)

    # Verify deletion
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is None
