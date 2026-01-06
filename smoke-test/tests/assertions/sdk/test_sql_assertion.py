"""
Integration tests for SQL assertion SDK methods.

Tests the following methods:
- sync_sql_assertion
- sync_smart_sql_assertion
"""

from typing import Any

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import InferenceSensitivity
from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
)

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.conftest import TEST_DATASET_URN
from tests.assertions.sdk.helpers import cleanup_assertion, get_assertion_by_urn
from tests.consistency_utils import wait_for_writes_to_sync


def test_sync_sql_assertion(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test create, update, and delete of SQL assertion."""
    original_statement = "SELECT COUNT(*) FROM test_table WHERE status = 'active'"
    updated_statement = "SELECT COUNT(*) FROM test_table WHERE status = 'inactive'"

    # Create SQL assertion
    assertion = datahub_client.assertions.sync_sql_assertion(
        dataset_urn=TEST_DATASET_URN,
        display_name="Test SQL Assertion",
        statement=original_statement,
        criteria_condition="IS_GREATER_THAN",
        criteria_parameters=0,
        schedule="0 * * * *",
    )

    assertion_urn = str(assertion.urn)
    assert assertion_urn

    wait_for_writes_to_sync()

    # Verify creation via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["type"] == "SQL"
    assert fetched["info"]["sqlAssertion"] is not None
    assert fetched["info"]["sqlAssertion"]["entityUrn"] == TEST_DATASET_URN

    # Verify creation values on returned assertion object
    assert assertion.statement == original_statement
    assert assertion.criteria_condition == SqlAssertionCondition.IS_GREATER_THAN
    assert assertion.criteria_parameters == 0
    assert str(assertion.dataset_urn) == TEST_DATASET_URN

    # Update assertion
    updated = datahub_client.assertions.sync_sql_assertion(
        dataset_urn=TEST_DATASET_URN,
        urn=assertion_urn,
        statement=updated_statement,
    )
    assert str(updated.urn) == assertion_urn

    wait_for_writes_to_sync()

    # Verify update was applied correctly
    assert updated.statement == updated_statement
    # Verify other fields weren't corrupted
    assert updated.criteria_condition == SqlAssertionCondition.IS_GREATER_THAN
    assert updated.criteria_parameters == 0
    assert str(updated.dataset_urn) == TEST_DATASET_URN
    assert updated.display_name == "Test SQL Assertion"

    # Cleanup
    cleanup_assertion(graph_client, assertion_urn)

    # Verify deletion
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is None


def test_sync_smart_sql_assertion(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test create, update, and delete of smart SQL assertion."""
    original_statement = "SELECT COUNT(*) FROM test_table"

    # Create smart SQL assertion
    assertion = datahub_client.assertions.sync_smart_sql_assertion(
        dataset_urn=TEST_DATASET_URN,
        display_name="Test Smart SQL Assertion",
        statement=original_statement,
        sensitivity="medium",
        schedule="0 * * * *",
    )

    assertion_urn = str(assertion.urn)
    assert assertion_urn

    wait_for_writes_to_sync()

    # Verify creation via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["type"] == "SQL"
    assert fetched["info"]["sqlAssertion"] is not None

    # Verify creation values on returned assertion object
    assert assertion.statement == original_statement
    assert assertion.sensitivity == InferenceSensitivity.MEDIUM
    assert str(assertion.dataset_urn) == TEST_DATASET_URN

    # Update assertion
    updated = datahub_client.assertions.sync_smart_sql_assertion(
        dataset_urn=TEST_DATASET_URN,
        urn=assertion_urn,
        sensitivity="high",
    )
    assert str(updated.urn) == assertion_urn

    wait_for_writes_to_sync()

    # Verify update was applied correctly
    assert updated.sensitivity == InferenceSensitivity.HIGH
    # Verify other fields weren't corrupted
    assert updated.statement == original_statement
    assert str(updated.dataset_urn) == TEST_DATASET_URN
    assert updated.display_name == "Test Smart SQL Assertion"

    # Cleanup
    cleanup_assertion(graph_client, assertion_urn)

    # Verify deletion
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is None
