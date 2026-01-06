"""
Integration tests for schema assertion SDK methods.

Tests the following methods:
- sync_schema_assertion
"""

from typing import Any

from acryl_datahub_cloud.sdk.assertion_input.schema_assertion_input import (
    SchemaAssertionCompatibility,
)

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.conftest import TEST_DATASET_URN
from tests.assertions.sdk.helpers import cleanup_assertion, get_assertion_by_urn
from tests.consistency_utils import wait_for_writes_to_sync


def test_sync_schema_assertion(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test create, update, and delete of schema assertion."""
    initial_fields = [
        {"path": "id", "type": "STRING"},
        {"path": "count", "type": "NUMBER"},
    ]
    updated_fields = [
        {"path": "id", "type": "STRING"},
        {"path": "count", "type": "NUMBER"},
        {"path": "name", "type": "STRING"},
    ]

    # Create schema assertion
    assertion = datahub_client.assertions.sync_schema_assertion(
        dataset_urn=TEST_DATASET_URN,
        display_name="Test Schema Assertion",
        compatibility="EXACT_MATCH",
        fields=initial_fields,
        schedule="0 * * * *",
    )

    assertion_urn = str(assertion.urn)
    assert assertion_urn

    wait_for_writes_to_sync()

    # Verify creation via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["type"] == "DATA_SCHEMA"
    assert fetched["info"]["schemaAssertion"] is not None
    assert fetched["info"]["schemaAssertion"]["entityUrn"] == TEST_DATASET_URN
    assert fetched["info"]["schemaAssertion"]["compatibility"] == "EXACT_MATCH"

    # Verify creation values on returned assertion object
    assert assertion.compatibility == SchemaAssertionCompatibility.EXACT_MATCH
    assert len(assertion.fields) == 2
    assert str(assertion.dataset_urn) == TEST_DATASET_URN

    # Update assertion
    updated = datahub_client.assertions.sync_schema_assertion(
        dataset_urn=TEST_DATASET_URN,
        urn=assertion_urn,
        compatibility="SUPERSET",
        fields=updated_fields,
    )
    assert str(updated.urn) == assertion_urn

    wait_for_writes_to_sync()

    # Verify update via GraphQL
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is not None
    assert fetched["info"]["schemaAssertion"]["compatibility"] == "SUPERSET"

    # Verify update was applied correctly on returned assertion object
    assert updated.compatibility == SchemaAssertionCompatibility.SUPERSET
    assert len(updated.fields) == 3
    # Verify field paths are correct
    field_paths = [f.path for f in updated.fields]
    assert "id" in field_paths
    assert "count" in field_paths
    assert "name" in field_paths
    # Verify other fields weren't corrupted
    assert str(updated.dataset_urn) == TEST_DATASET_URN
    assert updated.display_name == "Test Schema Assertion"

    # Cleanup
    cleanup_assertion(graph_client, assertion_urn)

    # Verify deletion
    fetched = get_assertion_by_urn(graph_client, TEST_DATASET_URN, assertion_urn)
    assert fetched is None
