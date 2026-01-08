"""
Integration tests for schema assertion SDK methods.

Tests the following methods:
- sync_schema_assertion
"""

import logging
from typing import Any

from acryl_datahub_cloud.sdk.assertion_input.schema_assertion_input import (
    SchemaAssertionCompatibility,
)

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from tests.assertions.sdk.helpers import (
    cleanup_assertion,
    generate_unique_test_id,
    get_assertion_by_urn,
    get_nested_value,
    wait_for_assertion_sync,
)

logger = logging.getLogger(__name__)


def test_schema_superset_compatibility(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test schema assertion with SUPERSET compatibility mode.

    SUPERSET means the actual schema can have more fields than the expected schema.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        fields = [
            {"path": "id", "type": "STRING"},
            {"path": "name", "type": "STRING"},
        ]

        assertion = datahub_client.assertions.sync_schema_assertion(
            dataset_urn=test_data,
            display_name=f"Superset Schema Check {test_id}",
            compatibility="SUPERSET",
            fields=fields,
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created SUPERSET schema assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "DATA_SCHEMA"
        assert fetched["info"]["schemaAssertion"]["compatibility"] == "SUPERSET"

        # Verify SDK return object
        assert assertion.compatibility == SchemaAssertionCompatibility.SUPERSET

        # Verify monitor was created (using nested data from GraphQL response)
        monitor = fetched.get("monitor")
        assert monitor is not None and monitor.get("urn"), "Monitor should exist"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_schema_subset_compatibility(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test schema assertion with SUBSET compatibility mode.

    SUBSET means the actual schema can be a subset of the expected schema.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        fields = [
            {"path": "id", "type": "STRING"},
            {"path": "name", "type": "STRING"},
            {"path": "optional_field", "type": "STRING"},
        ]

        assertion = datahub_client.assertions.sync_schema_assertion(
            dataset_urn=test_data,
            display_name=f"Subset Schema Check {test_id}",
            compatibility="SUBSET",
            fields=fields,
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created SUBSET schema assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["schemaAssertion"]["compatibility"] == "SUBSET"

        # Verify SDK return object
        assert assertion.compatibility == SchemaAssertionCompatibility.SUBSET

        # Verify monitor was created (using nested data from GraphQL response)
        monitor = fetched.get("monitor")
        assert monitor is not None and monitor.get("urn"), "Monitor should exist"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_schema_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test schema assertion with ALL optional parameters.

    Creates assertion with custom URN, compatibility, fields with native_type, tags,
    incident_behavior, and schedule.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-schema-all-params-{test_id}"
    assertion_urn = None

    try:
        fields = [
            {"path": "user_id", "type": "STRING", "native_type": "VARCHAR(255)"},
            {"path": "price", "type": "NUMBER", "native_type": "DECIMAL(10,2)"},
            {"path": "created_at", "type": "DATE", "native_type": "TIMESTAMP"},
        ]

        assertion = datahub_client.assertions.sync_schema_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params Schema {test_id}",
            compatibility="EXACT_MATCH",
            fields=fields,
            schedule="0 * * * *",
            incident_behavior=["raise_on_fail"],
            tags=["urn:li:tag:schema-test"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created schema assertion with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "DATA_SCHEMA"
        assert fetched["urn"] == custom_urn

        # Verify compatibility
        compatibility = fetched["info"]["schemaAssertion"]["compatibility"]
        assert compatibility == "EXACT_MATCH", (
            f"Expected compatibility EXACT_MATCH, got {compatibility}"
        )

        # Verify fields count and paths
        schema_fields = fetched["info"]["schemaAssertion"]["fields"]
        assert len(schema_fields) == 3, f"Expected 3 fields, got {len(schema_fields)}"
        field_paths = [f["path"] for f in schema_fields]
        assert "user_id" in field_paths, "Expected user_id field"
        assert "price" in field_paths, "Expected price field"
        assert "created_at" in field_paths, "Expected created_at field"

        # Verify native types are set
        native_types = [f.get("nativeType") for f in schema_fields]
        assert "VARCHAR(255)" in native_types
        assert "DECIMAL(10,2)" in native_types
        assert "TIMESTAMP" in native_types

        # Verify source type is NATIVE
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "NATIVE"

        # Verify incident_behavior (actions)
        actions = get_nested_value(fetched, "actions")
        assert actions is not None, "actions should be set"
        on_failure_types = [a["type"] for a in actions.get("onFailure", [])]
        assert "RAISE_INCIDENT" in on_failure_types, (
            "Expected RAISE_INCIDENT in onFailure"
        )

        # Verify tags
        tags = get_nested_value(fetched, "tags.tags")
        assert tags is not None and len(tags) > 0, "Expected tags to be set"
        tag_urns = [t["tag"]["urn"] for t in tags]
        assert "urn:li:tag:schema-test" in tag_urns, (
            f"Expected schema-test tag, got {tag_urns}"
        )

        # Verify schedule is set in monitor
        assertions_specs = get_nested_value(
            fetched, "monitor.info.assertionMonitor.assertions"
        )
        assert assertions_specs and len(assertions_specs) > 0, (
            "Expected assertion specs"
        )
        schedule_cron = assertions_specs[0].get("schedule", {}).get("cron")
        assert schedule_cron == "0 * * * *", (
            f"Expected schedule 0 * * * *, got {schedule_cron}"
        )

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_schema_update_merge_behavior(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test that updating fields list preserves compatibility.

    This tests the merge behavior for schema assertions.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        initial_fields = [
            {"path": "id", "type": "STRING"},
        ]

        # Create assertion with EXACT_MATCH compatibility
        assertion = datahub_client.assertions.sync_schema_assertion(
            dataset_urn=test_data,
            display_name=f"Merge Test Schema {test_id}",
            compatibility="EXACT_MATCH",
            fields=initial_fields,
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created schema for merge test: {assertion_urn}")

        wait_for_assertion_sync()

        # Capture original values
        original_compatibility = assertion.compatibility
        original_display_name = assertion.display_name

        # Update only the fields
        updated_fields = [
            {"path": "id", "type": "STRING"},
            {"path": "name", "type": "STRING"},
        ]

        updated = datahub_client.assertions.sync_schema_assertion(
            dataset_urn=test_data,
            urn=assertion_urn,
            fields=updated_fields,
        )

        wait_for_assertion_sync()

        # Verify the fields changed via SDK return object
        assert len(updated.fields) == 2
        assert str(updated.urn) == assertion_urn

        # Verify compatibility is preserved via SDK return object
        assert updated.compatibility == original_compatibility
        assert updated.display_name == original_display_name

        # Verify via GraphQL that the update was persisted to the server
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None

        # Verify fields were updated via GraphQL
        schema_fields = fetched["info"]["schemaAssertion"]["fields"]
        assert len(schema_fields) == 2, f"Expected 2 fields, got {len(schema_fields)}"
        field_paths = [f["path"] for f in schema_fields]
        assert "id" in field_paths, "Expected 'id' field in schema"
        assert "name" in field_paths, "Expected 'name' field in schema"

        # Verify compatibility is preserved via GraphQL
        compatibility = fetched["info"]["schemaAssertion"]["compatibility"]
        assert compatibility == "EXACT_MATCH", (
            f"Expected compatibility EXACT_MATCH, got {compatibility}"
        )

        # Cleanup and verify deletion
        cleanup_assertion(graph_client, assertion_urn)
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is None, "Assertion should be deleted"
        assertion_urn = None  # Prevent double cleanup in finally

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)
