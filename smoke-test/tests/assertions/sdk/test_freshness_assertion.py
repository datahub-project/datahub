"""
Integration tests for freshness assertion SDK methods.

Tests the following methods:
- sync_freshness_assertion
- sync_smart_freshness_assertion
"""

import logging
from typing import Any

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import InferenceSensitivity

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


def test_freshness_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test freshness assertion with ALL optional parameters.

    Creates assertion with custom URN, tags, incident_behavior, detection_mechanism
    (last_modified_column format), and schedule to verify all fields are set correctly.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-freshness-all-params-{test_id}"
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_freshness_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params Freshness {test_id}",
            schedule="0 * * * *",
            lookback_window={"unit": "HOUR", "multiple": 2},
            freshness_schedule_check_type="fixed_interval",
            detection_mechanism={
                "type": "last_modified_column",
                "column_name": "updated_at",
            },
            incident_behavior=["raise_on_fail", "resolve_on_pass"],
            tags=["urn:li:tag:test-tag"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created freshness assertion with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "FRESHNESS"
        assert fetched["urn"] == custom_urn

        # Verify source type is NATIVE
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "NATIVE"

        # Verify freshness_schedule_check_type
        schedule_type = get_nested_value(
            fetched, "info.freshnessAssertion.schedule.type"
        )
        assert schedule_type == "FIXED_INTERVAL", (
            f"Expected schedule type FIXED_INTERVAL, got {schedule_type}"
        )

        # Verify lookback_window
        fixed_interval = get_nested_value(
            fetched, "info.freshnessAssertion.schedule.fixedInterval"
        )
        assert fixed_interval is not None, "Expected fixedInterval to be set"
        assert fixed_interval.get("unit") == "HOUR", (
            f"Expected unit HOUR, got {fixed_interval.get('unit')}"
        )
        assert fixed_interval.get("multiple") == 2, (
            f"Expected multiple 2, got {fixed_interval.get('multiple')}"
        )

        # Verify incident behavior (actions)
        actions = get_nested_value(fetched, "actions")
        assert actions is not None
        # Check that onFailure has RAISE_INCIDENT action
        on_failure_types = [a["type"] for a in actions.get("onFailure", [])]
        assert "RAISE_INCIDENT" in on_failure_types
        # Check that onSuccess has RESOLVE_INCIDENT action
        on_success_types = [a["type"] for a in actions.get("onSuccess", [])]
        assert "RESOLVE_INCIDENT" in on_success_types

        # Verify tags
        tags = get_nested_value(fetched, "tags.tags")
        assert tags is not None and len(tags) > 0, "Expected tags to be set"
        tag_urns = [t["tag"]["urn"] for t in tags]
        assert "urn:li:tag:test-tag" in tag_urns, f"Expected test-tag, got {tag_urns}"

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

        # Verify detection_mechanism via monitor params (last_modified_column)
        params = assertions_specs[0].get("parameters", {})
        source_type = get_nested_value(params, "datasetFreshnessParameters.sourceType")
        assert source_type == "FIELD_VALUE", (
            f"Expected sourceType FIELD_VALUE, got {source_type}"
        )

        # Verify the field is set with LAST_MODIFIED kind
        field = get_nested_value(params, "datasetFreshnessParameters.field")
        assert field is not None, "field should be set"
        assert "updated_at" in str(field.get("path", "")), (
            f"Expected updated_at in field path, got {field}"
        )
        assert field.get("kind") == "LAST_MODIFIED", (
            f"Expected field kind LAST_MODIFIED, got {field.get('kind')}"
        )

        # Verify SDK return object
        assert assertion.schedule.cron == "0 * * * *"
        assert str(assertion.dataset_urn) == test_data

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_smart_freshness_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test smart freshness assertion with ALL optional parameters.

    Creates assertion with custom URN, sensitivity, exclusion_windows, training_data_lookback_days,
    tags, and incident_behavior to verify all smart-specific fields.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-smart-freshness-all-params-{test_id}"
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_smart_freshness_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params Smart Freshness {test_id}",
            detection_mechanism="information_schema",
            sensitivity="high",
            exclusion_windows=[
                {
                    "start": "2025-01-04T00:00:00",
                    "end": "2025-01-05T23:59:59",
                }
            ],
            training_data_lookback_days=30,
            incident_behavior=["raise_on_fail"],
            tags=["urn:li:tag:smart-test"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created smart freshness with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "FRESHNESS"
        assert fetched["urn"] == custom_urn

        # Verify source type is INFERRED (smart assertion)
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "INFERRED"

        # Verify SDK return object
        assert assertion.sensitivity == InferenceSensitivity.HIGH
        assert str(assertion.dataset_urn) == test_data

        # Verify sensitivity is captured in monitor
        # Sensitivity levels: LOW=1, MEDIUM=5, HIGH=10
        sensitivity_level = get_nested_value(
            fetched,
            "monitor.info.assertionMonitor.settings.inferenceSettings.sensitivity.level",
        )
        assert sensitivity_level == 10

        # Verify detection_mechanism via monitor params
        assertions_specs = get_nested_value(
            fetched, "monitor.info.assertionMonitor.assertions"
        )
        assert assertions_specs and len(assertions_specs) > 0, (
            "Expected assertion specs"
        )
        params = assertions_specs[0].get("parameters", {})
        detection_source_type = get_nested_value(
            params, "datasetFreshnessParameters.sourceType"
        )
        assert detection_source_type == "INFORMATION_SCHEMA", (
            f"Expected sourceType INFORMATION_SCHEMA, got {detection_source_type}"
        )

        # Verify training_data_lookback_days
        lookback_days = get_nested_value(
            fetched,
            "monitor.info.assertionMonitor.settings.inferenceSettings.trainingDataLookbackWindowDays",
        )
        assert lookback_days == 30, f"Expected lookback days 30, got {lookback_days}"

        # Verify exclusion_windows
        exclusion_windows = get_nested_value(
            fetched,
            "monitor.info.assertionMonitor.settings.inferenceSettings.exclusionWindows",
        )
        assert exclusion_windows is not None and len(exclusion_windows) > 0, (
            "Expected exclusion windows to be set"
        )

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
        assert "urn:li:tag:smart-test" in tag_urns, (
            f"Expected smart-test tag, got {tag_urns}"
        )

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_freshness_update_merge_behavior(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test that updating only one field preserves other fields.

    This tests the merge behavior - when updating with only partial fields,
    the non-provided fields should be preserved from the existing assertion.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        # Create assertion with all fields
        assertion = datahub_client.assertions.sync_freshness_assertion(
            dataset_urn=test_data,
            display_name=f"Merge Test Freshness {test_id}",
            schedule="0 * * * *",
            lookback_window={"unit": "HOUR", "multiple": 2},
            freshness_schedule_check_type="fixed_interval",
            detection_mechanism="information_schema",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created freshness for merge test: {assertion_urn}")

        wait_for_assertion_sync()

        # Capture original values
        original_display_name = assertion.display_name
        original_dataset_urn = str(assertion.dataset_urn)

        # Update only the schedule
        updated = datahub_client.assertions.sync_freshness_assertion(
            dataset_urn=test_data,
            urn=assertion_urn,
            schedule="0 */3 * * *",  # Change from hourly to every 3 hours
        )

        wait_for_assertion_sync()

        # Verify the updated field changed via SDK return object
        assert updated.schedule.cron == "0 */3 * * *"
        assert str(updated.urn) == assertion_urn

        # Verify non-updated fields are preserved via SDK return object
        assert updated.display_name == original_display_name
        assert str(updated.dataset_urn) == original_dataset_urn

        # Verify via GraphQL that the update was persisted to the server
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None

        # Verify schedule was updated via GraphQL
        cron = get_nested_value(fetched, "info.freshnessAssertion.schedule.cron.cron")
        assert cron == "0 */3 * * *", f"Expected schedule 0 */3 * * *, got {cron}"

        # Verify other fields preserved via GraphQL
        assert fetched["info"]["type"] == "FRESHNESS"

        # Cleanup and verify deletion
        cleanup_assertion(graph_client, assertion_urn)
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is None, "Assertion should be deleted"
        assertion_urn = None  # Prevent double cleanup in finally

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)
