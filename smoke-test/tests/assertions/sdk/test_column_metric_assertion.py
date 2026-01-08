"""
Integration tests for column metric assertion SDK methods.

Tests the following methods:
- sync_column_metric_assertion
- sync_smart_column_metric_assertion
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


def test_column_metric_between_operator(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test column metric assertion with BETWEEN operator.

    Tests BETWEEN with tuple params (min, max).
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_column_metric_assertion(
            dataset_urn=test_data,
            display_name=f"Mean Range Check {test_id}",
            column_name="price",
            metric_type="MEAN",
            operator="BETWEEN",
            criteria_parameters=(10, 100),  # Mean between 10 and 100
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created BETWEEN column metric assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "FIELD"

        # Verify operator and parameters
        operator = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.operator"
        )
        assert operator == "BETWEEN"

        min_value = get_nested_value(
            fetched,
            "info.fieldAssertion.fieldMetricAssertion.parameters.minValue.value",
        )
        max_value = get_nested_value(
            fetched,
            "info.fieldAssertion.fieldMetricAssertion.parameters.maxValue.value",
        )
        assert min_value == "10"
        assert max_value == "100"

        # Verify SDK return object
        assert assertion.column_name == "price"

        # Verify monitor was created (using nested data from GraphQL response)
        monitor = fetched.get("monitor")
        assert monitor is not None and monitor.get("urn"), "Monitor should exist"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_column_metric_in_operator(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test column metric assertion with IN operator.

    Tests IN with list params - different param pattern.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_column_metric_assertion(
            dataset_urn=test_data,
            display_name=f"Unique Count In Range {test_id}",
            column_name="user_id",
            metric_type="UNIQUE_COUNT",
            operator="IN",
            criteria_parameters=[
                1,
                5,
                10,
                50,
                100,
            ],  # Unique count must be one of these
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created IN column metric assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "FIELD"

        # Verify operator and parameters
        operator = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.operator"
        )
        assert operator == "IN"

        # For IN operator, verify the parameters contain the expected values
        params = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.parameters"
        )
        assert params is not None

        # IN operator stores values in the 'value' field as a JSON-encoded list
        param_value = get_nested_value(params, "value.value")
        assert param_value is not None, "IN operator should have value parameter"
        # Verify the expected values are present (as strings in the API response)
        expected_values = {"1", "5", "10", "50", "100"}
        # The value may be a JSON string like "[1, 5, 10, 50, 100]" or individual values
        # Check that the expected values appear in the parameter
        for val in expected_values:
            assert val in str(param_value), (
                f"Expected value {val} not found in params: {param_value}"
            )

        # Verify SDK return object
        assert assertion.column_name == "user_id"

        # Verify monitor was created (using nested data from GraphQL response)
        monitor = fetched.get("monitor")
        assert monitor is not None and monitor.get("urn"), "Monitor should exist"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_column_metric_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test column metric assertion with ALL optional parameters.

    Creates assertion with custom URN, metric_type, operator, params, detection_mechanism
    (changed_rows_query format), tags, incident_behavior, and schedule.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-col-metric-all-params-{test_id}"
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_column_metric_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params Column Metric {test_id}",
            column_name="quantity",
            metric_type="MIN",
            operator="GREATER_THAN",
            criteria_parameters=0,
            schedule="0 * * * *",
            detection_mechanism={
                "type": "changed_rows_query",
                "column_name": "updated_at",
            },
            incident_behavior=["raise_on_fail"],
            tags=["urn:li:tag:column-metric-test"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created column metric assertion with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "FIELD"
        assert fetched["urn"] == custom_urn

        # Verify source type is NATIVE (not INFERRED)
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "NATIVE"

        # Verify column_name
        field_path = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.field.path"
        )
        assert "quantity" in str(field_path), (
            f"Expected quantity in field path, got {field_path}"
        )

        # Verify metric_type
        metric = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.metric"
        )
        assert metric == "MIN", f"Expected metric MIN, got {metric}"

        # Verify operator
        operator = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.operator"
        )
        assert operator == "GREATER_THAN", (
            f"Expected operator GREATER_THAN, got {operator}"
        )

        # Verify criteria_parameters
        params_value = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.parameters.value.value"
        )
        assert params_value == "0", f"Expected params value 0, got {params_value}"

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
        assert "urn:li:tag:column-metric-test" in tag_urns, (
            f"Expected column-metric-test tag, got {tag_urns}"
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

        # Verify detection_mechanism via monitor params (changed_rows_query)
        params = assertions_specs[0].get("parameters", {})
        detection_source_type = get_nested_value(
            params, "datasetFieldParameters.sourceType"
        )
        assert detection_source_type == "CHANGED_ROWS_QUERY", (
            f"Expected sourceType CHANGED_ROWS_QUERY, got {detection_source_type}"
        )

        # Verify the changedRowsField contains the column name we specified
        changed_rows_field = get_nested_value(
            params, "datasetFieldParameters.changedRowsField"
        )
        assert changed_rows_field is not None, "changedRowsField should be set"
        assert "updated_at" in str(changed_rows_field.get("path", "")), (
            f"Expected updated_at in changedRowsField path, got {changed_rows_field}"
        )

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_smart_column_metric_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test smart column metric assertion with ALL optional parameters.

    Creates assertion with custom URN, metric_type, sensitivity, exclusion_windows,
    training_data_lookback_days, and tags.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-smart-col-metric-all-params-{test_id}"
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_smart_column_metric_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params Smart Column Metric {test_id}",
            column_name="price",
            metric_type="MEAN",
            sensitivity="low",
            schedule="0 * * * *",
            exclusion_windows=[
                {
                    "start": "2025-01-01T00:00:00",
                    "end": "2025-01-02T00:00:00",
                }
            ],
            training_data_lookback_days=28,
            tags=["urn:li:tag:smart-column-metric-test"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created smart column metric with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "FIELD"
        assert fetched["urn"] == custom_urn

        # Verify source type is INFERRED (smart assertion)
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "INFERRED"

        # Verify SDK return object
        assert assertion.sensitivity == InferenceSensitivity.LOW

        # Verify column_name
        field_path = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.field.path"
        )
        assert "price" in str(field_path), (
            f"Expected price in field path, got {field_path}"
        )

        # Verify metric_type
        metric = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.metric"
        )
        assert metric == "MEAN", f"Expected metric MEAN, got {metric}"

        # Verify sensitivity is captured in monitor
        # Sensitivity levels: LOW=1, MEDIUM=5, HIGH=10
        sensitivity_level = get_nested_value(
            fetched,
            "monitor.info.assertionMonitor.settings.inferenceSettings.sensitivity.level",
        )
        assert sensitivity_level == 1

        # Verify training_data_lookback_days
        lookback_days = get_nested_value(
            fetched,
            "monitor.info.assertionMonitor.settings.inferenceSettings.trainingDataLookbackWindowDays",
        )
        assert lookback_days == 28, f"Expected lookback days 28, got {lookback_days}"

        # Verify exclusion_windows
        exclusion_windows = get_nested_value(
            fetched,
            "monitor.info.assertionMonitor.settings.inferenceSettings.exclusionWindows",
        )
        assert exclusion_windows is not None and len(exclusion_windows) > 0, (
            "Expected exclusion windows to be set"
        )

        # Verify tags
        tags = get_nested_value(fetched, "tags.tags")
        assert tags is not None and len(tags) > 0, "Expected tags to be set"
        tag_urns = [t["tag"]["urn"] for t in tags]
        assert "urn:li:tag:smart-column-metric-test" in tag_urns, (
            f"Expected smart-column-metric-test tag, got {tag_urns}"
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


def test_column_metric_update_merge_behavior(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test that updating operator/params preserves column_name.

    This tests the merge behavior for column metric assertions.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        # Create assertion with specific column
        assertion = datahub_client.assertions.sync_column_metric_assertion(
            dataset_urn=test_data,
            display_name=f"Merge Test Column Metric {test_id}",
            column_name="price",
            metric_type="NULL_COUNT",
            operator="LESS_THAN",
            criteria_parameters=10,
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created column metric for merge test: {assertion_urn}")

        wait_for_assertion_sync()

        # Capture original values
        original_column_name = assertion.column_name
        original_display_name = assertion.display_name

        # Update only the criteria parameters
        updated = datahub_client.assertions.sync_column_metric_assertion(
            dataset_urn=test_data,
            urn=assertion_urn,
            operator="LESS_THAN",
            criteria_parameters=50,  # Change from 10 to 50
        )

        wait_for_assertion_sync()

        # Verify the criteria changed via SDK return object
        assert float(updated.criteria_parameters) == 50
        assert str(updated.urn) == assertion_urn

        # Verify column_name is preserved via SDK return object
        assert updated.column_name == original_column_name
        assert updated.display_name == original_display_name

        # Verify via GraphQL that the update was persisted to the server
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None

        # Verify criteria_parameters was updated via GraphQL
        params_value = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.parameters.value.value"
        )
        assert params_value == "50", f"Expected params value 50, got {params_value}"

        # Verify column_name is preserved via GraphQL
        field_path = get_nested_value(
            fetched, "info.fieldAssertion.fieldMetricAssertion.field.path"
        )
        assert "price" in field_path, (
            f"Expected field path to contain price, got {field_path}"
        )

        # Cleanup and verify deletion
        cleanup_assertion(graph_client, assertion_urn)
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is None, "Assertion should be deleted"
        assertion_urn = None  # Prevent double cleanup in finally

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)
