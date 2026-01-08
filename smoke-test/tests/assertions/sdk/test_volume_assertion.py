"""
Integration tests for volume assertion SDK methods.

Tests the following methods:
- sync_volume_assertion
- sync_smart_volume_assertion
"""

import logging
from typing import Any

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import InferenceSensitivity
from acryl_datahub_cloud.sdk.assertion_input.volume_assertion_input import (
    VolumeAssertionCondition,
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


def test_volume_row_count_within_range(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test volume assertion with range-based condition.

    Tests ROW_COUNT_IS_WITHIN_A_RANGE with tuple params (min, max).
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_volume_assertion(
            dataset_urn=test_data,
            display_name=f"Row Count Range Check {test_id}",
            schedule="0 * * * *",
            criteria_condition="ROW_COUNT_IS_WITHIN_A_RANGE",
            criteria_parameters=(100, 10000),  # Between 100 and 10,000 rows
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created range-based volume assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "VOLUME"
        assert (
            fetched["info"]["volumeAssertion"]["rowCountTotal"]["operator"] == "BETWEEN"
        )

        # Verify criteria_parameters (100, 10000) via GraphQL
        min_value = get_nested_value(
            fetched,
            "info.volumeAssertion.rowCountTotal.parameters.minValue.value",
        )
        max_value = get_nested_value(
            fetched,
            "info.volumeAssertion.rowCountTotal.parameters.maxValue.value",
        )
        assert min_value == "100", f"Expected minValue 100, got {min_value}"
        assert max_value == "10000", f"Expected maxValue 10000, got {max_value}"

        # Verify SDK return object
        assert (
            assertion.criteria.condition
            == VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE
        )
        assert str(assertion.dataset_urn) == test_data

        # Verify monitor was created (using nested data from GraphQL response)
        monitor = fetched.get("monitor")
        assert monitor is not None and monitor.get("urn"), "Monitor should exist"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_volume_row_count_growth_percentage(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test volume assertion with growth-based condition.

    Tests ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE - different condition type.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_volume_assertion(
            dataset_urn=test_data,
            display_name=f"Row Growth Percentage Check {test_id}",
            schedule="0 * * * *",
            criteria_condition="ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE",
            criteria_parameters=50,  # Row count grows by at most 50%
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created growth-based volume assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "VOLUME"
        # Growth assertions use rowCountChange instead of rowCountTotal
        assert fetched["info"]["volumeAssertion"]["rowCountChange"] is not None

        # Verify SDK return object
        assert (
            assertion.criteria.condition
            == VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE
        )

        # Verify monitor was created (using nested data from GraphQL response)
        monitor = fetched.get("monitor")
        assert monitor is not None and monitor.get("urn"), "Monitor should exist"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_volume_detection_mechanism_query_with_filter(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test volume assertion with query detection mechanism and filter.

    Uses query detection with additional_filter for more precise volume tracking.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_volume_assertion(
            dataset_urn=test_data,
            display_name=f"Volume Query with Filter {test_id}",
            schedule="0 * * * *",
            criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
            criteria_parameters=100,
            detection_mechanism={
                "type": "query",
                "additional_filter": "status = 'active'",
            },
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created volume assertion with query filter: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "VOLUME"

        # Verify detection mechanism is set correctly in the monitor's parameters
        assertions_specs = get_nested_value(
            fetched, "monitor.info.assertionMonitor.assertions"
        )
        assert assertions_specs and len(assertions_specs) > 0, (
            "Expected assertion specs"
        )
        params = assertions_specs[0].get("parameters", {})
        source_type = get_nested_value(params, "datasetVolumeParameters.sourceType")
        assert source_type == "QUERY", f"Expected sourceType QUERY, got {source_type}"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_volume_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test volume assertion with ALL optional parameters.

    Creates assertion with custom URN, criteria, detection_mechanism,
    tags, incident_behavior, and schedule.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-volume-all-params-{test_id}"
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_volume_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params Volume {test_id}",
            schedule="0 * * * *",
            criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
            criteria_parameters=100,
            detection_mechanism="query",
            incident_behavior=["raise_on_fail"],
            tags=["urn:li:tag:volume-all-params-test"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created volume assertion with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "VOLUME"
        assert fetched["urn"] == custom_urn

        # Verify source type is NATIVE (not INFERRED)
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "NATIVE"

        # Verify criteria via rowCountTotal
        row_count_total = get_nested_value(
            fetched, "info.volumeAssertion.rowCountTotal"
        )
        assert row_count_total is not None
        assert row_count_total.get("operator") == "GREATER_THAN_OR_EQUAL_TO"

        # Verify criteria_parameters
        params_value = get_nested_value(
            fetched, "info.volumeAssertion.rowCountTotal.parameters.value.value"
        )
        assert params_value == "100", f"Expected params value 100, got {params_value}"

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
        assert "urn:li:tag:volume-all-params-test" in tag_urns, (
            f"Expected volume-all-params-test tag, got {tag_urns}"
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

        # Verify detection_mechanism via monitor params
        params = assertions_specs[0].get("parameters", {})
        detection_source_type = get_nested_value(
            params, "datasetVolumeParameters.sourceType"
        )
        assert detection_source_type == "QUERY", (
            f"Expected sourceType QUERY, got {detection_source_type}"
        )

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_smart_volume_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test smart volume assertion with ALL optional parameters.

    Creates assertion with custom URN, sensitivity, exclusion_windows, detection_mechanism,
    tags, incident_behavior, and schedule.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-smart-volume-all-params-{test_id}"
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_smart_volume_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params Smart Volume {test_id}",
            detection_mechanism="query",
            sensitivity="high",
            schedule="0 * * * *",
            exclusion_windows=[
                {
                    "start": "2025-01-01T00:00:00",
                    "end": "2025-01-01T23:59:59",
                }
            ],
            training_data_lookback_days=14,
            incident_behavior=["raise_on_fail"],
            tags=["urn:li:tag:volume-test"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created smart volume with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "VOLUME"
        assert fetched["urn"] == custom_urn

        # Verify source type is INFERRED (smart assertion)
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "INFERRED"

        # Verify smart volume assertions use ROW_COUNT_TOTAL type (not ROW_COUNT_CHANGE)
        # Note: Smart assertions don't populate rowCountTotal field (which contains
        # explicit thresholds) - they use inferred thresholds instead.
        volume_assertion_type = get_nested_value(fetched, "info.volumeAssertion.type")
        assert volume_assertion_type == "ROW_COUNT_TOTAL", (
            f"Smart volume assertion should have type ROW_COUNT_TOTAL, got {volume_assertion_type}"
        )

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
            params, "datasetVolumeParameters.sourceType"
        )
        assert detection_source_type == "QUERY", (
            f"Expected sourceType QUERY, got {detection_source_type}"
        )

        # Verify training_data_lookback_days
        lookback_days = get_nested_value(
            fetched,
            "monitor.info.assertionMonitor.settings.inferenceSettings.trainingDataLookbackWindowDays",
        )
        assert lookback_days == 14, f"Expected lookback days 14, got {lookback_days}"

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
        assert "urn:li:tag:volume-test" in tag_urns, (
            f"Expected volume-test tag, got {tag_urns}"
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


def test_volume_update_merge_behavior(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test that updating criteria preserves detection_mechanism.

    This tests the merge behavior for volume assertions.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        # Create assertion with detection mechanism
        assertion = datahub_client.assertions.sync_volume_assertion(
            dataset_urn=test_data,
            display_name=f"Merge Test Volume {test_id}",
            schedule="0 * * * *",
            criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
            criteria_parameters=100,
            detection_mechanism="query",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created volume for merge test: {assertion_urn}")

        wait_for_assertion_sync()

        # Capture original values
        original_display_name = assertion.display_name
        original_dataset_urn = str(assertion.dataset_urn)

        # Update only the criteria parameters
        updated = datahub_client.assertions.sync_volume_assertion(
            dataset_urn=test_data,
            urn=assertion_urn,
            criteria_condition="ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
            criteria_parameters=500,  # Change from 100 to 500
        )

        wait_for_assertion_sync()

        # Verify the updated field changed via SDK return object
        assert updated.criteria.parameters == 500
        assert str(updated.urn) == assertion_urn

        # Verify non-updated fields are preserved via SDK return object
        assert updated.display_name == original_display_name
        assert str(updated.dataset_urn) == original_dataset_urn

        # Verify via GraphQL that the update was persisted to the server
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None

        # Verify criteria_parameters was updated via GraphQL
        params_value = get_nested_value(
            fetched, "info.volumeAssertion.rowCountTotal.parameters.value.value"
        )
        assert params_value == "500", f"Expected params value 500, got {params_value}"

        # Verify detection mechanism is preserved via GraphQL
        assertions_specs = get_nested_value(
            fetched, "monitor.info.assertionMonitor.assertions"
        )
        assert assertions_specs and len(assertions_specs) > 0, (
            "Expected assertion specs"
        )
        params = assertions_specs[0].get("parameters", {})
        source_type = get_nested_value(params, "datasetVolumeParameters.sourceType")
        assert source_type == "QUERY", (
            f"Expected sourceType QUERY (preserved), got {source_type}"
        )

        # Cleanup and verify deletion
        cleanup_assertion(graph_client, assertion_urn)
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is None, "Assertion should be deleted"
        assertion_urn = None  # Prevent double cleanup in finally

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)
