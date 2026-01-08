"""
Integration tests for SQL assertion SDK methods.

Tests the following methods:
- sync_sql_assertion
- sync_smart_sql_assertion
"""

import logging
from typing import Any

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import InferenceSensitivity
from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
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


def test_sql_condition_within_range(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test SQL assertion with range-based condition.

    Tests IS_WITHIN_A_RANGE with tuple params (min, max).
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_sql_assertion(
            dataset_urn=test_data,
            display_name=f"SQL Range Check {test_id}",
            statement="SELECT AVG(price) FROM test_table",
            criteria_condition="IS_WITHIN_A_RANGE",
            criteria_parameters=(10, 100),  # Between 10 and 100
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created range-based SQL assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "SQL"
        assert fetched["info"]["sqlAssertion"]["operator"] == "BETWEEN"

        # Verify criteria_parameters (10, 100) via GraphQL
        min_value = get_nested_value(
            fetched, "info.sqlAssertion.parameters.minValue.value"
        )
        max_value = get_nested_value(
            fetched, "info.sqlAssertion.parameters.maxValue.value"
        )
        assert min_value == "10", f"Expected minValue 10, got {min_value}"
        assert max_value == "100", f"Expected maxValue 100, got {max_value}"

        # Verify SDK return object
        assert assertion.criteria_condition == SqlAssertionCondition.IS_WITHIN_A_RANGE

        # Verify monitor was created (using nested data from GraphQL response)
        monitor = fetched.get("monitor")
        assert monitor is not None and monitor.get("urn"), "Monitor should exist"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_sql_condition_grows_percentage(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test SQL assertion with growth-based condition.

    Tests GROWS_AT_MOST_PERCENTAGE - different condition type.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_sql_assertion(
            dataset_urn=test_data,
            display_name=f"SQL Growth Check {test_id}",
            statement="SELECT SUM(quantity) FROM test_table",
            criteria_condition="GROWS_AT_MOST_PERCENTAGE",
            criteria_parameters=50,  # Grows by at most 50%
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created growth-based SQL assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "SQL"

        # Verify SDK return object
        assert (
            assertion.criteria_condition
            == SqlAssertionCondition.GROWS_AT_MOST_PERCENTAGE
        )

        # Verify monitor was created (using nested data from GraphQL response)
        monitor = fetched.get("monitor")
        assert monitor is not None and monitor.get("urn"), "Monitor should exist"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_sql_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test SQL assertion with ALL optional parameters.

    Creates assertion with custom URN, statement, criteria, tags, incident_behavior, schedule.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-sql-all-params-{test_id}"
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_sql_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params SQL {test_id}",
            statement="SELECT COUNT(*) FROM test_table WHERE created_at > NOW() - INTERVAL '1 day'",
            criteria_condition="IS_GREATER_THAN",
            criteria_parameters=0,
            schedule="0 * * * *",
            incident_behavior=["raise_on_fail", "resolve_on_pass"],
            tags=["urn:li:tag:sql-test"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created SQL assertion with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "SQL"
        assert fetched["urn"] == custom_urn

        # Verify source type is NATIVE
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "NATIVE"

        # Verify statement
        statement = get_nested_value(fetched, "info.sqlAssertion.statement")
        assert "SELECT COUNT(*)" in statement, f"Expected statement, got {statement}"

        # Verify criteria_condition (operator)
        operator = get_nested_value(fetched, "info.sqlAssertion.operator")
        assert operator == "GREATER_THAN", f"Expected GREATER_THAN, got {operator}"

        # Verify criteria_parameters
        params_value = get_nested_value(
            fetched, "info.sqlAssertion.parameters.value.value"
        )
        assert params_value == "0", f"Expected params value 0, got {params_value}"

        # Verify incident_behavior (actions)
        actions = get_nested_value(fetched, "actions")
        assert actions is not None, "actions should be set"
        on_failure_types = [a["type"] for a in actions.get("onFailure", [])]
        assert "RAISE_INCIDENT" in on_failure_types, (
            "Expected RAISE_INCIDENT in onFailure"
        )
        on_success_types = [a["type"] for a in actions.get("onSuccess", [])]
        assert "RESOLVE_INCIDENT" in on_success_types, (
            "Expected RESOLVE_INCIDENT in onSuccess"
        )

        # Verify tags
        tags = get_nested_value(fetched, "tags.tags")
        assert tags is not None and len(tags) > 0, "Expected tags to be set"
        tag_urns = [t["tag"]["urn"] for t in tags]
        assert "urn:li:tag:sql-test" in tag_urns, (
            f"Expected sql-test tag, got {tag_urns}"
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


def test_smart_sql_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test smart SQL assertion with ALL optional parameters.

    Creates assertion with custom URN, statement, sensitivity, exclusion_windows,
    training_data_lookback_days, tags.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-smart-sql-all-params-{test_id}"
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_smart_sql_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params Smart SQL {test_id}",
            statement="SELECT COUNT(*) FROM test_table",
            sensitivity="low",
            schedule="0 * * * *",
            exclusion_windows=[
                {
                    "start": "2025-01-04T00:00:00",
                    "end": "2025-01-05T23:59:59",
                }
            ],
            training_data_lookback_days=21,
            tags=["urn:li:tag:smart-sql-test"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created smart SQL with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "SQL"
        assert fetched["urn"] == custom_urn

        # Verify source type is INFERRED (smart assertion)
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "INFERRED"

        # Verify statement
        statement = get_nested_value(fetched, "info.sqlAssertion.statement")
        assert "SELECT COUNT(*)" in statement, f"Expected statement, got {statement}"

        # Verify SDK return object
        assert assertion.sensitivity == InferenceSensitivity.LOW

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
        assert lookback_days == 21, f"Expected lookback days 21, got {lookback_days}"

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
        assert "urn:li:tag:smart-sql-test" in tag_urns, (
            f"Expected smart-sql-test tag, got {tag_urns}"
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


def test_sql_update_merge_behavior(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test that updating statement preserves criteria.

    This tests the merge behavior for SQL assertions.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        # Create assertion with specific criteria
        assertion = datahub_client.assertions.sync_sql_assertion(
            dataset_urn=test_data,
            display_name=f"Merge Test SQL {test_id}",
            statement="SELECT COUNT(*) FROM test_table",
            criteria_condition="IS_GREATER_THAN",
            criteria_parameters=100,
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created SQL for merge test: {assertion_urn}")

        wait_for_assertion_sync()

        # Capture original values
        original_criteria_condition = assertion.criteria_condition
        original_criteria_parameters = assertion.criteria_parameters
        original_display_name = assertion.display_name

        # Update only the statement
        updated = datahub_client.assertions.sync_sql_assertion(
            dataset_urn=test_data,
            urn=assertion_urn,
            statement="SELECT COUNT(*) FROM test_table WHERE status = 'active'",
        )

        wait_for_assertion_sync()

        # Verify the statement changed via SDK return object
        assert "status = 'active'" in updated.statement
        assert str(updated.urn) == assertion_urn

        # Verify criteria fields are preserved via SDK return object
        assert updated.criteria_condition == original_criteria_condition
        assert updated.criteria_parameters == original_criteria_parameters
        assert updated.display_name == original_display_name

        # Verify via GraphQL that the update was persisted to the server
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None

        # Verify statement was updated via GraphQL
        statement = get_nested_value(fetched, "info.sqlAssertion.statement")
        assert "status = 'active'" in statement, (
            f"Expected statement to contain 'status = active', got {statement}"
        )

        # Verify criteria are preserved via GraphQL
        operator = get_nested_value(fetched, "info.sqlAssertion.operator")
        assert operator == "GREATER_THAN", (
            f"Expected operator GREATER_THAN, got {operator}"
        )
        params_value = get_nested_value(
            fetched, "info.sqlAssertion.parameters.value.value"
        )
        # GraphQL returns numeric values as strings, may include decimal (e.g., "100.0")
        assert float(params_value) == 100, (
            f"Expected params value 100, got {params_value}"
        )

        # Cleanup and verify deletion
        cleanup_assertion(graph_client, assertion_urn)
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is None, "Assertion should be deleted"
        assertion_urn = None  # Prevent double cleanup in finally

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)
