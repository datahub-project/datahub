"""
Integration tests for column value assertion SDK methods.

Tests the sync_column_value_assertion method with various operators and configurations.
Column value assertions validate individual row values in a column.
"""

import logging
from typing import Any

import pytest
from acryl_datahub_cloud.sdk import SqlExpression
from acryl_datahub_cloud.sdk.errors import SDKUsageError

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


def test_column_value_assertion_not_null(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test column value assertion with NOT_NULL operator.

    Validates that a column has no null values - a common data quality check.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_column_value_assertion(
            dataset_urn=test_data,
            display_name=f"Not Null Check {test_id}",
            column_name="user_id",
            operator="NOT_NULL",
            fail_threshold_type="COUNT",
            fail_threshold_value=0,
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created column value assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "FIELD"
        assert fetched["info"]["fieldAssertion"]["type"] == "FIELD_VALUES"
        assert (
            fetched["info"]["fieldAssertion"]["fieldValuesAssertion"]["operator"]
            == "NOT_NULL"
        )

        # Verify SDK return object
        assert assertion.column_name == "user_id"
        assert str(assertion.dataset_urn) == test_data

        # Verify monitor was created (using nested data from GraphQL response)
        monitor = fetched.get("monitor")
        assert monitor is not None and monitor.get("urn"), "Monitor should exist"

        # Verify source type is NATIVE (not INFERRED since not smart assertion)
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "NATIVE"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_column_value_assertion_regex_match(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test column value assertion with REGEX_MATCH operator.

    Real-world use case: validate email format in an email column.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        # Simple email regex pattern
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

        assertion = datahub_client.assertions.sync_column_value_assertion(
            dataset_urn=test_data,
            display_name=f"Email Format Check {test_id}",
            column_name="email",
            operator="REGEX_MATCH",
            criteria_parameters=email_pattern,
            fail_threshold_type="PERCENTAGE",
            fail_threshold_value=5,  # Allow up to 5% invalid emails
            exclude_nulls=True,  # Ignore null values
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created regex assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert (
            fetched["info"]["fieldAssertion"]["fieldValuesAssertion"]["operator"]
            == "REGEX_MATCH"
        )
        assert (
            fetched["info"]["fieldAssertion"]["fieldValuesAssertion"]["excludeNulls"]
            is True
        )

        # Verify failThreshold is set correctly
        fail_threshold = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.failThreshold"
        )
        assert fail_threshold is not None, "failThreshold should be set"
        assert fail_threshold.get("type") == "PERCENTAGE", (
            f"Expected failThreshold type PERCENTAGE, got {fail_threshold.get('type')}"
        )
        assert fail_threshold.get("value") == 5, (
            f"Expected failThreshold value 5, got {fail_threshold.get('value')}"
        )

        # Verify criteria_parameters (regex pattern) is set correctly
        params = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.parameters"
        )
        assert params is not None, "parameters should be set"
        param_value = get_nested_value(params, "value.value")
        assert param_value is not None, "REGEX_MATCH should have value parameter"
        # The regex pattern should contain key parts of the email pattern
        assert "@" in str(param_value) or "a-zA-Z" in str(param_value), (
            f"Expected email pattern in params, got {param_value}"
        )

        # Verify SDK return object
        assert assertion.column_name == "email"
        assert assertion.exclude_nulls is True

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_column_value_assertion_between_with_transform(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test column value assertion with BETWEEN operator and LENGTH transform.

    Real-world use case: validate that string length is within a range.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_column_value_assertion(
            dataset_urn=test_data,
            display_name=f"Name Length Check {test_id}",
            column_name="name",
            operator="BETWEEN",
            criteria_parameters=(1, 100),  # Name length between 1 and 100 chars
            transform="LENGTH",
            fail_threshold_type="COUNT",
            fail_threshold_value=0,
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created length check assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert (
            fetched["info"]["fieldAssertion"]["fieldValuesAssertion"]["operator"]
            == "BETWEEN"
        )
        assert (
            fetched["info"]["fieldAssertion"]["fieldValuesAssertion"]["transform"][
                "type"
            ]
            == "LENGTH"
        )

        # Verify criteria parameters (minValue=1, maxValue=100)
        params = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.parameters"
        )
        assert params is not None, "parameters should be set"
        min_value = get_nested_value(params, "minValue.value")
        max_value = get_nested_value(params, "maxValue.value")
        assert min_value == "1", f"Expected minValue 1, got {min_value}"
        assert max_value == "100", f"Expected maxValue 100, got {max_value}"

        # Verify failThreshold is set correctly (COUNT=0)
        fail_threshold = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.failThreshold"
        )
        assert fail_threshold is not None, "failThreshold should be set"
        assert fail_threshold.get("type") == "COUNT", (
            f"Expected failThreshold type COUNT, got {fail_threshold.get('type')}"
        )
        assert fail_threshold.get("value") == 0, (
            f"Expected failThreshold value 0, got {fail_threshold.get('value')}"
        )

        # Verify SDK return object
        assert assertion.column_name == "name"
        assert assertion.transform == "LENGTH"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_column_value_assertion_greater_than_with_threshold(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test column value assertion with GREATER_THAN and percentage failure threshold.

    Real-world use case: validate positive quantities with tolerance for some failures.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_column_value_assertion(
            dataset_urn=test_data,
            display_name=f"Positive Quantity Check {test_id}",
            column_name="quantity",
            operator="GREATER_THAN",
            criteria_parameters=0,
            fail_threshold_type="PERCENTAGE",
            fail_threshold_value=5,  # Allow up to 5% failures
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created quantity check assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert (
            fetched["info"]["fieldAssertion"]["fieldValuesAssertion"]["operator"]
            == "GREATER_THAN"
        )

        # Verify criteria_parameters (0) via GraphQL
        params = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.parameters"
        )
        assert params is not None, "parameters should be set"
        param_value = get_nested_value(params, "value.value")
        assert param_value == "0", f"Expected param value 0, got {param_value}"

        # Verify failThreshold is set correctly
        fail_threshold = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.failThreshold"
        )
        assert fail_threshold is not None, "failThreshold should be set"
        assert fail_threshold.get("type") == "PERCENTAGE", (
            f"Expected failThreshold type PERCENTAGE, got {fail_threshold.get('type')}"
        )
        assert fail_threshold.get("value") == 5, (
            f"Expected failThreshold value 5, got {fail_threshold.get('value')}"
        )

        # Verify SDK return object
        assert assertion.column_name == "quantity"

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_column_value_with_all_params(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test column value assertion with ALL optional parameters.

    Creates assertion with custom URN, tags, incident_behavior, detection_mechanism,
    and all column value specific options.
    """
    test_id = generate_unique_test_id()
    custom_urn = f"urn:li:assertion:test-col-value-all-params-{test_id}"
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_column_value_assertion(
            dataset_urn=test_data,
            urn=custom_urn,
            display_name=f"Full Params Column Value {test_id}",
            column_name="price",
            operator="GREATER_THAN",
            criteria_parameters=0,
            fail_threshold_type="PERCENTAGE",
            fail_threshold_value=5,
            exclude_nulls=True,
            schedule="0 * * * *",
            detection_mechanism={
                "type": "changed_rows_query",
                "column_name": "updated_at",
            },
            incident_behavior=["raise_on_fail"],
            tags=["urn:li:tag:column-value-test"],
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn == custom_urn, (
            f"Expected URN {custom_urn}, got {assertion_urn}"
        )
        logger.info(f"Created column value assertion with all params: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "FIELD"
        assert fetched["urn"] == custom_urn

        # Verify source type is NATIVE
        source_type = get_nested_value(fetched, "info.source.type")
        assert source_type == "NATIVE"

        # Verify operator
        operator = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.operator"
        )
        assert operator == "GREATER_THAN", f"Expected GREATER_THAN, got {operator}"

        # Verify exclude_nulls
        exclude_nulls = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.excludeNulls"
        )
        assert exclude_nulls is True

        # Verify fail_threshold
        fail_threshold = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.failThreshold"
        )
        assert fail_threshold is not None
        assert fail_threshold.get("type") == "PERCENTAGE"
        assert fail_threshold.get("value") == 5

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
        assert "urn:li:tag:column-value-test" in tag_urns, (
            f"Expected column-value-test tag, got {tag_urns}"
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

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_column_value_assertion_update_preserves_fields(
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
        assertion = datahub_client.assertions.sync_column_value_assertion(
            dataset_urn=test_data,
            display_name=f"Merge Test {test_id}",
            column_name="price",
            operator="GREATER_THAN",
            criteria_parameters=0,
            fail_threshold_type="PERCENTAGE",
            fail_threshold_value=10,
            exclude_nulls=True,
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created assertion for merge test: {assertion_urn}")

        wait_for_assertion_sync()

        # Capture original values
        original_column = assertion.column_name
        original_exclude_nulls = assertion.exclude_nulls
        original_display_name = assertion.display_name

        # Update only the fail_threshold_value
        updated = datahub_client.assertions.sync_column_value_assertion(
            dataset_urn=test_data,
            urn=assertion_urn,
            fail_threshold_value=20,  # Change from 10 to 20
        )

        wait_for_assertion_sync()

        # Verify the updated field changed via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None

        # Verify fail_threshold_value was updated from 10 to 20
        fail_threshold = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.failThreshold"
        )
        assert fail_threshold is not None, "failThreshold should be set"
        assert fail_threshold.get("type") == "PERCENTAGE", (
            f"Expected failThreshold type PERCENTAGE, got {fail_threshold.get('type')}"
        )
        assert fail_threshold.get("value") == 20, (
            f"Expected failThreshold value 20 (updated), got {fail_threshold.get('value')}"
        )

        # Verify non-updated fields are preserved via SDK return object
        assert str(updated.urn) == assertion_urn
        assert updated.column_name == original_column
        assert updated.exclude_nulls == original_exclude_nulls
        assert updated.display_name == original_display_name
        assert str(updated.dataset_urn) == test_data

        # Verify other fields preserved via GraphQL
        assert (
            fetched["info"]["fieldAssertion"]["fieldValuesAssertion"]["operator"]
            == "GREATER_THAN"
        )
        assert (
            fetched["info"]["fieldAssertion"]["fieldValuesAssertion"]["excludeNulls"]
            is True
        )

        # Cleanup and verify deletion
        cleanup_assertion(graph_client, assertion_urn)
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is None, "Assertion should be deleted"
        assertion_urn = None  # Prevent double cleanup in finally

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_column_value_assertion_sql_expression(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test column value assertion with SQL expression criteria.

    Real-world use case: validate customer_id exists in a reference table of valid customers.
    """
    test_id = generate_unique_test_id()
    assertion_urn = None

    try:
        assertion = datahub_client.assertions.sync_column_value_assertion(
            dataset_urn=test_data,
            display_name=f"Valid Customer Check {test_id}",
            column_name="user_id",
            operator="IN",
            criteria_parameters=SqlExpression(
                "SELECT id FROM valid_customers WHERE active = true"
            ),
            fail_threshold_type="COUNT",
            fail_threshold_value=0,
            schedule="0 * * * *",
        )

        assertion_urn = str(assertion.urn)
        assert assertion_urn
        logger.info(f"Created SQL expression assertion: {assertion_urn}")

        wait_for_assertion_sync()

        # Verify creation via GraphQL
        fetched = get_assertion_by_urn(graph_client, test_data, assertion_urn)
        assert fetched is not None
        assert fetched["info"]["type"] == "FIELD"
        assert (
            fetched["info"]["fieldAssertion"]["fieldValuesAssertion"]["operator"]
            == "IN"
        )

        # Verify parameter type is SQL
        params = get_nested_value(
            fetched, "info.fieldAssertion.fieldValuesAssertion.parameters"
        )
        assert params is not None
        param_type = get_nested_value(params, "value.type")
        assert param_type == "SQL", f"Expected parameter type SQL, got {param_type}"

        # Verify SQL value is preserved
        param_value = get_nested_value(params, "value.value")
        assert "SELECT" in param_value
        assert "valid_customers" in param_value

        # Verify SDK return object has SqlExpression
        assert isinstance(assertion.criteria_parameters, SqlExpression)
        assert "SELECT" in assertion.criteria_parameters.sql
        assert assertion.is_sql_criteria is True

    finally:
        if assertion_urn:
            cleanup_assertion(graph_client, assertion_urn)


def test_column_value_assertion_sql_expression_invalid_operator(
    test_data: Any, graph_client: DataHubGraph, datahub_client: DataHubClient
) -> None:
    """Test that SQL expression cannot be used with NULL operator.

    SQL expressions are only valid for operators that accept value parameters,
    not for no-parameter operators like NULL, NOT_NULL, IS_TRUE, IS_FALSE.
    """
    test_id = generate_unique_test_id()

    # Should raise SDKUsageError when using SQL with NULL operator
    with pytest.raises(SDKUsageError) as exc_info:
        datahub_client.assertions.sync_column_value_assertion(
            dataset_urn=test_data,
            display_name=f"Invalid SQL Test {test_id}",
            column_name="user_id",
            operator="NULL",  # NULL doesn't accept parameters
            criteria_parameters=SqlExpression("SELECT id FROM valid_customers"),
            schedule="0 * * * *",
        )

    # Verify error message mentions SQL and operator incompatibility
    error_message = str(exc_info.value).lower()
    assert "sql" in error_message or "operator" in error_message
    logger.info(
        f"Correctly rejected SQL expression with NULL operator: {exc_info.value}"
    )
