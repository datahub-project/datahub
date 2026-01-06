"""Tests for the SQL assertion client."""

import pytest

from acryl_datahub_cloud.sdk.assertion_input.sql_assertion_input import (
    SqlAssertionCondition,
)
from acryl_datahub_cloud.sdk.assertions_client import AssertionsClient
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, DatasetUrn, MonitorUrn
from tests.sdk.assertions.conftest import StubDataHubClient, StubEntityClient


def test_sync_sql_assertion_raises_on_corrupted_existing_assertion_missing_statement(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that updating an assertion with corrupted backend data (missing statement) raises clear error."""
    # Create a corrupted assertion entity (missing statement)
    corrupted_assertion = Assertion(
        id=AssertionUrn("urn:li:assertion:corrupted"),
        info=models.SqlAssertionInfoClass(
            type=models.SqlAssertionTypeClass.METRIC,
            entity=str(any_dataset_urn),
            statement=None,  # type: ignore[arg-type]  # Intentionally corrupted for testing
            operator=models.AssertionStdOperatorClass.GREATER_THAN,
            parameters=models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    type=models.AssertionStdParameterTypeClass.NUMBER,
                    value="100.0",
                ),
            ),
        ),
        description="Corrupted SQL Assertion",
        source=models.AssertionSourceClass(
            type=models.AssertionSourceTypeClass.NATIVE,
            created=models.AuditStampClass(
                actor="urn:li:corpuser:test",
                time=1609459200000,
            ),
        ),
    )
    monitor = Monitor(
        id=MonitorUrn(entity=any_dataset_urn, id="urn:li:assertion:corrupted"),
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
        ),
    )
    stub_entity_client = StubEntityClient(
        monitor_entity=monitor, assertion_entity=corrupted_assertion
    )
    stub_client = StubDataHubClient(entity_client=stub_entity_client)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]

    with pytest.raises(SDKUsageError, match="statement is required"):
        client.sync_sql_assertion(
            dataset_urn=any_dataset_urn,
            urn="urn:li:assertion:corrupted",
            # Not providing statement - should fetch from backend and fail
        )


def test_sync_sql_assertion_raises_on_corrupted_existing_assertion_missing_criteria(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that updating an assertion with corrupted backend data (missing criteria) raises clear error."""
    # Create a corrupted assertion entity (missing operator/parameters)
    corrupted_assertion = Assertion(
        id=AssertionUrn("urn:li:assertion:corrupted"),
        info=models.SqlAssertionInfoClass(
            type=models.SqlAssertionTypeClass.METRIC,
            entity=str(any_dataset_urn),
            statement="SELECT COUNT(*) FROM test_table",
            operator=None,  # type: ignore[arg-type]  # Intentionally corrupted for testing
            parameters=None,  # type: ignore[arg-type]  # Intentionally corrupted for testing
        ),
        description="Corrupted SQL Assertion",
        source=models.AssertionSourceClass(
            type=models.AssertionSourceTypeClass.NATIVE,
            created=models.AuditStampClass(
                actor="urn:li:corpuser:test",
                time=1609459200000,
            ),
        ),
    )
    monitor = Monitor(
        id=MonitorUrn(entity=any_dataset_urn, id="urn:li:assertion:corrupted"),
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
        ),
    )
    stub_entity_client = StubEntityClient(
        monitor_entity=monitor, assertion_entity=corrupted_assertion
    )
    stub_client = StubDataHubClient(entity_client=stub_entity_client)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]

    with pytest.raises(SDKUsageError, match="criteria .* is required"):
        client.sync_sql_assertion(
            dataset_urn=any_dataset_urn,
            urn="urn:li:assertion:corrupted",
            # Not providing criteria - should fetch from backend and fail
        )


def test_sync_sql_assertion_raises_on_partial_criteria_condition_only(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that providing only criteria_condition without parameters raises clear error."""
    stub_client = StubDataHubClient()
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]

    with pytest.raises(
        SDKUsageError,
        match="criteria_condition and criteria_parameters must both be provided or both omitted",
    ):
        client.sync_sql_assertion(
            dataset_urn=any_dataset_urn,
            urn="urn:li:assertion:some_urn",
            statement="SELECT COUNT(*) FROM table",
            criteria_condition=SqlAssertionCondition.IS_EQUAL_TO,
            # criteria_parameters missing
        )


def test_sync_sql_assertion_raises_on_partial_criteria_parameters_only(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that providing only criteria_parameters without condition raises clear error."""
    stub_client = StubDataHubClient()
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]

    with pytest.raises(
        SDKUsageError,
        match="criteria_condition and criteria_parameters must both be provided or both omitted",
    ):
        client.sync_sql_assertion(
            dataset_urn=any_dataset_urn,
            urn="urn:li:assertion:some_urn",
            statement="SELECT COUNT(*) FROM table",
            criteria_parameters=50,
            # criteria_condition missing
        )


def test_sync_sql_assertion_single_fetch_optimization(
    sql_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that sync_sql_assertion only fetches backend data once when updating an existing assertion.

    This test validates the single-fetch optimization: when updating an assertion,
    we should fetch the assertion and monitor entities once and reuse that data
    for merging, rather than fetching multiple times.
    """
    from unittest.mock import MagicMock

    client = AssertionsClient(sql_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Wrap the entity client's get method with a spy to count calls
    original_get = client.client.entities.get
    get_spy = MagicMock(wraps=original_get)
    client.client.entities.get = get_spy  # type: ignore[method-assign]

    # Mock upsert to prevent actual writes
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign]

    # Call sync_sql_assertion with a URN (update scenario) and only changing enabled status
    # This should fetch backend data once to get existing values
    result = client.sync_sql_assertion(
        dataset_urn=any_dataset_urn,
        urn="urn:li:assertion:smart_freshness_assertion",  # Use the URN from conftest fixtures
        enabled=False,  # Only change enabled status, other fields should be fetched
    )

    # Verify that the assertion was created successfully with fetched values
    assert result is not None
    assert result.statement == "SELECT COUNT(*) FROM test_table"
    assert result.criteria is not None

    # Verify that entities.get() was called exactly twice:
    # 1. Once for fetching the assertion entity
    # 2. Once for fetching the monitor entity (after search finds the monitor URN)
    # This proves the single-fetch optimization: we don't re-fetch the same data multiple times
    assert get_spy.call_count == 2, (
        f"Expected exactly 2 calls to entities.get() (assertion + monitor), but got {get_spy.call_count}"
    )

    # Verify we fetched the correct URNs
    call_args = [str(call[0][0]) for call in get_spy.call_args_list]
    assert "urn:li:assertion:smart_freshness_assertion" in call_args
    # Monitor URN will vary based on the fixture setup
