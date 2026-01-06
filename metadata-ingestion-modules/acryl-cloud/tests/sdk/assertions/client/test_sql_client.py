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
