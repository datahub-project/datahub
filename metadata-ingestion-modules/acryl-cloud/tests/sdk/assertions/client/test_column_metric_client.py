"""Tests for ColumnMetricAssertionClient internal methods."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud.sdk.assertion_client.column_metric import (
    ColumnMetricAssertionClient,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn, MonitorUrn
from tests.sdk.assertions.conftest import StubDataHubClient, StubEntityClient

FROZEN_TIME = "2025-01-01 10:30:00"

_any_dataset_urn = DatasetUrn.from_string(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
)
_any_assertion_urn = AssertionUrn("urn:li:assertion:test_assertion")


@pytest.fixture
def column_metric_assertion_with_value_params() -> Assertion:
    """Assertion entity with value-based parameters."""
    return Assertion(
        id=_any_assertion_urn,
        info=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity=str(_any_dataset_urn),
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="amount", type="number", nativeType="NUMBER"
                ),
                metric=models.FieldMetricTypeClass.NULL_COUNT,
                operator=models.AssertionStdOperatorClass.GREATER_THAN,
                parameters=models.AssertionStdParametersClass(
                    value=models.AssertionStdParameterClass(
                        value="10", type=models.AssertionStdParameterTypeClass.NUMBER
                    ),
                ),
            ),
        ),
        description="Test Column Metric",
        source=models.AssertionSourceClass(
            type=models.AssertionSourceTypeClass.NATIVE,
            created=models.AuditStampClass(
                actor="urn:li:corpuser:test_user",
                time=1609459200000,
            ),
        ),
        last_updated=models.AuditStampClass(
            actor="urn:li:corpuser:test_user_updated",
            time=1609545600000,
        ),
    )


@pytest.fixture
def column_metric_assertion_with_range_params() -> Assertion:
    """Assertion entity with range-based parameters (minValue/maxValue)."""
    return Assertion(
        id=_any_assertion_urn,
        info=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity=str(_any_dataset_urn),
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="amount", type="number", nativeType="NUMBER"
                ),
                metric=models.FieldMetricTypeClass.MEAN,
                operator=models.AssertionStdOperatorClass.BETWEEN,
                parameters=models.AssertionStdParametersClass(
                    minValue=models.AssertionStdParameterClass(
                        value="100", type=models.AssertionStdParameterTypeClass.NUMBER
                    ),
                    maxValue=models.AssertionStdParameterClass(
                        value="500", type=models.AssertionStdParameterTypeClass.NUMBER
                    ),
                ),
            ),
        ),
    )


@pytest.fixture
def active_monitor() -> Monitor:
    """Monitor entity in active mode."""
    return Monitor(
        id=MonitorUrn(entity=_any_dataset_urn, id=str(_any_assertion_urn)),
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=str(_any_assertion_urn),
                        schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
                        parameters=models.AssertionEvaluationParametersClass(
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_FIELD,
                            datasetFieldParameters=models.DatasetFieldAssertionParametersClass(
                                sourceType=models.DatasetFieldAssertionSourceTypeClass.ALL_ROWS_QUERY,
                            ),
                        ),
                    )
                ],
            ),
        ),
    )


def test_retrieve_assertion_and_monitor_calls_helper() -> None:
    """Test that _retrieve_assertion_and_monitor delegates to helper function."""
    stub_client = StubDataHubClient(StubEntityClient())
    client = ColumnMetricAssertionClient(stub_client)  # type: ignore[arg-type]

    with patch(
        "acryl_datahub_cloud.sdk.assertion_client.column_metric.retrieve_assertion_and_monitor_by_urn"
    ) as mock_retrieve:
        mock_retrieve.return_value = (None, MagicMock(), None)

        client._retrieve_assertion_and_monitor(_any_assertion_urn, _any_dataset_urn)

        mock_retrieve.assert_called_once_with(
            stub_client, _any_assertion_urn, _any_dataset_urn
        )


@freeze_time(FROZEN_TIME)
def test_retrieve_and_merge_extracts_field_from_existing_assertion_with_value(
    column_metric_assertion_with_value_params: Assertion,
    active_monitor: Monitor,
) -> None:
    """Test extracting column_name, metric_type, operator, and value-based parameters from existing assertion."""
    stub_entity_client = StubEntityClient(
        monitor_entity=active_monitor,
        assertion_entity=column_metric_assertion_with_value_params,
    )
    stub_client = StubDataHubClient(stub_entity_client)
    client = ColumnMetricAssertionClient(stub_client)  # type: ignore[arg-type]

    result = client._retrieve_and_merge_column_metric_assertion_and_monitor(
        dataset_urn=_any_dataset_urn,
        urn=_any_assertion_urn,
        column_name=None,
        metric_type=None,
        operator=None,
        criteria_parameters=None,
        display_name=None,
        enabled=None,
        detection_mechanism=None,
        incident_behavior=None,
        tags=None,
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:updater"),
        now_utc=datetime(2025, 1, 1, 10, 30, tzinfo=timezone.utc),
        schedule=None,
    )

    assert result.column_name == "amount"
    assert result.metric_type == models.FieldMetricTypeClass.NULL_COUNT
    assert result.operator == models.AssertionStdOperatorClass.GREATER_THAN
    assert result.criteria_parameters == 10


@freeze_time(FROZEN_TIME)
def test_retrieve_and_merge_extracts_range_parameters_from_existing_assertion(
    column_metric_assertion_with_range_params: Assertion,
    active_monitor: Monitor,
) -> None:
    """Test extracting minValue/maxValue range parameters from existing assertion."""
    stub_entity_client = StubEntityClient(
        monitor_entity=active_monitor,
        assertion_entity=column_metric_assertion_with_range_params,
    )
    stub_client = StubDataHubClient(stub_entity_client)
    client = ColumnMetricAssertionClient(stub_client)  # type: ignore[arg-type]

    result = client._retrieve_and_merge_column_metric_assertion_and_monitor(
        dataset_urn=_any_dataset_urn,
        urn=_any_assertion_urn,
        column_name=None,
        metric_type=None,
        operator=None,
        criteria_parameters=None,
        display_name=None,
        enabled=None,
        detection_mechanism=None,
        incident_behavior=None,
        tags=None,
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:updater"),
        now_utc=datetime(2025, 1, 1, 10, 30, tzinfo=timezone.utc),
        schedule=None,
    )

    assert result.column_name == "amount"
    assert result.operator == models.AssertionStdOperatorClass.BETWEEN
    assert result.criteria_parameters == (100, 500)


@freeze_time(FROZEN_TIME)
def test_retrieve_and_merge_with_urn_but_no_existing_assertion_returns_new_input() -> (
    None
):
    """Test that when URN is provided but assertion doesn't exist, a new input is returned."""
    stub_entity_client = StubEntityClient(monitor_entity=None, assertion_entity=None)
    stub_client = StubDataHubClient(stub_entity_client)
    client = ColumnMetricAssertionClient(stub_client)  # type: ignore[arg-type]

    result = client._retrieve_and_merge_column_metric_assertion_and_monitor(
        dataset_urn=_any_dataset_urn,
        urn=_any_assertion_urn,
        column_name="amount",
        metric_type="NULL_COUNT",
        operator="GREATER_THAN",
        criteria_parameters=5,
        display_name="Test",
        enabled=True,
        detection_mechanism=None,
        incident_behavior=None,
        tags=None,
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:creator"),
        now_utc=datetime(2025, 1, 1, 10, 30, tzinfo=timezone.utc),
        schedule=None,
    )

    assert result.urn == _any_assertion_urn
    assert result.column_name == "amount"
    assert result.metric_type == models.FieldMetricTypeClass.NULL_COUNT
    assert result.operator == models.AssertionStdOperatorClass.GREATER_THAN
    assert result.criteria_parameters == 5
    assert result.enabled is True


@freeze_time(FROZEN_TIME)
def test_retrieve_and_merge_validates_required_fields_when_urn_provided_but_no_assertion() -> (
    None
):
    """Test validation error when URN is provided but assertion doesn't exist and required fields are missing."""
    stub_entity_client = StubEntityClient(monitor_entity=None, assertion_entity=None)
    stub_client = StubDataHubClient(stub_entity_client)
    client = ColumnMetricAssertionClient(stub_client)  # type: ignore[arg-type]

    with pytest.raises(SDKUsageError, match="column_name is required"):
        client._retrieve_and_merge_column_metric_assertion_and_monitor(
            dataset_urn=_any_dataset_urn,
            urn=_any_assertion_urn,
            column_name=None,
            metric_type="NULL_COUNT",
            operator="GREATER_THAN",
            criteria_parameters=5,
            display_name=None,
            enabled=None,
            detection_mechanism=None,
            incident_behavior=None,
            tags=None,
            updated_by=CorpUserUrn.from_string("urn:li:corpuser:updater"),
            now_utc=datetime(2025, 1, 1, 10, 30, tzinfo=timezone.utc),
            schedule=None,
        )


@freeze_time(FROZEN_TIME)
def test_merge_column_metric_input_merges_all_fields(
    column_metric_assertion_with_value_params: Assertion,
    active_monitor: Monitor,
) -> None:
    """Test that _merge_column_metric_input correctly merges fields."""
    from acryl_datahub_cloud.sdk.assertion.column_metric_assertion import (
        ColumnMetricAssertion,
    )
    from acryl_datahub_cloud.sdk.assertion_input.column_metric_assertion_input import (
        _ColumnMetricAssertionInput,
    )

    stub_entity_client = StubEntityClient(
        monitor_entity=active_monitor,
        assertion_entity=column_metric_assertion_with_value_params,
    )
    stub_client = StubDataHubClient(stub_entity_client)
    client = ColumnMetricAssertionClient(stub_client)  # type: ignore[arg-type]

    existing_assertion = ColumnMetricAssertion._from_entities(
        column_metric_assertion_with_value_params, active_monitor
    )

    assertion_input = _ColumnMetricAssertionInput(
        urn=_any_assertion_urn,
        entity_client=stub_entity_client,
        dataset_urn=_any_dataset_urn,
        column_name="amount",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        operator=models.AssertionStdOperatorClass.GREATER_THAN,
        criteria_parameters=10,
        display_name="Test",
        enabled=True,
        detection_mechanism=None,
        incident_behavior=None,
        tags=None,
        created_by=CorpUserUrn.from_string("urn:li:corpuser:creator"),
        created_at=datetime(2025, 1, 1, 10, 30, tzinfo=timezone.utc),
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:updater"),
        updated_at=datetime(2025, 1, 1, 10, 30, tzinfo=timezone.utc),
        gms_criteria_type_info=None,
    )

    result = client._merge_column_metric_input(
        dataset_urn=_any_dataset_urn,
        column_name="amount",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        operator=models.AssertionStdOperatorClass.GREATER_THAN,
        criteria_parameters=10,
        urn=_any_assertion_urn,
        display_name="Updated Display Name",
        enabled=False,
        schedule=None,
        detection_mechanism=None,
        incident_behavior=None,
        tags=None,
        now_utc=datetime(2025, 1, 1, 11, 0, tzinfo=timezone.utc),
        assertion_input=assertion_input,
        maybe_assertion_entity=column_metric_assertion_with_value_params,
        maybe_monitor_entity=active_monitor,
        existing_assertion=existing_assertion,
        gms_criteria_type_info=None,
    )

    assert result.urn == _any_assertion_urn
    assert result.column_name == "amount"
    assert result.display_name == "Updated Display Name"
    assert result.enabled is False
    assert result.updated_at == datetime(2025, 1, 1, 10, 30, tzinfo=timezone.utc)


@freeze_time(FROZEN_TIME)
def test_retrieve_and_merge_with_assertion_but_no_monitor(
    column_metric_assertion_with_value_params: Assertion,
) -> None:
    """Test that when assertion exists but monitor doesn't, a placeholder monitor is created."""
    stub_entity_client = StubEntityClient(
        monitor_entity=None,
        assertion_entity=column_metric_assertion_with_value_params,
    )
    stub_client = StubDataHubClient(stub_entity_client)
    client = ColumnMetricAssertionClient(stub_client)  # type: ignore[arg-type]

    result = client._retrieve_and_merge_column_metric_assertion_and_monitor(
        dataset_urn=_any_dataset_urn,
        urn=_any_assertion_urn,
        column_name=None,
        metric_type=None,
        operator=None,
        criteria_parameters=None,
        display_name="Updated Name",
        enabled=False,
        detection_mechanism=None,
        incident_behavior=None,
        tags=None,
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:updater"),
        now_utc=datetime(2025, 1, 1, 10, 30, tzinfo=timezone.utc),
        schedule=None,
    )

    assert result.column_name == "amount"
    assert result.display_name == "Updated Name"
    assert result.enabled is False
