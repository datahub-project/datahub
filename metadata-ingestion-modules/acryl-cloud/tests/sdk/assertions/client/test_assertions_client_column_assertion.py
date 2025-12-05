from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Union
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    AssertionIncidentBehavior,
    DetectionMechanism,
    DetectionMechanismInputTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_assertion_input import (
    ColumnMetricAssertionParameters,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    MetricInputType,
    OperatorInputType,
)
from acryl_datahub_cloud.sdk.assertions_client import (
    DEFAULT_CREATED_BY,
    AssertionsClient,
)
from acryl_datahub_cloud.sdk.errors import (
    SDKUsageError,
)
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
    TagUrn,
)
from datahub.sdk._shared import TagsInputType
from tests.sdk.assertions.conftest import StubDataHubClient

FROZEN_TIME = "2025-01-01 10:30:00"

_any_dataset_urn = DatasetUrn.from_string(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
)

GENERATED_DISPLAY_NAME_LENGTH = 22


@dataclass
class ColumnMetricAssertionCreateParams:
    dataset_urn: Union[str, DatasetUrn]
    column_name: str
    metric_type: MetricInputType
    operator: OperatorInputType
    criteria_parameters: Optional[ColumnMetricAssertionParameters] = None
    display_name: Optional[str] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    created_by: Optional[CorpUserUrn] = None
    enabled: Optional[bool] = None


@dataclass
class ColumnMetricAssertionSyncParams:
    dataset_urn: Union[str, DatasetUrn]
    urn: Optional[Union[str, AssertionUrn]] = None
    column_name: Optional[str] = None
    metric_type: Optional[MetricInputType] = None
    operator: Optional[OperatorInputType] = None
    criteria_parameters: Optional[ColumnMetricAssertionParameters] = None
    display_name: Optional[str] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None
    enabled: Optional[bool] = None
    schedule: Optional[Union[str, models.CronScheduleClass]] = None


@freeze_time(FROZEN_TIME)
def test_create_column_metric_assertion_minimal_input(
    native_column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test creating a column metric assertion with minimal input parameters."""
    # Arrange
    client = AssertionsClient(native_column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    input_params = ColumnMetricAssertionCreateParams(
        dataset_urn=_any_dataset_urn,
        column_name="amount",
        metric_type="NULL_COUNT",
        operator="GREATER_THAN",
        criteria_parameters=10,
        enabled=True,
    )

    # Act
    actual_assertion = client._column_metric_client._create_column_metric_assertion(
        **input_params.__dict__
    )

    # Assert
    assert actual_assertion.dataset_urn == _any_dataset_urn
    assert actual_assertion.column_name == "amount"
    assert actual_assertion.metric_type == models.FieldMetricTypeClass.NULL_COUNT
    assert actual_assertion.operator == models.AssertionStdOperatorClass.GREATER_THAN
    assert actual_assertion.criteria_parameters == "10"
    assert actual_assertion.mode == AssertionMode.ACTIVE
    assert actual_assertion.schedule == DEFAULT_EVERY_SIX_HOURS_SCHEDULE
    assert actual_assertion.incident_behavior == []  # Default when not specified
    assert actual_assertion.detection_mechanism == DetectionMechanism.ALL_ROWS_QUERY()
    assert actual_assertion.tags == []
    assert actual_assertion.created_by == CorpUserUrn.from_string(DEFAULT_CREATED_BY)
    assert actual_assertion.created_at == datetime(
        2025, 1, 1, 10, 30, tzinfo=timezone.utc
    )
    assert actual_assertion.updated_by == CorpUserUrn.from_string(DEFAULT_CREATED_BY)
    assert actual_assertion.updated_at == datetime(
        2025, 1, 1, 10, 30, tzinfo=timezone.utc
    )
    assert actual_assertion.display_name.startswith(
        "New Assertion"
    )  # Generated display name
    assert actual_assertion.urn is not None  # URN should be generated
    assert client.client.entities.create.call_count == 2  # assertion + monitor


@freeze_time(FROZEN_TIME)
def test_create_column_metric_assertion_with_range_parameters(
    native_column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test creating a column metric assertion with range parameters."""
    # Arrange
    client = AssertionsClient(native_column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    input_params = ColumnMetricAssertionCreateParams(
        dataset_urn=_any_dataset_urn,
        column_name="amount",
        metric_type="MEAN",
        operator="BETWEEN",
        criteria_parameters=(100.0, 500.0),
    )

    # Act
    actual_assertion = client._column_metric_client._create_column_metric_assertion(
        **input_params.__dict__
    )

    # Assert
    assert actual_assertion.column_name == "amount"
    assert actual_assertion.operator == models.AssertionStdOperatorClass.BETWEEN
    assert (
        isinstance(actual_assertion.criteria_parameters, tuple)
        and len(actual_assertion.criteria_parameters) == 2
    )  # Range has two values
    assert actual_assertion.urn is not None


@freeze_time(FROZEN_TIME)
def test_create_column_metric_assertion_null_operator_no_parameters(
    native_column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test creating a column metric assertion with NULL operator (no parameters needed)."""
    # Arrange
    client = AssertionsClient(native_column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    input_params = ColumnMetricAssertionCreateParams(
        dataset_urn=_any_dataset_urn,
        column_name="last_modified",
        metric_type="NULL_COUNT",
        operator="NULL",
        criteria_parameters=None,
    )

    # Act
    actual_assertion = client._column_metric_client._create_column_metric_assertion(
        **input_params.__dict__
    )

    # Assert
    assert actual_assertion.column_name == "last_modified"
    assert actual_assertion.operator == models.AssertionStdOperatorClass.NULL
    assert actual_assertion.criteria_parameters is None
    assert actual_assertion.urn is not None


@freeze_time(FROZEN_TIME)
def test_sync_column_metric_assertion_create_minimal_input(
    native_column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test syncing a column metric assertion with minimal input (creation case)."""
    # Arrange
    client = AssertionsClient(native_column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    input_params = ColumnMetricAssertionSyncParams(
        dataset_urn=_any_dataset_urn,
        column_name="amount",
        metric_type="NULL_COUNT",
        operator="GREATER_THAN",
        criteria_parameters=5,
    )

    # Act
    actual_assertion = client.sync_column_metric_assertion(**input_params.__dict__)

    # Assert
    assert actual_assertion.column_name == "amount"
    assert actual_assertion.criteria_parameters == "5"
    assert actual_assertion.urn is not None
    assert client.client.entities.create.call_count == 2  # assertion + monitor


# TODO: Fix complex Assertion entity creation for proper type compatibility
# @freeze_time(FROZEN_TIME)
# @patch(
#     "acryl_datahub_cloud.sdk.assertions_client.AssertionsClient._retrieve_assertion_and_monitor"
# )
# def test_sync_column_metric_assertion_update_existing(
#     mock_retrieve: MagicMock,
#     native_column_metric_stub_datahub_client: StubDataHubClient,
# ) -> None:
#     """Test syncing a column metric assertion that updates an existing one."""
#     pass  # Complex test requiring proper entity mocking


@freeze_time(FROZEN_TIME)
def test_sync_column_metric_assertion_validation_error_missing_column_name(
    native_column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that sync_column_metric_assertion raises validation error when column_name is missing for creation."""
    # Arrange
    client = AssertionsClient(native_column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    input_params = ColumnMetricAssertionSyncParams(
        dataset_urn=_any_dataset_urn,
        # column_name is missing
        metric_type="NULL_COUNT",
        operator="GREATER_THAN",
        criteria_parameters=5,
    )

    # Act & Assert
    with pytest.raises(SDKUsageError, match="column_name is required"):
        client.sync_column_metric_assertion(**input_params.__dict__)


@freeze_time(FROZEN_TIME)
def test_sync_column_metric_assertion_validation_error_missing_metric_type(
    native_column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that sync_column_metric_assertion raises validation error when metric_type is missing for creation."""
    # Arrange
    client = AssertionsClient(native_column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    input_params = ColumnMetricAssertionSyncParams(
        dataset_urn=_any_dataset_urn,
        column_name="amount",
        # metric_type is missing
        operator="GREATER_THAN",
        criteria_parameters=5,
    )

    # Act & Assert
    with pytest.raises(SDKUsageError, match="metric_type is required"):
        client.sync_column_metric_assertion(**input_params.__dict__)


@freeze_time(FROZEN_TIME)
def test_sync_column_metric_assertion_validation_error_missing_operator(
    native_column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test that sync_column_metric_assertion raises validation error when operator is missing for creation."""
    # Arrange
    client = AssertionsClient(native_column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    input_params = ColumnMetricAssertionSyncParams(
        dataset_urn=_any_dataset_urn,
        column_name="amount",
        metric_type="NULL_COUNT",
        # operator is missing
        criteria_parameters=5,
    )

    # Act & Assert
    with pytest.raises(SDKUsageError, match="operator is required"):
        client.sync_column_metric_assertion(**input_params.__dict__)


# TODO: Fix complex Assertion entity creation for proper type compatibility
# @freeze_time(FROZEN_TIME)
# @patch(
#     "acryl_datahub_cloud.sdk.assertions_client.AssertionsClient._retrieve_assertion_and_monitor"
# )
# def test_sync_column_metric_assertion_dataset_urn_mismatch_error(
#     mock_retrieve: MagicMock,
#     native_column_metric_stub_datahub_client: StubDataHubClient,
# ) -> None:
#     """Test that sync_column_metric_assertion raises error when dataset URNs don't match."""
#     pass  # Complex test requiring proper entity mocking


@freeze_time(FROZEN_TIME)
def test_sync_column_metric_assertion_comprehensive_parameters(
    native_column_metric_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test creating a column metric assertion with comprehensive parameters."""
    # Arrange
    client = AssertionsClient(native_column_metric_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    user = CorpUserUrn.from_string("urn:li:corpuser:test_user")
    custom_schedule = models.CronScheduleClass(cron="0 */2 * * *", timezone="UTC")

    input_params = ColumnMetricAssertionSyncParams(
        dataset_urn=_any_dataset_urn,
        column_name="amount",
        metric_type="MEAN",
        operator="BETWEEN",
        criteria_parameters=(1000.0, 10000.0),
        display_name="Amount Range Check",
        detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
        incident_behavior=[
            AssertionIncidentBehavior.RAISE_ON_FAIL,
            AssertionIncidentBehavior.RESOLVE_ON_PASS,
        ],
        tags=[
            TagUrn.from_string("urn:li:tag:amount"),
            TagUrn.from_string("urn:li:tag:financial"),
        ],
        updated_by=user,
        enabled=True,
        schedule=custom_schedule,
    )

    # Act
    actual_assertion = client.sync_column_metric_assertion(**input_params.__dict__)

    # Assert
    assert actual_assertion.display_name == "Amount Range Check"
    assert actual_assertion.column_name == "amount"
    assert actual_assertion.schedule == custom_schedule
    assert len(actual_assertion.incident_behavior) == 2
    assert len(actual_assertion.tags) == 2
    assert actual_assertion.created_by == user
    assert actual_assertion.urn is not None
    assert client.client.entities.create.call_count == 2  # assertion + monitor
