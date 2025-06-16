from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Union
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
    VolumeAssertion,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
    AssertionIncidentBehavior,
    DetectionMechanism,
    DetectionMechanismInputTypes,
)
from acryl_datahub_cloud.sdk.assertion_input.volume_assertion_input import (
    RowCountChange,
    RowCountTotal,
    VolumeAssertionOperator,
    _VolumeAssertionDefinitionTypes,
)
from acryl_datahub_cloud.sdk.assertions_client import (
    DEFAULT_CREATED_BY,
    AssertionsClient,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    Assertion,
)
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from acryl_datahub_cloud.sdk.errors import (
    SDKUsageError,
)
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
    MonitorUrn,
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
class VolumeAssertionCreateParams:
    dataset_urn: Union[str, DatasetUrn]
    display_name: Optional[str] = None
    definition: Optional[_VolumeAssertionDefinitionTypes] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    created_by: Optional[CorpUserUrn] = None
    enabled: Optional[bool] = None


@dataclass
class VolumeAssertionSyncParams:
    dataset_urn: Union[str, DatasetUrn]
    urn: Optional[Union[str, AssertionUrn]] = None
    display_name: Optional[str] = None
    definition: Optional[_VolumeAssertionDefinitionTypes] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None
    enabled: Optional[bool] = None
    schedule: Optional[Union[str, models.CronScheduleClass]] = None


@freeze_time(FROZEN_TIME)
def test_create_volume_assertion_minimal_input(
    native_volume_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test creating a volume assertion with minimal input parameters."""
    # Arrange
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    input_params = VolumeAssertionCreateParams(
        dataset_urn=_any_dataset_urn,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
    )

    expected_assertion = VolumeAssertion(
        urn=None,  # type: ignore[arg-type]  # URN is generated during creation, updated with actual value in test
        dataset_urn=_any_dataset_urn,
        display_name="New Assertion",
        mode=AssertionMode.ACTIVE,
        schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
        incident_behavior=[],
        tags=[],
        created_by=DEFAULT_CREATED_BY,
        created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
        updated_by=DEFAULT_CREATED_BY,
        updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
    )

    # Act
    assertion = client._create_volume_assertion(
        dataset_urn=input_params.dataset_urn,
        display_name=input_params.display_name,
        definition=input_params.definition,
        detection_mechanism=input_params.detection_mechanism,
        incident_behavior=input_params.incident_behavior,
        tags=input_params.tags,
        created_by=input_params.created_by,
        enabled=input_params.enabled if input_params.enabled is not None else True,
    )

    # Assert - Update expected assertion with the actual URN from the created assertion
    expected_assertion._urn = assertion.urn  # Update with actual URN
    _validate_volume_assertion_created_vs_expected(
        assertion, input_params, expected_assertion
    )


@freeze_time(FROZEN_TIME)
def test_create_volume_assertion_full_input(
    native_volume_stub_datahub_client: StubDataHubClient,
) -> None:
    """Test creating a volume assertion with full input parameters."""
    # Arrange
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    input_params = VolumeAssertionCreateParams(
        dataset_urn=_any_dataset_urn,
        display_name="Test Volume Assertion",
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.BETWEEN,
            parameters=(100, 500),
        ),
        detection_mechanism=DetectionMechanism.QUERY(),
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        tags=["urn:li:tag:my_tag_1"],
        created_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
        enabled=False,
    )

    expected_assertion = VolumeAssertion(
        urn=None,  # type: ignore[arg-type]  # URN is generated during creation, updated with actual value in test
        dataset_urn=_any_dataset_urn,
        display_name="Test Volume Assertion",
        mode=AssertionMode.INACTIVE,
        schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.BETWEEN,
            parameters=(100, 500),
        ),
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        tags=[TagUrn.from_string("urn:li:tag:my_tag_1")],
        created_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
        created_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
        updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
    )

    # Act
    assertion = client._create_volume_assertion(
        dataset_urn=input_params.dataset_urn,
        display_name=input_params.display_name,
        definition=input_params.definition,
        detection_mechanism=input_params.detection_mechanism,
        incident_behavior=input_params.incident_behavior,
        tags=input_params.tags,
        created_by=input_params.created_by,
        enabled=input_params.enabled if input_params.enabled is not None else True,
    )

    # Assert - Update expected assertion with the actual URN from the created assertion
    expected_assertion._urn = assertion.urn  # Update with actual URN
    _validate_volume_assertion_created_vs_expected(
        assertion, input_params, expected_assertion
    )


def test_create_volume_assertion_entities_client_called(
    native_volume_stub_datahub_client: StubDataHubClient,
) -> None:
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing
    assertion = client._create_volume_assertion(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
    )
    assert mock_create.call_count == 2
    assert assertion


def test_create_volume_assertion_missing_definition(
    native_volume_stub_datahub_client: StubDataHubClient,
) -> None:
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    with pytest.raises(SDKUsageError, match="Volume assertion definition is required"):
        client._create_volume_assertion(
            dataset_urn=_any_dataset_urn,
            definition=None,  # Missing definition
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "enabled, expected_monitor_mode",
    [
        pytest.param(True, models.MonitorModeClass.ACTIVE, id="enabled_true"),
        pytest.param(False, models.MonitorModeClass.INACTIVE, id="enabled_false"),
    ],
)
def test_create_volume_assertion_enabled_parameter(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    enabled: bool,
    expected_monitor_mode: models.MonitorModeClass,
) -> None:
    """Test that the enabled parameter controls the monitor mode correctly."""
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    client._create_volume_assertion(
        dataset_urn=any_dataset_urn,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
        enabled=enabled,
    )

    # Verify that create was called with the correct monitor mode
    assert mock_create.call_count == 2  # assertion + monitor

    # Check the monitor entity (second call)
    monitor_entity = mock_create.call_args_list[1][0][0]
    assert monitor_entity.info.status.mode == expected_monitor_mode


@freeze_time(FROZEN_TIME)
def test_create_volume_assertion_enabled_defaults_to_true(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that the enabled parameter defaults to True when not specified."""
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create = MagicMock()
    client.client.entities.create = mock_create  # type: ignore[method-assign] # Override for testing

    # Don't specify enabled parameter
    client._create_volume_assertion(
        dataset_urn=any_dataset_urn,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
    )

    # Verify that monitor is created as ACTIVE (default enabled=True)
    assert mock_create.call_count == 2  # assertion + monitor

    monitor_entity = mock_create.call_args_list[1][0][0]
    assert monitor_entity.info.status.mode == models.MonitorModeClass.ACTIVE


@freeze_time(FROZEN_TIME)
def test_sync_volume_assertion_valid_simple_input(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    native_volume_assertion_entity_with_all_fields: Assertion,
    native_volume_monitor_with_all_fields: Monitor,
) -> None:
    """Test sync_volume_assertion with minimal input parameters."""

    # MyPy assertions - we know these are not None from the fixtures
    assert native_volume_assertion_entity_with_all_fields.description is not None
    assert native_volume_assertion_entity_with_all_fields.source is not None
    assert native_volume_assertion_entity_with_all_fields.source.created is not None
    assert (
        native_volume_assertion_entity_with_all_fields.source.created.actor is not None
    )
    assert (
        native_volume_assertion_entity_with_all_fields.source.created.time is not None
    )

    # Arrange
    input_params = VolumeAssertionSyncParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
    )
    mock_upsert = MagicMock()
    native_volume_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    assertion = client.sync_volume_assertion(
        dataset_urn=input_params.dataset_urn,
        urn=input_params.urn,
        display_name=input_params.display_name,
        definition=input_params.definition,
        detection_mechanism=input_params.detection_mechanism,
        incident_behavior=input_params.incident_behavior,
        tags=input_params.tags,
        updated_by=input_params.updated_by,
        enabled=input_params.enabled,
        schedule=input_params.schedule,
    )

    # Assert
    expected_assertion = VolumeAssertion(
        urn=any_assertion_urn,
        dataset_urn=any_dataset_urn,
        display_name=native_volume_assertion_entity_with_all_fields.description,
        mode=AssertionMode.ACTIVE,
        schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
        incident_behavior=[
            AssertionIncidentBehavior.RAISE_ON_FAIL,
            AssertionIncidentBehavior.RESOLVE_ON_PASS,
        ],
        tags=[TagUrn.from_string("urn:li:tag:native_volume_assertion_tag")],
        created_by=CorpUserUrn.from_string(
            native_volume_assertion_entity_with_all_fields.source.created.actor
        ),
        created_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        updated_by=DEFAULT_CREATED_BY,
        updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
    )

    _validate_volume_assertion_synced_vs_expected(
        assertion, input_params, expected_assertion
    )

    assert mock_upsert.call_count == 2

    called_with_assertion = mock_upsert.call_args_list[0][0][0]
    assert called_with_assertion.urn == any_assertion_urn
    assert isinstance(called_with_assertion.info, models.VolumeAssertionInfoClass)

    called_with_monitor = mock_upsert.call_args_list[1][0][0]
    assert called_with_monitor.urn == any_monitor_urn
    assert called_with_monitor.info.type == models.MonitorTypeClass.ASSERTION
    assert called_with_monitor.info.status.mode == models.MonitorModeClass.ACTIVE


@freeze_time(FROZEN_TIME)
def test_sync_volume_assertion_valid_full_input(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    native_volume_assertion_entity_with_all_fields: Assertion,
) -> None:
    """Test sync_volume_assertion with full input parameters."""

    # MyPy assertions - we know these are not None from the fixtures
    assert native_volume_assertion_entity_with_all_fields.source is not None
    assert native_volume_assertion_entity_with_all_fields.source.created is not None
    assert (
        native_volume_assertion_entity_with_all_fields.source.created.actor is not None
    )

    # Arrange
    input_params = VolumeAssertionSyncParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        display_name="test_display_name",
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.BETWEEN,
            parameters=(50, 150),
        ),
        detection_mechanism=DetectionMechanism.QUERY(),
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        tags=[TagUrn.from_string("urn:li:tag:test_tag")],
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
        enabled=False,
    )
    mock_upsert = MagicMock()
    native_volume_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act
    assertion = client.sync_volume_assertion(
        dataset_urn=input_params.dataset_urn,
        urn=input_params.urn,
        display_name=input_params.display_name,
        definition=input_params.definition,
        detection_mechanism=input_params.detection_mechanism,
        incident_behavior=input_params.incident_behavior,
        tags=input_params.tags,
        updated_by=input_params.updated_by,
        enabled=input_params.enabled,
        schedule=input_params.schedule,
    )

    # Assert
    expected_assertion = VolumeAssertion(
        urn=any_assertion_urn,
        dataset_urn=any_dataset_urn,
        display_name="test_display_name",
        mode=AssertionMode.INACTIVE,  # enabled=False
        schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.BETWEEN,
            parameters=(50, 150),
        ),
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        tags=[TagUrn.from_string("urn:li:tag:test_tag")],
        created_by=CorpUserUrn.from_string(
            native_volume_assertion_entity_with_all_fields.source.created.actor
        ),
        created_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:test_user"),
        updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
    )

    _validate_volume_assertion_synced_vs_expected(
        assertion, input_params, expected_assertion
    )

    assert mock_upsert.call_count == 2

    called_with_assertion = mock_upsert.call_args_list[0][0][0]
    assert called_with_assertion.urn == any_assertion_urn
    assert isinstance(called_with_assertion.info, models.VolumeAssertionInfoClass)

    called_with_monitor = mock_upsert.call_args_list[1][0][0]
    assert called_with_monitor.urn == any_monitor_urn
    assert called_with_monitor.info.type == models.MonitorTypeClass.ASSERTION
    assert (
        called_with_monitor.info.status.mode == models.MonitorModeClass.INACTIVE
    )  # enabled=False


@pytest.mark.parametrize(
    "urn, expected_create_assertion_call_count, expected_upsert_entity_call_count",
    [
        pytest.param(None, 1, 0, id="urn_is_none"),
        pytest.param(
            AssertionUrn.from_string("urn:li:assertion:test"),
            0,
            2,
            id="urn_is_not_none",
        ),
    ],
)
def test_sync_volume_assertion_calls_create_assertion_if_urn_is_not_set(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    urn: Optional[Union[str, AssertionUrn]],
    expected_create_assertion_call_count: int,
    expected_upsert_entity_call_count: int,
) -> None:
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert_entity = MagicMock()
    client.client.entities.upsert = mock_upsert_entity  # type: ignore[method-assign] # Override for testing
    mock_create_assertion = MagicMock()
    client._create_volume_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    client.sync_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=urn,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
    )
    assert mock_create_assertion.call_count == expected_create_assertion_call_count
    assert mock_upsert_entity.call_count == expected_upsert_entity_call_count
    if urn is None:
        assert mock_create_assertion.call_args[1]["dataset_urn"] == any_dataset_urn


@pytest.mark.parametrize(
    "updated_by, expected_updated_by",
    [
        pytest.param(None, DEFAULT_CREATED_BY, id="no_updated_by_set"),
        pytest.param(
            CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            CorpUserUrn.from_string("urn:li:corpuser:test_user"),
            id="updated_by_set",
        ),
    ],
)
def test_sync_volume_assertion_uses_default_if_updated_by_is_not_set(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    updated_by: Optional[CorpUserUrn],
    expected_updated_by: CorpUserUrn,
) -> None:
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_create_assertion = MagicMock()
    client.client.entities.create = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    native_volume_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    client.sync_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        updated_by=updated_by,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
    )
    assertion_entity_upserted = mock_upsert.call_args_list[0][0][0]
    assert assertion_entity_upserted.last_updated.actor == str(expected_updated_by)
    assert mock_create_assertion.call_count == 0


def test_sync_volume_assertion_calls_create_if_assertion_and_monitor_entities_do_not_exist(
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    any_monitor_urn: MonitorUrn,
) -> None:
    empty_stub_datahub_client = (
        StubDataHubClient()
    )  # Assertion and Monitor entities do not exist
    assert empty_stub_datahub_client.entities.get(any_assertion_urn) is None
    assert empty_stub_datahub_client.entities.get(any_monitor_urn) is None

    client = AssertionsClient(empty_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    empty_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    mock_create_assertion = MagicMock(
        return_value=VolumeAssertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            display_name="Mock assertion",
            mode=AssertionMode.ACTIVE,
            schedule=DEFAULT_EVERY_SIX_HOURS_SCHEDULE,
            incident_behavior=[],
            tags=[],
            definition=RowCountTotal(
                operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
                parameters=100,
            ),
        )
    )
    client._create_volume_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    client.sync_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
    )
    assert mock_upsert.call_count == 0
    assert mock_create_assertion.call_count == 1
    assert mock_create_assertion.call_args[1]["dataset_urn"] == any_dataset_urn


def test_sync_volume_assertion_raises_error_if_assertion_and_input_have_different_dataset_urns(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
) -> None:
    # volume_stub_datahub_client entity client returns assertion and monitor
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    with pytest.raises(SDKUsageError, match="Dataset URN mismatch"):
        client.sync_volume_assertion(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:test,not_the_same_dataset_urn,PROD)",
            urn=any_assertion_urn,
            definition=RowCountTotal(
                operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
                parameters=100,
            ),
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "enabled, expected_assertion_mode",
    [
        pytest.param(True, AssertionMode.ACTIVE, id="enabled_true"),
        pytest.param(False, AssertionMode.INACTIVE, id="enabled_false"),
        pytest.param(None, AssertionMode.ACTIVE, id="enabled_none_preserves_existing"),
    ],
)
def test_sync_volume_assertion_enabled_parameter_merging(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    native_volume_assertion_entity_with_all_fields: Assertion,
    native_volume_monitor_with_all_fields: Monitor,
    enabled: Optional[bool],
    expected_assertion_mode: AssertionMode,
) -> None:
    """Test that the enabled parameter merges correctly with existing values."""
    # Set existing monitor to ACTIVE state
    native_volume_monitor_with_all_fields.info.status.mode = (
        models.MonitorModeClass.ACTIVE
    )

    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    client.client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    result = client.sync_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        enabled=enabled,
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
    )

    # Verify the returned assertion has the expected mode
    assert result.mode == expected_assertion_mode

    # Verify the monitor entity was upserted with correct mode
    assert mock_upsert.call_count == 2  # assertion + monitor
    monitor_entity = mock_upsert.call_args_list[1][0][0]
    expected_monitor_mode = (
        models.MonitorModeClass.ACTIVE
        if expected_assertion_mode == AssertionMode.ACTIVE
        else models.MonitorModeClass.INACTIVE
    )
    assert monitor_entity.info.status.mode == expected_monitor_mode


@freeze_time(FROZEN_TIME)
def test_sync_volume_assertion_enabled_none_preserves_inactive(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    native_volume_assertion_entity_with_all_fields: Assertion,
    native_volume_monitor_with_all_fields: Monitor,
) -> None:
    """Test that enabled=None preserves existing INACTIVE state."""
    # Set existing monitor to INACTIVE state
    native_volume_monitor_with_all_fields.info.status.mode = (
        models.MonitorModeClass.INACTIVE
    )

    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    client.client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    result = client.sync_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        enabled=None,  # Should preserve existing state
        definition=RowCountTotal(
            operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
            parameters=100,
        ),
    )

    # Verify the returned assertion preserves INACTIVE mode
    assert result.mode == AssertionMode.INACTIVE

    # Verify the monitor entity was upserted with INACTIVE mode preserved
    assert mock_upsert.call_count == 2  # assertion + monitor
    monitor_entity = mock_upsert.call_args_list[1][0][0]
    assert monitor_entity.info.status.mode == models.MonitorModeClass.INACTIVE


@freeze_time(FROZEN_TIME)
def test_sync_volume_assertion_enabled_calls_create_with_enabled_when_urn_is_none(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that sync passes enabled parameter to create when urn is None."""
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Mock the create method to verify it's called with enabled parameter
    with patch.object(client, "_create_volume_assertion") as mock_create:
        mock_create.return_value = MagicMock()  # Return a mock assertion

        client.sync_volume_assertion(
            dataset_urn=any_dataset_urn,
            urn=None,  # This should trigger create
            enabled=False,
            definition=RowCountTotal(
                operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
                parameters=100,
            ),
        )

        # Verify create was called with enabled=False
        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args[1]
        assert call_kwargs["enabled"] is False


@freeze_time(FROZEN_TIME)
def test_sync_volume_assertion_preserves_definition_from_backend_when_none_provided(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    native_volume_assertion_entity_with_all_fields: Assertion,
    native_volume_monitor_with_all_fields: Monitor,
) -> None:
    """Test that sync_volume_assertion preserves the existing definition from backend when definition=None."""
    # Arrange
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    native_volume_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    # The existing assertion from fixtures has a RowCountTotal definition with GREATER_THAN_OR_EQUAL_TO operator and value "100"
    existing_assertion = VolumeAssertion._from_entities(
        native_volume_assertion_entity_with_all_fields,
        native_volume_monitor_with_all_fields,
    )
    existing_definition = existing_assertion.definition

    # Verify the fixture definition to make test more explicit
    assert isinstance(existing_definition, RowCountTotal)
    assert (
        existing_definition.operator == VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO
    )
    assert existing_definition.parameters == 100.0  # Should be parsed as float

    # Act - call sync_volume_assertion without providing a definition (definition=None)
    # This should preserve the existing definition from the backend
    result_assertion = client.sync_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        display_name="Updated Display Name",  # Provide other fields to show they get updated
        definition=None,  # Explicitly pass None to test definition preservation
    )

    # Assert - verify the definition was preserved from the backend
    assert result_assertion.definition == existing_definition
    assert (
        result_assertion.display_name == "Updated Display Name"
    )  # Verify other fields were updated

    # Also verify the preserved definition properties explicitly
    assert isinstance(result_assertion.definition, RowCountTotal)
    assert (
        result_assertion.definition.operator
        == VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO
    )
    assert result_assertion.definition.parameters == 100.0

    # Verify that upsert was called
    assert mock_upsert.call_count == 2  # assertion + monitor

    # Verify the assertion entity that was upserted has the preserved definition
    upserted_assertion = mock_upsert.call_args_list[0][0][0]
    assert upserted_assertion.urn == any_assertion_urn
    assert isinstance(upserted_assertion.info, models.VolumeAssertionInfoClass)

    # Verify the preserved definition is reflected in the upserted entity's info
    assert (
        upserted_assertion.info.type == models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL
    )
    assert upserted_assertion.info.rowCountTotal is not None
    assert (
        upserted_assertion.info.rowCountTotal.operator
        == models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    )
    assert upserted_assertion.info.rowCountTotal.parameters is not None
    assert upserted_assertion.info.rowCountTotal.parameters.value is not None
    assert upserted_assertion.info.rowCountTotal.parameters.value.value == "100"


def test_sync_volume_assertion_raises_error_when_no_definition_provided_and_no_backend_definition(
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
) -> None:
    """Test that sync_volume_assertion raises proper error when definition=None and no backend assertion exists.

    Basically, this tests prevents the case of a sync operation without a definition when the sync is a creation.
    """
    # Arrange - create empty stub client (no assertion entities exist)
    empty_stub_datahub_client = StubDataHubClient()  # No entities
    client = AssertionsClient(empty_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    # Act & Assert - should raise error when no backend assertion exists and no definition provided
    with pytest.raises(
        SDKUsageError,
        match="Cannot sync assertion .* no existing definition found in backend and no definition provided in request",
    ):
        client.sync_volume_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,  # This assertion doesn't exist in backend
            display_name="Test Assertion",
            definition=None,  # No definition provided
        )


@freeze_time(FROZEN_TIME)
def test_sync_volume_assertion_accepts_dictionary_definition(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    native_volume_assertion_entity_with_all_fields: Assertion,
    native_volume_monitor_with_all_fields: Monitor,
) -> None:
    """Test that sync_volume_assertion accepts dictionary definition input and parses it correctly."""
    # Arrange
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    native_volume_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing

    # Use dictionary definition instead of typed object
    dict_definition = {
        "type": "row_count_total",
        "operator": "BETWEEN",
        "parameters": (50, 150),
    }

    # Act - call sync_volume_assertion with dictionary definition
    result_assertion = client.sync_volume_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        display_name="Test Dictionary Definition",
        definition=dict_definition,  # Dictionary input
    )

    # Assert - verify the definition was parsed correctly
    assert isinstance(result_assertion.definition, RowCountTotal)
    assert result_assertion.definition.operator == VolumeAssertionOperator.BETWEEN
    assert result_assertion.definition.parameters == (50, 150)
    assert result_assertion.display_name == "Test Dictionary Definition"

    # Verify that upsert was called
    assert mock_upsert.call_count == 2  # assertion + monitor

    # Verify the assertion entity that was upserted has the correct definition
    upserted_assertion = mock_upsert.call_args_list[0][0][0]
    assert upserted_assertion.urn == any_assertion_urn
    assert isinstance(upserted_assertion.info, models.VolumeAssertionInfoClass)

    # Verify the parsed definition is reflected in the upserted entity's info
    assert (
        upserted_assertion.info.type == models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL
    )
    assert upserted_assertion.info.rowCountTotal is not None
    assert (
        upserted_assertion.info.rowCountTotal.operator
        == models.AssertionStdOperatorClass.BETWEEN
    )
    assert upserted_assertion.info.rowCountTotal.parameters is not None
    assert upserted_assertion.info.rowCountTotal.parameters.minValue is not None
    assert upserted_assertion.info.rowCountTotal.parameters.maxValue is not None
    assert upserted_assertion.info.rowCountTotal.parameters.minValue.value == "50"
    assert upserted_assertion.info.rowCountTotal.parameters.maxValue.value == "150"


@freeze_time(FROZEN_TIME)
def test_create_volume_assertion_accepts_dictionary_definition(
    native_volume_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that _create_volume_assertion accepts dictionary definition input and parses it correctly."""
    # Arrange
    client = AssertionsClient(native_volume_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing

    # Use dictionary definition for row count change
    dict_definition = {
        "type": "row_count_change",
        "kind": "percent",
        "operator": "GREATER_THAN_OR_EQUAL_TO",
        "parameters": 15,
    }

    # Act - call _create_volume_assertion with dictionary definition
    result_assertion = client._create_volume_assertion(
        dataset_urn=any_dataset_urn,
        display_name="Test Dictionary Create",
        definition=dict_definition,  # Dictionary input
    )

    # Assert - verify the definition was parsed correctly
    assert isinstance(result_assertion.definition, RowCountChange)
    assert result_assertion.definition.kind == "percent"
    assert (
        result_assertion.definition.operator
        == VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO
    )
    assert result_assertion.definition.parameters == 15
    assert result_assertion.display_name == "Test Dictionary Create"


def _validate_volume_assertion_created_vs_expected(
    assertion: VolumeAssertion,
    input_params: VolumeAssertionCreateParams,
    expected_assertion: VolumeAssertion,
) -> None:
    if input_params.display_name is not None:
        assert assertion.display_name == expected_assertion.display_name
    else:
        assert assertion.display_name.startswith(
            "New Assertion"
        )  # Generated display name
        assert len(assertion.display_name) == GENERATED_DISPLAY_NAME_LENGTH

    assert assertion.definition == expected_assertion.definition
    assert assertion.incident_behavior == expected_assertion.incident_behavior
    assert assertion.tags == expected_assertion.tags
    assert assertion.created_by == expected_assertion.created_by
    assert assertion.created_at == expected_assertion.created_at
    assert assertion.updated_by == expected_assertion.updated_by
    assert assertion.updated_at == expected_assertion.updated_at
    assert assertion.mode == expected_assertion.mode


def _validate_volume_assertion_synced_vs_expected(
    assertion: VolumeAssertion,
    input_params: VolumeAssertionSyncParams,
    expected_assertion: VolumeAssertion,
) -> None:
    if input_params.display_name is not None:
        assert assertion.display_name == expected_assertion.display_name
    else:
        # For sync operations, we expect the existing display name to be preserved
        assert assertion.display_name == expected_assertion.display_name

    assert assertion.definition == expected_assertion.definition
    assert assertion.incident_behavior == expected_assertion.incident_behavior
    assert assertion.tags == expected_assertion.tags
    assert assertion.created_by == expected_assertion.created_by
    assert assertion.created_at == expected_assertion.created_at
    assert assertion.updated_by == expected_assertion.updated_by
    assert assertion.updated_at == expected_assertion.updated_at
    assert assertion.mode == expected_assertion.mode
