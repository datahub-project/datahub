from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional, TypedDict, Union
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from acryl_datahub_cloud._sdk_extras.assertion import (
    AssertionMode,
    SmartFreshnessAssertion,
)
from acryl_datahub_cloud._sdk_extras.assertion_input import (
    _DETECTION_MECHANISM_CONCRETE_TYPES,
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_SENSITIVITY,
    AssertionIncidentBehavior,
    DetectionMechanism,
    DetectionMechanismInputTypes,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
    _DetectionMechanismTypes,
    _SmartFreshnessAssertionInput,
)
from acryl_datahub_cloud._sdk_extras.assertions_client import (
    DEFAULT_CREATED_BY,
    AssertionsClient,
    _merge_field,
)
from acryl_datahub_cloud._sdk_extras.entities.assertion import (
    Assertion,
    TagsInputType,
)
from acryl_datahub_cloud._sdk_extras.entities.monitor import Monitor
from acryl_datahub_cloud._sdk_extras.errors import (
    SDKUsageError,
)
from datahub.emitter.mce_builder import parse_ts_millis
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
    MonitorUrn,
    TagUrn,
)
from tests.conftest import StubDataHubClient, StubEntityClient

FROZEN_TIME = "2025-01-01 10:30:00"

_any_dataset_urn = DatasetUrn.from_string(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
)

GENERATED_DISPLAY_NAME_LENGTH = 22


@dataclass
class SmartFreshnessAssertionInputParams:
    dataset_urn: Union[str, DatasetUrn]
    display_name: Optional[str] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    sensitivity: Optional[InferenceSensitivity] = None
    exclusion_windows: Optional[list[FixedRangeExclusionWindow]] = None
    training_data_lookback_days: Optional[int] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    created_by: Optional[CorpUserUrn] = None


@dataclass
class SmartFreshnessAssertionOutputParams:
    dataset_urn: Union[str, DatasetUrn]
    display_name: str
    detection_mechanism: _DetectionMechanismTypes
    sensitivity: InferenceSensitivity
    exclusion_windows: list[FixedRangeExclusionWindow]
    training_data_lookback_days: int
    incident_behavior: list[AssertionIncidentBehavior]
    tags: TagsInputType
    created_by: CorpUserUrn
    created_at: datetime
    updated_by: CorpUserUrn
    updated_at: datetime


@dataclass
class SmartFreshnessAssertionUpsertInputParams:
    dataset_urn: Union[str, DatasetUrn]
    urn: Optional[Union[str, AssertionUrn]] = None
    display_name: Optional[str] = None
    detection_mechanism: Optional[DetectionMechanismInputTypes] = None
    sensitivity: Optional[InferenceSensitivity] = None
    exclusion_windows: Optional[list[FixedRangeExclusionWindow]] = None
    training_data_lookback_days: Optional[int] = None
    incident_behavior: Optional[list[AssertionIncidentBehavior]] = None
    tags: Optional[TagsInputType] = None
    updated_by: Optional[CorpUserUrn] = None


@freeze_time(FROZEN_TIME)
def test__upsert_and_merge_smart_freshness_assertion_valid_simple_input(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_monitor_urn: MonitorUrn,
    any_assertion_urn: AssertionUrn,
    monitor_with_all_fields: Monitor,
    assertion_entity_with_all_fields: Assertion,
) -> None:
    field_name = "field"
    field_type = "DateTypeClass"
    input_params = SmartFreshnessAssertionUpsertInputParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
            column_name=field_name, additional_filter=None
        ),
        display_name="Explicitly Input Display Name",
    )

    # Arrange
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub

    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType="nativeType",
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )

        # Act
        assertion = client._upsert_and_merge_smart_freshness_assertion(
            **asdict(input_params)
        )

    # Assert
    _validate_assertion_vs_input(
        assertion,
        input_params,
        SmartFreshnessAssertionOutputParams(
            dataset_urn=input_params.dataset_urn,
            display_name=input_params.display_name or "",
            detection_mechanism=DetectionMechanism.LAST_MODIFIED_COLUMN(
                column_name="field", additional_filter=None
            ),
            sensitivity=InferenceSensitivity.LOW,  # From the stored monitor entity
            exclusion_windows=[  # This is set on the stored test entity, not the input params
                FixedRangeExclusionWindow(
                    start=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    end=datetime(2021, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                )
            ],
            training_data_lookback_days=99,  # From the stored monitor entity
            incident_behavior=[
                AssertionIncidentBehavior.RAISE_ON_FAIL,
                AssertionIncidentBehavior.RESOLVE_ON_PASS,
            ],  # This is set on the stored test entity, not the input params
            tags=[TagUrn.from_string("urn:li:tag:smart_freshness_assertion_tag")],
            created_by=CorpUserUrn.from_string(
                "urn:li:corpuser:acryl-cloud-user-created"
            ),
            created_at=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            updated_by=CorpUserUrn.from_string("urn:li:corpuser:__datahub_system"),
            updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
        ),
    )

    assert mock_upsert.call_count == 2

    called_with_assertion = mock_upsert.call_args_list[0][0][0]
    assert called_with_assertion.urn == any_assertion_urn
    assert isinstance(called_with_assertion.info, models.FreshnessAssertionInfoClass)
    assert isinstance(
        assertion_entity_with_all_fields.info, models.FreshnessAssertionInfoClass
    )
    assert called_with_assertion.info.type == assertion_entity_with_all_fields.info.type
    assert (
        called_with_assertion.info.entity
        == assertion_entity_with_all_fields.info.entity
    )

    called_with_monitor = mock_upsert.call_args_list[1][0][0]
    assert called_with_monitor.urn == any_monitor_urn
    assert called_with_monitor.info.type == monitor_with_all_fields.info.type
    assert (
        called_with_monitor.info.status.mode == monitor_with_all_fields.info.status.mode
    )
    assert called_with_monitor.info.assertionMonitor.assertions[0].assertion == str(
        assertion_entity_with_all_fields.urn
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].schedule.cron
        == _SmartFreshnessAssertionInput.DEFAULT_SCHEDULE.cron
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].schedule.timezone
        == _SmartFreshnessAssertionInput.DEFAULT_SCHEDULE.timezone
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[0].parameters.type
        == models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[
            0
        ].parameters.datasetFreshnessParameters.sourceType
        == models.DatasetFreshnessSourceTypeClass.FIELD_VALUE
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[
            0
        ].parameters.datasetFreshnessParameters.field.path
        == field_name
    )
    assert (
        called_with_monitor.info.assertionMonitor.assertions[
            0
        ].parameters.datasetFreshnessParameters.field.type
        == field_type
    )


def test__upsert_and_merge_smart_freshness_assertion_calls_create_assertion_if_urn_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    mock_create_assertion = MagicMock()
    client.create_smart_freshness_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    client._upsert_and_merge_smart_freshness_assertion(
        dataset_urn=any_dataset_urn,
        urn=None,
    )
    assert mock_create_assertion.call_count == 1
    assert mock_create_assertion.call_args[1]["dataset_urn"] == any_dataset_urn
    assert mock_upsert.call_count == 0


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
def test__upsert_and_merge_smart_freshness_assertion_uses_default_if_updated_by_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    updated_by: Optional[CorpUserUrn],
    expected_updated_by: CorpUserUrn,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType="nativeType",
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            updated_by=updated_by,
        )
    assertion_entity_upserted = mock_upsert.call_args_list[0][0][0]
    assert assertion_entity_upserted.last_updated.actor == str(expected_updated_by)


def test__upsert_and_merge_smart_freshness_assertion_calls_create_if_assertion_and_monitor_entities_do_not_exist(
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    any_monitor_urn: MonitorUrn,
) -> None:
    stub_datahub_client = (
        StubDataHubClient()
    )  # Assertion and Monitor entities do not exist
    assert stub_datahub_client.entities.get(any_assertion_urn) is None
    assert stub_datahub_client.entities.get(any_monitor_urn) is None

    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    mock_create_assertion = MagicMock(
        return_value=SmartFreshnessAssertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            display_name="Mock assertion",
            mode=AssertionMode.ACTIVE,
            exclusion_windows=[],
            incident_behavior=[],
            tags=[],
        )
    )
    client.create_smart_freshness_assertion = mock_create_assertion  # type: ignore[method-assign] # Override for testing
    client._upsert_and_merge_smart_freshness_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
    )
    assert mock_upsert.call_count == 0
    assert mock_create_assertion.call_count == 1
    assert mock_create_assertion.call_args[1]["dataset_urn"] == any_dataset_urn


@freeze_time(FROZEN_TIME)
def test__upsert_and_merge_smart_freshness_assertion_calls_upsert_if_assertion_exists_but_monitor_does_not(
    stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
    any_monitor_urn: MonitorUrn,
    assertion_entity_with_all_fields: Assertion,
) -> None:
    # Arrange
    stub_datahub_client = StubDataHubClient(
        entity_client=StubEntityClient(
            assertion_entity=assertion_entity_with_all_fields,
            monitor_entity=None,
        )
    )
    assert stub_datahub_client.entities.get(any_assertion_urn) is not None
    assert stub_datahub_client.entities.get(any_monitor_urn) is None

    # Act
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.create = MagicMock()  # type: ignore[method-assign] # Override for testing
    mock_upsert = MagicMock()
    stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign] # Override for testing
    input_params = SmartFreshnessAssertionUpsertInputParams(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        display_name="Explicitly Input Display Name",
    )
    assertion = client._upsert_and_merge_smart_freshness_assertion(
        **asdict(input_params)
    )

    # Assert
    assert mock_upsert.call_count == 2
    _validate_assertion_vs_input(
        assertion,
        input_params,
        SmartFreshnessAssertionOutputParams(
            dataset_urn=any_dataset_urn,
            display_name="Explicitly Input Display Name",
            detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
            sensitivity=InferenceSensitivity.MEDIUM,  # From the stored monitor entity
            exclusion_windows=[],
            training_data_lookback_days=ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,  # From the stored monitor entity
            incident_behavior=[
                AssertionIncidentBehavior.RAISE_ON_FAIL,
                AssertionIncidentBehavior.RESOLVE_ON_PASS,
            ],  # This is set on the stored test entity, not the input params
            tags=[TagUrn.from_string("urn:li:tag:smart_freshness_assertion_tag")],
            created_by=CorpUserUrn.from_string(
                "urn:li:corpuser:acryl-cloud-user-created"
            ),
            created_at=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            updated_by=CorpUserUrn.from_string("urn:li:corpuser:__datahub_system"),
            updated_at=datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
        ),
    )


def test__upsert_and_merge_smart_freshness_assertion_raises_error_if_assertion_and_input_have_different_dataset_urns(
    stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
) -> None:
    # stub_datahub_client entity client returns assertion and monitor
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    with pytest.raises(SDKUsageError, match="Dataset URN mismatch"):
        client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:test,not_the_same_dataset_urn,PROD)",
            urn=any_assertion_urn,
        )


def test__upsert_and_merge_smart_freshness_assertion_uses_existing_assertion_display_name_if_input_display_name_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    assertion_entity_with_all_fields: Assertion,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType=field_type,
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        assertion = client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            display_name=None,
        )
    assert assertion.display_name == assertion_entity_with_all_fields.description


def test__upsert_and_merge_smart_freshness_assertion_uses_empty_display_name_if_existing_assertion_has_no_display_name(
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    assertion_entity_with_all_fields: Assertion,
) -> None:
    assertion_entity_with_all_fields.set_description(None)  # type: ignore[arg-type] # Setting to None for testing
    client = AssertionsClient(
        StubDataHubClient(  # type: ignore[arg-type]  # Stub
            StubEntityClient(
                assertion_entity=Assertion(
                    description=None,
                    info=models.FreshnessAssertionInfoClass(
                        type=models.AssertionTypeClass.FRESHNESS,
                        entity=str(any_dataset_urn),
                    ),
                )
            )
        )
    )  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    assertion = client._upsert_and_merge_smart_freshness_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        display_name=None,
    )
    assert (
        assertion.display_name == ""
    )  # Set to empty string in SmartFreshnessAssertion


def test__upsert_and_merge_smart_freshness_assertion_uses_existing_assertion_exclusion_windows_if_input_exclusion_windows_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    monitor_with_all_fields: Monitor,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType=field_type,
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        assertion = client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            exclusion_windows=None,
        )

    assert monitor_with_all_fields.info.assertionMonitor
    assert monitor_with_all_fields.info.assertionMonitor.settings
    assert monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings
    assert monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.exclusionWindows
    assert len(assertion.exclusion_windows) == len(
        monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.exclusionWindows
    )
    for i in range(len(assertion.exclusion_windows)):
        fixed_range = monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.exclusionWindows[
            i
        ].fixedRange
        assert fixed_range is not None
        assert assertion.exclusion_windows[i].start == parse_ts_millis(
            fixed_range.startTimeMillis
        )
        assert assertion.exclusion_windows[i].end == parse_ts_millis(
            fixed_range.endTimeMillis
        )


def test__upsert_and_merge_smart_freshness_assertion_uses_existing_assertion_incident_behavior_if_input_incident_behavior_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    assertion_entity_with_all_fields: Assertion,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType=field_type,
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        assertion = client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            incident_behavior=None,
        )
    assert len(assertion.incident_behavior) == len(
        assertion_entity_with_all_fields.on_failure
    ) + len(assertion_entity_with_all_fields.on_success)


def test__upsert_and_merge_smart_freshness_assertion_uses_existing_assertion_tags_if_input_tags_is_not_set(
    stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    assertion_entity_with_all_fields: Assertion,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType=field_type,
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        assertion = client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            tags=None,
        )
    assert assertion.tags
    assert assertion_entity_with_all_fields.tags
    assert len(assertion.tags) == len(assertion_entity_with_all_fields.tags)
    for i in range(len(assertion.tags)):
        assert str(assertion.tags[i]) == str(
            assertion_entity_with_all_fields.tags[i].tag
        )


def test__upsert_and_merge_smart_freshness_assertion_uses_input_display_name_if_input_display_name_is_set(
    stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType=field_type,
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        assertion = client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            display_name="",
        )
    assert assertion.display_name == ""


def test__upsert_and_merge_smart_freshness_assertion_uses_input_exclusion_windows_if_input_exclusion_windows_is_set(
    stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    monitor_with_all_fields: Monitor,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType=field_type,
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        assertion = client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            exclusion_windows=[],  # Empty list means set to no exclusion windows
        )
    assert monitor_with_all_fields.info.assertionMonitor
    assert monitor_with_all_fields.info.assertionMonitor.settings
    assert monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings
    assert monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.exclusionWindows
    length_of_existing_monitor_exclusion_windows = len(
        monitor_with_all_fields.info.assertionMonitor.settings.adjustmentSettings.exclusionWindows
    )
    # There are existing exclusion windows in the monitor, but we are setting this to the empty list so the
    # resulting exclusion windows should be empty
    assert length_of_existing_monitor_exclusion_windows == 1
    assert assertion.exclusion_windows == []
    assert (
        len(assertion.exclusion_windows) != length_of_existing_monitor_exclusion_windows
    )


def test__upsert_and_merge_smart_freshness_assertion_uses_input_incident_behavior_if_input_incident_behavior_is_set(
    stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    monitor_with_all_fields: Monitor,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType=field_type,
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        assertion = client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            incident_behavior=[],  # Empty list means set to no incident behavior
        )
    assert assertion.incident_behavior == []


def test__upsert_and_merge_smart_freshness_assertion_uses_input_tags_if_input_tags_is_set(
    stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
    any_dataset_urn: DatasetUrn,
    monitor_with_all_fields: Monitor,
) -> None:
    client = AssertionsClient(stub_datahub_client)  # type: ignore[arg-type]  # Stub
    client.client.entities.upsert = MagicMock()  # type: ignore[method-assign] # Override for testing
    field_name = "field"
    field_type = "DateTypeClass"
    with patch.object(
        _SmartFreshnessAssertionInput, "_create_field_spec", new_callable=MagicMock
    ) as mock_create_field_spec:
        mock_create_field_spec.return_value = models.FreshnessFieldSpecClass(
            path=field_name,
            type=field_type,
            nativeType=field_type,
            kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
        )
        assertion = client._upsert_and_merge_smart_freshness_assertion(
            dataset_urn=any_dataset_urn,
            urn=any_assertion_urn,
            tags=[],  # Empty list means set to no tags
        )
    assert assertion.tags == []


class _OtherSmartFreshnessAssertionInputInputParams(TypedDict, total=False):
    sensitivity: InferenceSensitivity
    exclusion_windows: list[FixedRangeExclusionWindow]
    training_data_lookback_days: int
    incident_behavior: list[AssertionIncidentBehavior]
    tags: TagsInputType
    created_by: CorpUserUrn
    created_at: datetime
    updated_by: CorpUserUrn
    updated_at: datetime


class _OtherSmartFreshnessAssertionInputInputParamsWithDisplayName(
    TypedDict, total=False
):
    display_name: str
    sensitivity: InferenceSensitivity
    exclusion_windows: list[FixedRangeExclusionWindow]
    training_data_lookback_days: int
    incident_behavior: list[AssertionIncidentBehavior]
    created_by: CorpUserUrn
    created_at: datetime
    updated_by: CorpUserUrn
    updated_at: datetime


_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS: _OtherSmartFreshnessAssertionInputInputParams = {
    "sensitivity": DEFAULT_SENSITIVITY,
    "exclusion_windows": [],
    "training_data_lookback_days": ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    "incident_behavior": [],
    "tags": [],
    "created_by": DEFAULT_CREATED_BY,
    "created_at": datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
    "updated_by": DEFAULT_CREATED_BY,
    "updated_at": datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
}

_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS_NO_TAGS: _OtherSmartFreshnessAssertionInputInputParamsWithDisplayName = {
    "display_name": "some_default_name_for_testing",
    "sensitivity": DEFAULT_SENSITIVITY,
    "exclusion_windows": [],
    "training_data_lookback_days": ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    "incident_behavior": [],
    "created_by": DEFAULT_CREATED_BY,
    "created_at": datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
    "updated_by": DEFAULT_CREATED_BY,
    "updated_at": datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
}


class _OtherSmartFreshnessAssertionInputParams(TypedDict, total=False):
    mode: AssertionMode
    exclusion_windows: list[FixedRangeExclusionWindow]
    incident_behavior: list[AssertionIncidentBehavior]
    tags: list[TagUrn]


_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS: _OtherSmartFreshnessAssertionInputParams = {
    "mode": AssertionMode.ACTIVE,
    "exclusion_windows": [],
    "incident_behavior": [],
    "tags": [],
}


class _OtherSmartFreshnessAssertionInputParamsWithDisplayName(TypedDict, total=False):
    display_name: str
    mode: AssertionMode
    exclusion_windows: list[FixedRangeExclusionWindow]
    incident_behavior: list[AssertionIncidentBehavior]


_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS_NO_TAGS: _OtherSmartFreshnessAssertionInputParamsWithDisplayName = {
    "display_name": "some_default_name_for_testing",
    "mode": AssertionMode.ACTIVE,
    "exclusion_windows": [],
    "incident_behavior": [],
}


@pytest.mark.parametrize(
    "input_field_value, input_field_name, validated_assertion_input, validated_existing_assertion, existing_entity_value, expected_value",
    [
        # Case 1: Input is None, existing value is not None -> use existing value
        pytest.param(
            None,
            "display_name",
            _SmartFreshnessAssertionInput(
                dataset_urn=_any_dataset_urn,
                entity_client=MagicMock(),
                display_name="default_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS,
            ),
            SmartFreshnessAssertion(
                urn=AssertionUrn("urn:li:assertion:test"),
                dataset_urn=DatasetUrn.from_string(_any_dataset_urn),
                display_name="existing_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS,
            ),
            "existing_entity_value",
            "existing_entity_value",
            id="input_none_existing_not_none",
        ),
        # Case 2: Input is None, existing value is None, validated_existing_assertion has value -> use validated_existing_assertion value (default value)
        pytest.param(
            None,
            "display_name",
            _SmartFreshnessAssertionInput(
                dataset_urn=_any_dataset_urn,
                entity_client=MagicMock(),
                display_name="default_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS,
            ),
            SmartFreshnessAssertion(
                urn=AssertionUrn("urn:li:assertion:test"),
                dataset_urn=DatasetUrn.from_string(_any_dataset_urn),
                display_name="existing_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS,
            ),
            None,
            "existing_name",
            id="input_none_existing_none_assertion_has_value",
        ),
        # Case 3: Input is None, existing value is None, existing assertion has None -> use default from input
        pytest.param(
            None,
            "display_name",
            _SmartFreshnessAssertionInput(
                dataset_urn=_any_dataset_urn,
                entity_client=MagicMock(),
                display_name="default_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS,
            ),
            SmartFreshnessAssertion(
                urn=AssertionUrn("urn:li:assertion:test"),
                dataset_urn=DatasetUrn.from_string(_any_dataset_urn),
                display_name=None,  # type: ignore[arg-type]  # Intentionally set to None to test the default input value
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS,
            ),
            None,
            "default_name",
            id="input_none_existing_none_assertion_none",
        ),
        # Case 4: Input is set (non-falsy), existing value is not None -> use input value
        pytest.param(
            "new_name",
            "display_name",
            _SmartFreshnessAssertionInput(
                dataset_urn=_any_dataset_urn,
                entity_client=MagicMock(),
                display_name="default_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS,
            ),
            SmartFreshnessAssertion(
                urn=AssertionUrn("urn:li:assertion:test"),
                dataset_urn=DatasetUrn.from_string(_any_dataset_urn),
                display_name="existing_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS,
            ),
            "existing_entity_value",
            "new_name",
            id="input_set_existing_not_none",
        ),
        # Case 5: Input is set (non-falsy), existing value is None -> use input value
        pytest.param(
            "new_name",
            "display_name",
            _SmartFreshnessAssertionInput(
                dataset_urn=_any_dataset_urn,
                entity_client=MagicMock(),
                display_name="default_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS,
            ),
            SmartFreshnessAssertion(
                urn=AssertionUrn("urn:li:assertion:test"),
                dataset_urn=DatasetUrn.from_string(_any_dataset_urn),
                display_name="existing_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS,
            ),
            None,
            "new_name",
            id="input_set_existing_none",
        ),
        # Case 6: Input is set (falsy - empty string), existing value is not None -> use input value
        pytest.param(
            "",
            "display_name",
            _SmartFreshnessAssertionInput(
                dataset_urn=_any_dataset_urn,
                entity_client=MagicMock(),
                display_name="default_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS,
            ),
            SmartFreshnessAssertion(
                urn=AssertionUrn("urn:li:assertion:test"),
                dataset_urn=DatasetUrn.from_string(_any_dataset_urn),
                display_name="existing_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS,
            ),
            "existing_entity_value",
            "",
            id="input_falsy_existing_not_none",
        ),
        # Case 7: Input is set (falsy - empty list), existing value is not None -> use input value
        pytest.param(
            [],
            "tags",
            _SmartFreshnessAssertionInput(
                dataset_urn=_any_dataset_urn,
                entity_client=MagicMock(),
                tags=[TagUrn("urn:li:tag:default_tag")],
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS_NO_TAGS,
            ),
            SmartFreshnessAssertion(
                urn=AssertionUrn("urn:li:assertion:test"),
                dataset_urn=DatasetUrn.from_string(_any_dataset_urn),
                tags=[TagUrn("urn:li:tag:existing_tag")],
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS_NO_TAGS,
            ),
            ["existing_entity_tag"],
            [],
            id="input_falsy_list_existing_not_none",
        ),
        # Case 8: Input is set (falsy - empty string), existing value is None -> use input value
        pytest.param(
            "",
            "display_name",
            _SmartFreshnessAssertionInput(
                dataset_urn=_any_dataset_urn,
                entity_client=MagicMock(),
                display_name="default_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_INPUT_PARAMS,
            ),
            SmartFreshnessAssertion(
                urn=AssertionUrn("urn:li:assertion:test"),
                dataset_urn=DatasetUrn.from_string(_any_dataset_urn),
                display_name="existing_name",
                **_OTHER_SMART_FRESHNESS_ASSERTION_INPUT_PARAMS,
            ),
            None,
            "",
            id="input_falsy_existing_none",
        ),
    ],
)
def test__merge_field(
    input_field_value: Any,
    input_field_name: str,
    validated_assertion_input: _SmartFreshnessAssertionInput,
    validated_existing_assertion: SmartFreshnessAssertion,
    existing_entity_value: Optional[Any],
    expected_value: Any,
) -> None:
    """Test the _merge_field function with various combinations of input, existing, and default values.
    Test cases:
    - Input is not set (set to None), AND existing value is not None (use existing value)
    - Input is not set (set to None), AND existing value is None (use default)
    - Input is set (set to a value), AND existing value is not None (use input value)
    - Input is set (set to a value), AND existing value is None (use input value)
    - Input is set (set to a FALSY value), AND existing value is not None (use input value)
    - Input is set (set to a FALSY value), AND existing value is None (use input value)
    """
    result = _merge_field(
        input_field_value=input_field_value,
        input_field_name=input_field_name,
        validated_assertion_input=validated_assertion_input,
        validated_existing_assertion=validated_existing_assertion,
        existing_entity_value=existing_entity_value,
    )
    assert result == expected_value


def _validate_assertion_vs_input(
    assertion: SmartFreshnessAssertion,
    input_params: Union[
        SmartFreshnessAssertionInputParams, SmartFreshnessAssertionUpsertInputParams
    ],
    expected_output_params: SmartFreshnessAssertionOutputParams,
) -> None:
    if input_params.display_name is not None:
        assert assertion.display_name == expected_output_params.display_name
    else:
        assert assertion.display_name.startswith(
            "New Assertion"
        )  # Generated display name
        assert len(assertion.display_name) == GENERATED_DISPLAY_NAME_LENGTH
    assert isinstance(
        assertion.detection_mechanism, _DETECTION_MECHANISM_CONCRETE_TYPES
    )
    assert (
        assertion.detection_mechanism.type
        == expected_output_params.detection_mechanism.type
    )
    assert assertion.sensitivity.value == expected_output_params.sensitivity.value
    assert assertion.exclusion_windows == expected_output_params.exclusion_windows
    assert (
        assertion.training_data_lookback_days
        == expected_output_params.training_data_lookback_days
    )
    assert assertion.incident_behavior == expected_output_params.incident_behavior
    assert assertion.tags == expected_output_params.tags
    assert assertion.created_by == expected_output_params.created_by
    assert assertion.created_at == expected_output_params.created_at
    assert assertion.updated_by == expected_output_params.updated_by
    assert assertion.updated_at == expected_output_params.updated_at
