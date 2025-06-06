from datetime import datetime, timezone

import pytest

from acryl_datahub_cloud._sdk_extras.assertion import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_SENSITIVITY,
    AssertionMode,
    SmartFreshnessAssertion,
)
from acryl_datahub_cloud._sdk_extras.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanism,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
    _SmartFreshnessAssertionInput,
)
from acryl_datahub_cloud._sdk_extras.entities.assertion import Assertion
from acryl_datahub_cloud._sdk_extras.entities.monitor import Monitor
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
    MonitorUrn,
    TagUrn,
)


def test_smart_freshness_assertion_creation_min_fields(
    any_assertion_urn: AssertionUrn,
) -> None:
    """Make sure the SmartFreshnessAssertion can be created with the minimum required fields."""
    SmartFreshnessAssertion(
        urn=any_assertion_urn,
        dataset_urn=DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
        ),
        display_name="Smart Freshness Assertion",
        mode=AssertionMode.ACTIVE,
        sensitivity=InferenceSensitivity.LOW,
        exclusion_windows=[
            FixedRangeExclusionWindow(
                start=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end=datetime(2021, 1, 2, tzinfo=timezone.utc),
            )
        ],
        training_data_lookback_days=30,
        detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        tags=[],
    )


def test_smart_freshness_assertion_all_attributes_are_readonly(
    any_assertion_urn: AssertionUrn,
) -> None:
    """Test that all attributes of SmartFreshnessAssertion are read-only."""
    assertion = SmartFreshnessAssertion(
        urn=any_assertion_urn,
        dataset_urn=DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
        ),
        display_name="Smart Freshness Assertion",
        mode=AssertionMode.ACTIVE,
        sensitivity=InferenceSensitivity.LOW,
        exclusion_windows=[
            FixedRangeExclusionWindow(
                start=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end=datetime(2021, 1, 2, tzinfo=timezone.utc),
            )
        ],
        training_data_lookback_days=30,
        detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        created_by=CorpUserUrn.from_string("urn:li:corpuser:acryl-cloud-user"),
        created_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        updated_by=CorpUserUrn.from_string("urn:li:corpuser:acryl-cloud-user"),
        updated_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        tags=[],
    )

    public_attributes_and_methods = [
        attr
        for attr in dir(assertion)
        if not attr.startswith("_") and not attr.startswith("__")
    ]

    settable_attributes = [
        attr
        for attr in public_attributes_and_methods
        if not callable(getattr(assertion, attr))
    ]

    # Test that each attribute cannot be set
    for attr in settable_attributes:
        with pytest.raises(
            AttributeError,
            # Different error message on Python 3.10+
            match=f"(can't set attribute '{attr}'|property '{attr}' of '{assertion.__class__.__name__}' object has no setter)",
        ):
            setattr(assertion, attr, getattr(assertion, attr))


@pytest.fixture
def minimal_monitor(any_monitor_urn: MonitorUrn) -> Monitor:
    """A monitor with minimal fields."""
    return Monitor(
        id=any_monitor_urn,
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
        ),
    )


@pytest.fixture
def monitor_without_sensitivity(any_monitor_urn: MonitorUrn) -> Monitor:
    """A monitor with all fields except sensitivity."""
    return Monitor(
        id=any_monitor_urn,
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion="urn:li:assertion:test",
                        schedule=models.CronScheduleClass(
                            cron=_SmartFreshnessAssertionInput.DEFAULT_SCHEDULE.cron,
                            timezone=_SmartFreshnessAssertionInput.DEFAULT_SCHEDULE.timezone,
                        ),
                        parameters=models.AssertionEvaluationParametersClass(
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS,
                            datasetFreshnessParameters=models.DatasetFreshnessAssertionParametersClass(
                                sourceType=models.DatasetFreshnessSourceTypeClass.FIELD_VALUE,
                                field=models.FreshnessFieldSpecClass(
                                    path="field",
                                    type="string",
                                    nativeType="string",
                                    kind=models.FreshnessFieldKindClass.LAST_MODIFIED,
                                ),
                            ),
                        ),
                    )
                ],
                settings=models.AssertionMonitorSettingsClass(
                    adjustmentSettings=models.AssertionAdjustmentSettingsClass(
                        exclusionWindows=[
                            models.AssertionExclusionWindowClass(
                                type=models.AssertionExclusionWindowTypeClass.FIXED_RANGE,
                                fixedRange=models.AbsoluteTimeWindowClass(
                                    startTimeMillis=1609459200000,  # 2021-01-01 00:00:00 UTC
                                    endTimeMillis=1609545600000,  # 2021-01-02 00:00:00 UTC
                                ),
                            ),
                        ],
                        trainingDataLookbackWindowDays=60,
                    ),
                ),
            ),
        ),
    )


@pytest.mark.parametrize(
    "monitor_fixture,expected_log_message",
    [
        pytest.param(
            "minimal_monitor",
            "Monitor {monitor_urn} does not have an `monitor.info.assertionMonitor` field, defaulting to InferenceSensitivity.MEDIUM",
            id="minimal_monitor",
        ),
        pytest.param(
            "monitor_without_sensitivity",
            "Monitor {monitor_urn} does not have an `monitor.info.assertionMonitor.settings.adjustmentSettings.sensitivity` field, defaulting to InferenceSensitivity.MEDIUM",
            id="monitor_without_sensitivity",
        ),
    ],
)
def test_smart_freshness_assertion_log_messages(
    monitor_fixture: str,
    expected_log_message: str,
    caplog: pytest.LogCaptureFixture,
    request: pytest.FixtureRequest,
) -> None:
    """Test that SmartFreshnessAssertion emits the correct log messages when fields are missing."""
    monitor = request.getfixturevalue(monitor_fixture)
    SmartFreshnessAssertion.from_entities(
        assertion=Assertion(
            id=AssertionUrn("urn:li:assertion:test"),
            info=models.FreshnessAssertionInfoClass(
                type=models.AssertionTypeClass.FRESHNESS,
                entity="urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)",
            ),
        ),
        monitor=monitor,
    )
    assert caplog.records[0].message == expected_log_message.format(
        monitor_urn=monitor.urn
    )


def test_smart_freshness_assertion_from_entities_all_fields(
    monitor_with_all_fields: Monitor, assertion_entity_with_all_fields: Assertion
) -> None:
    """Test that SmartFreshnessAssertion can be created from entities."""
    smart_freshness_assertion = SmartFreshnessAssertion.from_entities(
        assertion=assertion_entity_with_all_fields,
        monitor=monitor_with_all_fields,
    )

    assert smart_freshness_assertion.urn == assertion_entity_with_all_fields.urn
    assert smart_freshness_assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD)"
    )
    assert smart_freshness_assertion.display_name == "Smart Freshness Assertion"

    assert smart_freshness_assertion.mode == AssertionMode.ACTIVE

    assert smart_freshness_assertion.created_by == CorpUserUrn.from_string(
        "urn:li:corpuser:acryl-cloud-user-created"
    )
    assert smart_freshness_assertion.created_at == datetime(
        2021, 1, 1, tzinfo=timezone.utc
    )
    assert smart_freshness_assertion.updated_by == CorpUserUrn.from_string(
        "urn:li:corpuser:acryl-cloud-user-updated"
    )
    assert smart_freshness_assertion.updated_at == datetime(
        2021, 1, 2, tzinfo=timezone.utc
    )
    assert smart_freshness_assertion.tags == [
        TagUrn.from_string("urn:li:tag:smart_freshness_assertion_tag")
    ]
    assert smart_freshness_assertion.sensitivity == InferenceSensitivity.LOW
    assert smart_freshness_assertion.exclusion_windows == [
        FixedRangeExclusionWindow(
            start=datetime(2021, 1, 1, tzinfo=timezone.utc),
            end=datetime(2021, 1, 2, tzinfo=timezone.utc),
        )
    ]
    assert (
        smart_freshness_assertion.training_data_lookback_days
        != ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS
    )
    assert smart_freshness_assertion.training_data_lookback_days == 99
    assert smart_freshness_assertion.incident_behavior == [
        AssertionIncidentBehavior.RAISE_ON_FAIL,
        AssertionIncidentBehavior.RESOLVE_ON_PASS,
    ]
    assert smart_freshness_assertion.detection_mechanism != DEFAULT_DETECTION_MECHANISM
    assert (
        smart_freshness_assertion.detection_mechanism
        == DetectionMechanism.LAST_MODIFIED_COLUMN(
            column_name="field",
        )
    )


def test_smart_freshness_assertion_from_entities_minimal(
    minimal_monitor: Monitor,
) -> None:
    """Test that SmartFreshnessAssertion can be created from entities with minimal fields."""
    smart_freshness_assertion = SmartFreshnessAssertion.from_entities(
        assertion=Assertion(
            id=AssertionUrn("urn:li:assertion:minimal_assertion"),
            info=models.FreshnessAssertionInfoClass(
                type=models.AssertionTypeClass.FRESHNESS,
                entity="urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)",
            ),
        ),
        monitor=minimal_monitor,
    )

    assert smart_freshness_assertion.urn == AssertionUrn(
        "urn:li:assertion:minimal_assertion"
    )
    assert smart_freshness_assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
    )
    assert (
        smart_freshness_assertion.display_name == ""
    )  # Default empty string when no description
    assert (
        smart_freshness_assertion.incident_behavior == []
    )  # Default empty list when no actions
    assert smart_freshness_assertion.tags == []  # Default empty list when no tags

    # No created by, created at, updated by, updated at when no source or last updated:
    assert smart_freshness_assertion.created_by is None
    assert smart_freshness_assertion.created_at is None
    assert smart_freshness_assertion.updated_by is None
    assert smart_freshness_assertion.updated_at is None

    assert smart_freshness_assertion.sensitivity == DEFAULT_SENSITIVITY
    assert smart_freshness_assertion.exclusion_windows == []
    assert (
        smart_freshness_assertion.training_data_lookback_days
        == ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS
    )
    assert smart_freshness_assertion.detection_mechanism == DEFAULT_DETECTION_MECHANISM


# TODO: Add non happy path tests
# - missing required fields
# - invalid fields
# TODO: Test that the correct warning message is emitted
# TODO: Test that the correct error message is emitted when a path isn't supported
