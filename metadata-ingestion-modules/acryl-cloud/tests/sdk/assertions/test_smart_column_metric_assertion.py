"""Tests for the SmartColumnMetricAssertion class."""

from datetime import datetime, timezone

import pytest

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
)
from acryl_datahub_cloud.sdk.assertion.smart_column_metric_assertion import (
    SmartColumnMetricAssertion,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_SCHEDULE,
    DEFAULT_SENSITIVITY,
    AssertionIncidentBehavior,
    DetectionMechanism,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    AssertionUrn,
    CorpUserUrn,
    DatasetUrn,
    MonitorUrn,
    TagUrn,
)


def test_smart_column_metric_assertion_creation_min_fields(
    any_assertion_urn: AssertionUrn,
) -> None:
    """Make sure the SmartColumnMetricAssertion can be created with the minimum required fields."""
    SmartColumnMetricAssertion(
        urn=any_assertion_urn,
        dataset_urn=DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
        ),
        display_name="Smart Column Metric Assertion",
        mode=AssertionMode.ACTIVE,
        column_name="test_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        operator=models.AssertionStdOperatorClass.GREATER_THAN,
        value=10,
        value_type=models.AssertionStdParameterTypeClass.NUMBER,
        sensitivity=InferenceSensitivity.LOW,
        exclusion_windows=[
            FixedRangeExclusionWindow(
                start=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end=datetime(2021, 1, 2, tzinfo=timezone.utc),
            )
        ],
        training_data_lookback_days=30,
        detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        tags=[],
    )


def test_smart_column_metric_assertion_all_attributes_are_readonly(
    any_assertion_urn: AssertionUrn,
) -> None:
    """Test that all attributes of SmartColumnMetricAssertion are read-only."""
    assertion = SmartColumnMetricAssertion(
        urn=any_assertion_urn,
        dataset_urn=DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
        ),
        display_name="Smart Column Metric Assertion",
        mode=AssertionMode.ACTIVE,
        column_name="test_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        operator=models.AssertionStdOperatorClass.GREATER_THAN,
        value=10,
        value_type=models.AssertionStdParameterTypeClass.NUMBER,
        sensitivity=InferenceSensitivity.LOW,
        exclusion_windows=[
            FixedRangeExclusionWindow(
                start=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end=datetime(2021, 1, 2, tzinfo=timezone.utc),
            )
        ],
        training_data_lookback_days=30,
        detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
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
def column_metric_assertion_entity_with_all_fields(
    any_assertion_urn: AssertionUrn,
) -> Assertion:
    """Create a column metric assertion entity with all fields populated."""
    return Assertion(
        id=any_assertion_urn,
        info=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity="urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD)",
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="test_column",
                    type="string",
                    nativeType="VARCHAR",
                ),
                metric=models.FieldMetricTypeClass.NULL_COUNT,
                operator=models.AssertionStdOperatorClass.GREATER_THAN,
                parameters=models.AssertionStdParametersClass(
                    value=models.AssertionStdParameterClass(
                        value="10",
                        type=models.AssertionStdParameterTypeClass.NUMBER,
                    ),
                ),
            ),
        ),
        description="Smart Column Metric Assertion",
        source=models.AssertionSourceClass(
            type=models.AssertionSourceTypeClass.INFERRED,
            created=models.AuditStampClass(
                actor="urn:li:corpuser:acryl-cloud-user-created",
                time=1609459200000,  # 2021-01-01 00:00:00 UTC
            ),
        ),
        last_updated=models.AuditStampClass(
            actor="urn:li:corpuser:acryl-cloud-user-updated",
            time=1609545600000,  # 2021-01-02 00:00:00 UTC
        ),
        tags=[
            models.TagAssociationClass(
                tag="urn:li:tag:smart_column_metric_assertion_tag",
            )
        ],
        on_failure=[
            models.AssertionActionClass(
                type=models.AssertionActionTypeClass.RAISE_INCIDENT,
            )
        ],
        on_success=[
            models.AssertionActionClass(
                type=models.AssertionActionTypeClass.RESOLVE_INCIDENT,
            )
        ],
    )


@pytest.fixture
def column_metric_monitor_with_all_fields(
    any_monitor_urn: MonitorUrn, any_assertion_urn: AssertionUrn
) -> Monitor:
    """Create a monitor with all fields set for column metric assertions."""
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
                        assertion=str(any_assertion_urn),
                        schedule=DEFAULT_SCHEDULE,
                        parameters=models.AssertionEvaluationParametersClass(
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_FIELD,
                            datasetFieldParameters=models.DatasetFieldAssertionParametersClass(
                                sourceType=models.DatasetFieldAssertionSourceTypeClass.ALL_ROWS_QUERY,
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
                        trainingDataLookbackWindowDays=99,  # To differentiate from the default value
                        sensitivity=models.AssertionMonitorSensitivityClass(
                            level=1,  # LOW
                        ),
                    ),
                ),
            ),
        ),
    )


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


@pytest.mark.parametrize(
    "expected_log_message",
    [
        pytest.param(
            "Monitor {monitor_urn} does not have an `monitor.info.assertionMonitor` field, defaulting to InferenceSensitivity.MEDIUM",
            id="minimal_monitor",
        ),
    ],
)
def test_smart_column_metric_assertion_log_messages(
    expected_log_message: str,
    caplog: pytest.LogCaptureFixture,
    request: pytest.FixtureRequest,
    any_assertion_urn: AssertionUrn,
    minimal_monitor: Monitor,
) -> None:
    """Test that SmartColumnMetricAssertion emits the correct log messages when fields are missing."""
    monitor = minimal_monitor

    # Create a simple assertion directly instead of using the fixture
    assertion = Assertion(
        id=any_assertion_urn,
        info=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity="urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)",
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="test_column",
                    type="string",
                    nativeType="VARCHAR",
                ),
                metric=models.FieldMetricTypeClass.NULL_COUNT,
                operator=models.AssertionStdOperatorClass.GREATER_THAN,
                parameters=models.AssertionStdParametersClass(
                    value=models.AssertionStdParameterClass(
                        value="5",
                        type=models.AssertionStdParameterTypeClass.NUMBER,
                    ),
                ),
            ),
        ),
    )

    SmartColumnMetricAssertion._from_entities(
        assertion=assertion,
        monitor=monitor,
    )
    # Find the expected log message among all captured log messages
    expected_message = expected_log_message.format(monitor_urn=monitor.urn)
    log_messages = [record.message for record in caplog.records]
    assert expected_message in log_messages, (
        f"Expected message '{expected_message}' not found in log messages: {log_messages}"
    )


def test_smart_column_metric_assertion_from_entities_all_fields(
    column_metric_monitor_with_all_fields: Monitor,
    column_metric_assertion_entity_with_all_fields: Assertion,
) -> None:
    """Test that SmartColumnMetricAssertion can be created from entities with all fields populated."""
    smart_column_metric_assertion = SmartColumnMetricAssertion._from_entities(
        assertion=column_metric_assertion_entity_with_all_fields,
        monitor=column_metric_monitor_with_all_fields,
    )

    assert (
        smart_column_metric_assertion.urn
        == column_metric_assertion_entity_with_all_fields.urn
    )
    assert smart_column_metric_assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD)"
    )
    assert smart_column_metric_assertion.display_name == "Smart Column Metric Assertion"
    assert smart_column_metric_assertion.mode == AssertionMode.ACTIVE

    # Check column metric specific fields
    assert smart_column_metric_assertion.column_name == "test_column"
    assert (
        smart_column_metric_assertion.metric_type
        == models.FieldMetricTypeClass.NULL_COUNT
    )
    assert (
        smart_column_metric_assertion.operator
        == models.AssertionStdOperatorClass.GREATER_THAN
    )
    assert smart_column_metric_assertion.value == "10"
    assert (
        smart_column_metric_assertion.value_type
        == models.AssertionStdParameterTypeClass.NUMBER
    )

    # Check smart functionality fields
    assert smart_column_metric_assertion.sensitivity == InferenceSensitivity.LOW
    assert smart_column_metric_assertion.exclusion_windows == [
        FixedRangeExclusionWindow(
            start=datetime(2021, 1, 1, tzinfo=timezone.utc),
            end=datetime(2021, 1, 2, tzinfo=timezone.utc),
        )
    ]
    assert (
        smart_column_metric_assertion.training_data_lookback_days
        != ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS
    )
    assert smart_column_metric_assertion.training_data_lookback_days == 99

    # Check common fields
    assert smart_column_metric_assertion.created_by == CorpUserUrn.from_string(
        "urn:li:corpuser:acryl-cloud-user-created"
    )
    assert smart_column_metric_assertion.created_at == datetime(
        2021, 1, 1, tzinfo=timezone.utc
    )
    assert smart_column_metric_assertion.updated_by == CorpUserUrn.from_string(
        "urn:li:corpuser:acryl-cloud-user-updated"
    )
    assert smart_column_metric_assertion.updated_at == datetime(
        2021, 1, 2, tzinfo=timezone.utc
    )
    assert smart_column_metric_assertion.tags == [
        TagUrn.from_string("urn:li:tag:smart_column_metric_assertion_tag")
    ]
    assert smart_column_metric_assertion.incident_behavior == [
        AssertionIncidentBehavior.RAISE_ON_FAIL,
        AssertionIncidentBehavior.RESOLVE_ON_PASS,
    ]
    assert smart_column_metric_assertion.detection_mechanism is not None
    assert isinstance(
        smart_column_metric_assertion.detection_mechanism,
        DetectionMechanism.ALL_ROWS_QUERY,
    )
    assert smart_column_metric_assertion.detection_mechanism.additional_filter is None


def test_smart_column_metric_assertion_from_entities_minimal(
    minimal_monitor: Monitor,
) -> None:
    """Test that SmartColumnMetricAssertion can be created from entities with minimal fields."""
    assertion = Assertion(
        id=AssertionUrn("urn:li:assertion:minimal_assertion"),
        info=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity="urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)",
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="minimal_column",
                    type="string",
                    nativeType="VARCHAR",
                ),
                metric=models.FieldMetricTypeClass.NULL_COUNT,
                operator=models.AssertionStdOperatorClass.GREATER_THAN,
                parameters=models.AssertionStdParametersClass(
                    value=models.AssertionStdParameterClass(
                        value="5",
                        type=models.AssertionStdParameterTypeClass.NUMBER,
                    ),
                ),
            ),
        ),
    )

    smart_column_metric_assertion = SmartColumnMetricAssertion._from_entities(
        assertion=assertion,
        monitor=minimal_monitor,
    )

    assert smart_column_metric_assertion.urn == AssertionUrn(
        "urn:li:assertion:minimal_assertion"
    )
    assert smart_column_metric_assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
    )
    assert (
        smart_column_metric_assertion.display_name == ""
    )  # Default empty string when no description
    assert (
        smart_column_metric_assertion.incident_behavior == []
    )  # Default empty list when no actions
    assert smart_column_metric_assertion.tags == []  # Default empty list when no tags

    # Check column metric specific fields
    assert smart_column_metric_assertion.column_name == "minimal_column"
    assert (
        smart_column_metric_assertion.metric_type
        == models.FieldMetricTypeClass.NULL_COUNT
    )
    assert (
        smart_column_metric_assertion.operator
        == models.AssertionStdOperatorClass.GREATER_THAN
    )
    assert smart_column_metric_assertion.value == "5"
    assert (
        smart_column_metric_assertion.value_type
        == models.AssertionStdParameterTypeClass.NUMBER
    )

    # No created by, created at, updated by, updated at when no source or last updated:
    assert smart_column_metric_assertion.created_by is None
    assert smart_column_metric_assertion.created_at is None
    assert smart_column_metric_assertion.updated_by is None
    assert smart_column_metric_assertion.updated_at is None

    # Check smart functionality fields with default values
    assert smart_column_metric_assertion.sensitivity == DEFAULT_SENSITIVITY
    assert smart_column_metric_assertion.exclusion_windows == []
    assert (
        smart_column_metric_assertion.training_data_lookback_days
        == ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS
    )
    assert (
        smart_column_metric_assertion.detection_mechanism == DEFAULT_DETECTION_MECHANISM
    )


def test_column_metric_assertion_with_range_values() -> None:
    """Test SmartColumnMetricAssertion with range values instead of single value."""
    assertion = Assertion(
        id=AssertionUrn("urn:li:assertion:range_assertion"),
        info=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity="urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)",
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="range_column",
                    type="string",
                    nativeType="VARCHAR",
                ),
                metric=models.FieldMetricTypeClass.NULL_PERCENTAGE,
                operator=models.AssertionStdOperatorClass.BETWEEN,
                parameters=models.AssertionStdParametersClass(
                    minValue=models.AssertionStdParameterClass(
                        value="5",
                        type=models.AssertionStdParameterTypeClass.NUMBER,
                    ),
                    maxValue=models.AssertionStdParameterClass(
                        value="10",
                        type=models.AssertionStdParameterTypeClass.NUMBER,
                    ),
                ),
            ),
        ),
    )

    monitor = Monitor(
        id=MonitorUrn(
            entity=DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
            ),
            id="urn:li:assertion:range_assertion",
        ),
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
        ),
    )

    smart_column_metric_assertion = SmartColumnMetricAssertion._from_entities(
        assertion=assertion,
        monitor=monitor,
    )

    assert smart_column_metric_assertion.column_name == "range_column"
    assert (
        smart_column_metric_assertion.metric_type
        == models.FieldMetricTypeClass.NULL_PERCENTAGE
    )
    assert (
        smart_column_metric_assertion.operator
        == models.AssertionStdOperatorClass.BETWEEN
    )
    assert (
        smart_column_metric_assertion.value is None
    )  # No single value for BETWEEN operator
    assert (
        smart_column_metric_assertion.value_type is None
    )  # No single value type for BETWEEN operator
    assert smart_column_metric_assertion.range == ("5", "10")
    assert smart_column_metric_assertion.range_type == (
        models.AssertionStdParameterTypeClass.NUMBER,
        models.AssertionStdParameterTypeClass.NUMBER,
    )


def test_column_metric_assertion_with_no_parameter_operator() -> None:
    """Test SmartColumnMetricAssertion with operators that don't require parameters."""
    assertion = Assertion(
        id=AssertionUrn("urn:li:assertion:no_param_assertion"),
        info=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity="urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)",
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="no_param_column",
                    type="string",
                    nativeType="VARCHAR",
                ),
                metric=models.FieldMetricTypeClass.NULL_COUNT,
                operator=models.AssertionStdOperatorClass.NULL,
                parameters=models.AssertionStdParametersClass(),  # Empty parameters
            ),
        ),
    )

    monitor = Monitor(
        id=MonitorUrn(
            entity=DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
            ),
            id="urn:li:assertion:no_param_assertion",
        ),
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
        ),
    )

    smart_column_metric_assertion = SmartColumnMetricAssertion._from_entities(
        assertion=assertion,
        monitor=monitor,
    )

    assert smart_column_metric_assertion.column_name == "no_param_column"
    assert (
        smart_column_metric_assertion.metric_type
        == models.FieldMetricTypeClass.NULL_COUNT
    )
    assert (
        smart_column_metric_assertion.operator == models.AssertionStdOperatorClass.NULL
    )
    assert smart_column_metric_assertion.value is None  # No value for NULL operator
    assert (
        smart_column_metric_assertion.value_type is None
    )  # No value type for NULL operator
    assert smart_column_metric_assertion.range is None  # No range for NULL operator
    assert (
        smart_column_metric_assertion.range_type is None
    )  # No range type for NULL operator


def test_column_metric_assertion_with_changed_rows_detection_mechanism() -> None:
    """Test SmartColumnMetricAssertion with CHANGED_ROWS_QUERY detection mechanism."""
    assertion = Assertion(
        id=AssertionUrn("urn:li:assertion:changed_rows_assertion"),
        info=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity="urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)",
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="test_column",
                    type="string",
                    nativeType="VARCHAR",
                ),
                metric=models.FieldMetricTypeClass.NULL_COUNT,
                operator=models.AssertionStdOperatorClass.GREATER_THAN,
                parameters=models.AssertionStdParametersClass(
                    value=models.AssertionStdParameterClass(
                        value="10",
                        type=models.AssertionStdParameterTypeClass.NUMBER,
                    ),
                ),
            ),
        ),
    )

    monitor = Monitor(
        id=MonitorUrn(
            entity=DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
            ),
            id="urn:li:assertion:changed_rows_assertion",
        ),
        info=models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(
                mode=models.MonitorModeClass.ACTIVE,
            ),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion="urn:li:assertion:changed_rows_assertion",
                        schedule=DEFAULT_SCHEDULE,
                        parameters=models.AssertionEvaluationParametersClass(
                            type=models.AssertionEvaluationParametersTypeClass.DATASET_FIELD,
                            datasetFieldParameters=models.DatasetFieldAssertionParametersClass(
                                sourceType=models.DatasetFieldAssertionSourceTypeClass.CHANGED_ROWS_QUERY,
                                changedRowsField=models.FreshnessFieldSpecClass(
                                    path="watermark_column",
                                    type="number",
                                    nativeType="NUMBER",
                                    kind=models.FreshnessFieldKindClass.HIGH_WATERMARK,
                                ),
                            ),
                        ),
                    )
                ],
            ),
        ),
    )

    smart_column_metric_assertion = SmartColumnMetricAssertion._from_entities(
        assertion=assertion,
        monitor=monitor,
    )

    assert smart_column_metric_assertion.detection_mechanism is not None
    assert isinstance(
        smart_column_metric_assertion.detection_mechanism,
        DetectionMechanism.CHANGED_ROWS_QUERY,
    )
    assert (
        smart_column_metric_assertion.detection_mechanism.column_name
        == "watermark_column"
    )
