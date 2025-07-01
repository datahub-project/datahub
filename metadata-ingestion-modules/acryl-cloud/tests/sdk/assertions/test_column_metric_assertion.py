"""Tests for the ColumnMetricAssertion class."""

from datetime import datetime, timezone

import pytest

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    AssertionMode,
)
from acryl_datahub_cloud.sdk.assertion.column_metric_assertion import (
    ColumnMetricAssertion,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_SCHEDULE,
    AssertionIncidentBehavior,
    DetectionMechanism,
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


def test_column_metric_assertion_creation_min_fields(
    any_assertion_urn: AssertionUrn,
) -> None:
    """Make sure the ColumnMetricAssertion can be created with the minimum required fields."""
    ColumnMetricAssertion(
        urn=any_assertion_urn,
        dataset_urn=DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
        ),
        display_name="Column Metric Assertion",
        mode=AssertionMode.ACTIVE,
        column_name="test_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        operator=models.AssertionStdOperatorClass.GREATER_THAN,
        criteria_parameters=10,
        detection_mechanism=DetectionMechanism.ALL_ROWS_QUERY(),
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        tags=[],
    )


def test_column_metric_assertion_all_attributes_are_readonly(
    any_assertion_urn: AssertionUrn,
) -> None:
    """Test that all attributes of ColumnMetricAssertion are read-only."""
    assertion = ColumnMetricAssertion(
        urn=any_assertion_urn,
        dataset_urn=DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
        ),
        display_name="Column Metric Assertion",
        mode=AssertionMode.ACTIVE,
        column_name="test_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        operator=models.AssertionStdOperatorClass.GREATER_THAN,
        criteria_parameters=10,
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
        description="Column Metric Assertion",
        source=models.AssertionSourceClass(
            type=models.AssertionSourceTypeClass.NATIVE,
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
                tag="urn:li:tag:column_metric_assertion_tag",
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


def test_column_metric_assertion_from_entities_all_fields(
    column_metric_monitor_with_all_fields: Monitor,
    column_metric_assertion_entity_with_all_fields: Assertion,
) -> None:
    """Test that ColumnMetricAssertion can be created from entities with all fields populated."""
    column_metric_assertion = ColumnMetricAssertion._from_entities(
        assertion=column_metric_assertion_entity_with_all_fields,
        monitor=column_metric_monitor_with_all_fields,
    )

    assert (
        column_metric_assertion.urn
        == column_metric_assertion_entity_with_all_fields.urn
    )
    assert column_metric_assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,1234567890,PROD)"
    )
    assert column_metric_assertion.display_name == "Column Metric Assertion"
    assert column_metric_assertion.mode == AssertionMode.ACTIVE

    # Check column metric specific fields
    assert column_metric_assertion.column_name == "test_column"
    assert column_metric_assertion.metric_type == models.FieldMetricTypeClass.NULL_COUNT
    assert (
        column_metric_assertion.operator
        == models.AssertionStdOperatorClass.GREATER_THAN
    )
    assert column_metric_assertion.criteria_parameters == "10"

    # Check common fields
    assert column_metric_assertion.created_by == CorpUserUrn.from_string(
        "urn:li:corpuser:acryl-cloud-user-created"
    )
    assert column_metric_assertion.created_at == datetime(
        2021, 1, 1, tzinfo=timezone.utc
    )
    assert column_metric_assertion.updated_by == CorpUserUrn.from_string(
        "urn:li:corpuser:acryl-cloud-user-updated"
    )
    assert column_metric_assertion.updated_at == datetime(
        2021, 1, 2, tzinfo=timezone.utc
    )
    assert column_metric_assertion.tags == [
        TagUrn.from_string("urn:li:tag:column_metric_assertion_tag")
    ]
    assert column_metric_assertion.incident_behavior == [
        AssertionIncidentBehavior.RAISE_ON_FAIL,
        AssertionIncidentBehavior.RESOLVE_ON_PASS,
    ]
    assert column_metric_assertion.detection_mechanism is not None
    assert isinstance(
        column_metric_assertion.detection_mechanism,
        DetectionMechanism.ALL_ROWS_QUERY,
    )
    assert column_metric_assertion.detection_mechanism.additional_filter is None


def test_column_metric_assertion_from_entities_minimal(
    minimal_monitor: Monitor,
) -> None:
    """Test that ColumnMetricAssertion can be created from entities with minimal fields."""
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

    column_metric_assertion = ColumnMetricAssertion._from_entities(
        assertion=assertion,
        monitor=minimal_monitor,
    )

    assert column_metric_assertion.urn == AssertionUrn(
        "urn:li:assertion:minimal_assertion"
    )
    assert column_metric_assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
    )
    assert (
        column_metric_assertion.display_name == ""
    )  # Default empty string when no description
    assert (
        column_metric_assertion.incident_behavior == []
    )  # Default empty list when no actions
    assert column_metric_assertion.tags == []  # Default empty list when no tags

    # Check column metric specific fields
    assert column_metric_assertion.column_name == "minimal_column"
    assert column_metric_assertion.metric_type == models.FieldMetricTypeClass.NULL_COUNT
    assert (
        column_metric_assertion.operator
        == models.AssertionStdOperatorClass.GREATER_THAN
    )
    assert column_metric_assertion.criteria_parameters == "5"

    # No created by, created at, updated by, updated at when no source or last updated:
    assert column_metric_assertion.created_by is None
    assert column_metric_assertion.created_at is None
    assert column_metric_assertion.updated_by is None
    assert column_metric_assertion.updated_at is None

    assert column_metric_assertion.detection_mechanism == DEFAULT_DETECTION_MECHANISM


def test_column_metric_assertion_with_range_values() -> None:
    """Test ColumnMetricAssertion with range values instead of single value."""
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

    column_metric_assertion = ColumnMetricAssertion._from_entities(
        assertion=assertion,
        monitor=monitor,
    )

    assert column_metric_assertion.column_name == "range_column"
    assert (
        column_metric_assertion.metric_type
        == models.FieldMetricTypeClass.NULL_PERCENTAGE
    )
    assert column_metric_assertion.operator == models.AssertionStdOperatorClass.BETWEEN
    # Should have range parameters for BETWEEN operator
    assert column_metric_assertion.criteria_parameters == ("5", "10")


def test_column_metric_assertion_with_no_parameter_operator() -> None:
    """Test ColumnMetricAssertion with operators that don't require parameters."""
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

    column_metric_assertion = ColumnMetricAssertion._from_entities(
        assertion=assertion,
        monitor=monitor,
    )

    assert column_metric_assertion.column_name == "no_param_column"
    assert column_metric_assertion.metric_type == models.FieldMetricTypeClass.NULL_COUNT
    assert column_metric_assertion.operator == models.AssertionStdOperatorClass.NULL
    # No parameters for NULL operator
    assert column_metric_assertion.criteria_parameters is None


def test_column_metric_assertion_with_changed_rows_detection_mechanism() -> None:
    """Test ColumnMetricAssertion with CHANGED_ROWS_QUERY detection mechanism."""
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

    column_metric_assertion = ColumnMetricAssertion._from_entities(
        assertion=assertion,
        monitor=monitor,
    )

    assert column_metric_assertion.detection_mechanism is not None
    assert isinstance(
        column_metric_assertion.detection_mechanism,
        DetectionMechanism.CHANGED_ROWS_QUERY,
    )
    assert column_metric_assertion.detection_mechanism.column_name == "watermark_column"
