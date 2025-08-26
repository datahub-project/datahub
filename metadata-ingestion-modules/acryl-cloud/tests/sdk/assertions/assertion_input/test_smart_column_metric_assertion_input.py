"""For testing the _SmartColumnMetricAssertionInput class."""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Union

import pytest
from conftest import StubEntityClient

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanism,
    DetectionMechanismInputTypes,
    InferenceSensitivity,
)
from acryl_datahub_cloud.sdk.assertion_input.column_metric_constants import (
    FIELD_METRIC_TYPE_CONFIG,
    MetricType,
    OperatorType,
)
from acryl_datahub_cloud.sdk.assertion_input.smart_column_metric_assertion_input import (
    _SmartColumnMetricAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError, SDKUsageError
from datahub.emitter.enum_helpers import get_enum_options
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn
from datahub.sdk.entity_client import EntityClient


@dataclass
class SmartColumnMetricAssertionInputTestParams:
    """Test parameters for _SmartColumnMetricAssertionInput test cases.

    Contains input parameters and expected output for the _SmartColumnMetricAssertionInput class.
    """

    # Input parameters
    dataset_urn: Union[str, DatasetUrn]
    column_name: str
    metric_type: Union[str, models.FieldMetricTypeClass]
    urn: Optional[Union[str, AssertionUrn]] = None
    display_name: Optional[str] = None
    enabled: bool = True
    schedule: Optional[Union[str, models.CronScheduleClass]] = None
    sensitivity: Optional[Union[str, InferenceSensitivity]] = None
    exclusion_windows: Optional[List[models.AssertionExclusionWindowClass]] = None
    training_data_lookback_days: Optional[int] = None
    incident_behavior: Optional[
        Union[AssertionIncidentBehavior, list[AssertionIncidentBehavior]]
    ] = AssertionIncidentBehavior.RAISE_ON_FAIL
    tags: Optional[TagsInputType] = None
    created_by: Union[str, CorpUserUrn] = "urn:li:corpuser:test"
    created_at: datetime = datetime.now()
    updated_by: Union[str, CorpUserUrn] = "urn:li:corpuser:test"
    updated_at: datetime = datetime.now()

    # Expected output
    should_raise: bool = False
    expected_error_should_contain: Optional[str] = None
    expected_warning_logged: bool = False
    expected_warning_message: Optional[str] = None


@pytest.mark.parametrize(
    "params",
    [
        # Test cases for metric type validation
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_string_column_null_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.UNIQUE_COUNT,
                should_raise=False,
            ),
            id="valid_string_column_unique_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.MAX_LENGTH,
                should_raise=False,
            ),
            id="valid_string_column_max_length",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.MEAN,
                should_raise=False,
            ),
            id="valid_number_column_mean",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.MEDIAN,
                should_raise=False,
            ),
            id="valid_number_column_median",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NEGATIVE_COUNT,
                should_raise=False,
            ),
            id="valid_number_column_negative_count",
        ),
        # Test cases for new MetricType enum options
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=MetricType.EMPTY_COUNT,
                should_raise=False,
            ),
            id="valid_string_column_empty_count_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=MetricType.EMPTY_PERCENTAGE,
                should_raise=False,
            ),
            id="valid_string_column_empty_percentage_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=MetricType.MIN,
                should_raise=False,
            ),
            id="valid_number_column_min_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=MetricType.MAX,
                should_raise=False,
            ),
            id="valid_number_column_max_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=MetricType.MEAN,
                should_raise=False,
            ),
            id="valid_number_column_mean_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=MetricType.MEDIAN,
                should_raise=False,
            ),
            id="valid_number_column_median_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=MetricType.STDDEV,
                should_raise=False,
            ),
            id="valid_number_column_stddev_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=MetricType.NEGATIVE_COUNT,
                should_raise=False,
            ),
            id="valid_number_column_negative_count_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=MetricType.NEGATIVE_PERCENTAGE,
                should_raise=False,
            ),
            id="valid_number_column_negative_percentage_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=MetricType.ZERO_COUNT,
                should_raise=False,
            ),
            id="valid_number_column_zero_count_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=MetricType.ZERO_PERCENTAGE,
                should_raise=False,
            ),
            id="valid_number_column_zero_percentage_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="boolean_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_boolean_column_null_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="boolean_column",
                metric_type=models.FieldMetricTypeClass.UNIQUE_COUNT,
                should_raise=False,
            ),
            id="valid_boolean_column_unique_count",
        ),
        # Test cases for operator validation
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_string_column_regex_match",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_number_column_greater_than_or_equal_to",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_number_column_not_equal_to",
        ),
        # Test cases for value type validation
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_string_value_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_number_value_type",
        ),
        # Test cases for range type validation
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_string_range_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_number_range_type",
        ),
        # Error cases
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="invalid_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=True,
                expected_error_should_contain="Column invalid_column not found in dataset urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            ),
            id="invalid_column_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type="INVALID_METRIC",
                should_raise=True,
                expected_error_should_contain="Invalid value for FieldMetricTypeClass: INVALID_METRIC, valid options are",
            ),
            id="invalid_metric_type",
        ),
        # Test cases for date/time columns
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="date_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_date_column_null_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="time_column",
                metric_type=models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
                should_raise=False,
            ),
            id="valid_time_column_unique_percentage",
        ),
        # Test cases for null columns
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="null_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_null_column_null_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="null_column",
                metric_type=models.FieldMetricTypeClass.UNIQUE_COUNT,
                should_raise=False,
            ),
            id="valid_null_column_unique_count",
        ),
        # Test cases for invalid metric type combinations
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.MAX_LENGTH,
                should_raise=True,
                expected_error_should_contain="Metric type MAX_LENGTH is not allowed for field type NUMBER",
            ),
            id="invalid_number_column_max_length",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.MEAN,
                should_raise=True,
                expected_error_should_contain="Metric type MEAN is not allowed for field type STRING",
            ),
            id="invalid_string_column_mean",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="boolean_column",
                metric_type=models.FieldMetricTypeClass.MEDIAN,
                should_raise=True,
                expected_error_should_contain="Metric type MEDIAN is not allowed for field type BOOLEAN",
            ),
            id="invalid_boolean_column_median",
        ),
        # Test cases for value and metric type compatibility
        # Test cases for range and metric type compatibility
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_numeric_range_for_numeric_metric",
        ),
        # Test cases for value type and metric type compatibility
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_numeric_value_type_for_numeric_metric",
        ),
        # Test cases for range type and metric type compatibility
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="valid_numeric_range_type_for_numeric_metric",
        ),
        # Test cases for None criteria_parameters - now allowed at AssertionInput level
        # (validation moved to client level)
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="none_criteria_parameters_allowed",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                should_raise=False,
            ),
            id="none_criteria_parameters_allowed_for_range_operators",
        ),
    ],
)
def test_smart_column_metric_assertion_input(
    params: SmartColumnMetricAssertionInputTestParams,
    stub_entity_client: EntityClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test the _SmartColumnMetricAssertionInput class."""
    if params.should_raise:
        with pytest.raises(SDKUsageError) as exc_info:
            _SmartColumnMetricAssertionInput(
                dataset_urn=params.dataset_urn,
                entity_client=stub_entity_client,
                column_name=params.column_name,
                metric_type=params.metric_type,
                urn=params.urn,
                display_name=params.display_name,
                enabled=params.enabled,
                schedule=params.schedule,
                sensitivity=params.sensitivity,
                exclusion_windows=params.exclusion_windows,
                training_data_lookback_days=params.training_data_lookback_days,
                incident_behavior=params.incident_behavior,
                tags=params.tags,
                created_by=params.created_by,
                created_at=params.created_at,
                updated_by=params.updated_by,
                updated_at=params.updated_at,
            )
        if params.expected_error_should_contain:
            assert params.expected_error_should_contain in str(exc_info.value)
    else:
        assertion_input = _SmartColumnMetricAssertionInput(
            dataset_urn=params.dataset_urn,
            entity_client=stub_entity_client,
            column_name=params.column_name,
            metric_type=params.metric_type,
            urn=params.urn,
            display_name=params.display_name,
            enabled=params.enabled,
            schedule=params.schedule,
            sensitivity=params.sensitivity,
            exclusion_windows=params.exclusion_windows,
            training_data_lookback_days=params.training_data_lookback_days,
            incident_behavior=params.incident_behavior,
            tags=params.tags,
            created_by=params.created_by,
            created_at=params.created_at,
            updated_by=params.updated_by,
            updated_at=params.updated_at,
        )
        assert assertion_input is not None

    if params.expected_warning_logged and params.expected_warning_message:
        assert params.expected_warning_message in caplog.text
    else:
        assert not caplog.text


def test_that_all_field_metric_type_config_values_are_compatible_with_column_metric_assertions() -> (
    None
):
    ALL_FIELD_METRIC_TYPE_CONFIG_VALUES = {
        metric_type
        for metric_types in FIELD_METRIC_TYPE_CONFIG.values()
        for metric_type in metric_types
    }
    assert (
        set(get_enum_options(models.FieldMetricTypeClass))
        == ALL_FIELD_METRIC_TYPE_CONFIG_VALUES
    ), (
        "FIELD_METRIC_TYPE_CONFIG and get_enum_options(models.FieldMetricTypeClass) are out of sync"
    )


@pytest.fixture
def example_assertion_info() -> models.AssertionInfoClass:
    """Example assertion info for testing."""
    return models.AssertionInfoClass(
        type=models.AssertionTypeClass.FIELD,
        fieldAssertion=models.FieldAssertionInfoClass(
            type=models.FieldAssertionTypeClass.FIELD_METRIC,
            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            filter=models.DatasetFilterClass(
                type=models.DatasetFilterTypeClass.SQL,
                sql="SELECT * FROM dataset WHERE column_name = 'value'",
            ),
            fieldMetricAssertion=models.FieldMetricAssertionClass(
                field=models.SchemaFieldSpecClass(
                    path="date_column",
                    type="date",
                    nativeType="date",
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
        source=models.AssertionSourceClass(
            type=models.AssertionSourceTypeClass.INFERRED,
            created=models.AuditStampClass(
                time=1717929600,
                actor="urn:li:corpuser:test",
            ),
        ),
        lastUpdated=models.AuditStampClass(
            time=1717929600,
            actor="urn:li:corpuser:test",
        ),
        description="This assertion validates the null count of the column 'column_name' is greater than 10.",
    )


@pytest.fixture
def example_monitor_info() -> models.MonitorInfoClass:
    """Example monitor info for testing."""
    return models.MonitorInfoClass(
        type=models.MonitorTypeClass.ASSERTION,
        status=models.MonitorStatusClass(
            mode=models.MonitorModeClass.ACTIVE,
        ),
        assertionMonitor=models.AssertionMonitorClass(
            assertions=[
                models.AssertionEvaluationSpecClass(
                    assertion="urn:li:assertion:123",
                    schedule=models.CronScheduleClass(
                        cron="0 0 * * *",
                        timezone="America/New_York",
                    ),
                    parameters=models.AssertionEvaluationParametersClass(
                        type=models.AssertionEvaluationParametersTypeClass.DATASET_FIELD,
                        datasetFieldParameters=models.DatasetFieldAssertionParametersClass(
                            sourceType=models.DatasetFieldAssertionSourceTypeClass.CHANGED_ROWS_QUERY,
                            changedRowsField=models.FreshnessFieldSpecClass(
                                path="column_name",
                                type="string",
                                nativeType="string",
                                kind=models.FreshnessFieldKindClass.HIGH_WATERMARK,
                            ),
                        ),
                    ),
                )
            ],
            settings=models.AssertionMonitorSettingsClass(
                adjustmentSettings=models.AssertionAdjustmentSettingsClass(
                    algorithm=models.AdjustmentAlgorithmClass.CUSTOM,
                    algorithmName="stddev",
                    context={
                        "stdDev": "1.0",
                    },
                    exclusionWindows=[
                        models.AssertionExclusionWindowClass(
                            type=models.AssertionExclusionWindowTypeClass.FIXED_RANGE,
                            fixedRange=models.AbsoluteTimeWindowClass(
                                startTimeMillis=1717929600,
                                endTimeMillis=1717929600,
                            ),
                        )
                    ],
                    trainingDataLookbackWindowDays=10,
                    sensitivity=models.AssertionMonitorSensitivityClass(
                        level=1,
                    ),
                ),
            ),
        ),
    )


def test_smart_column_metric_assertion_info_basic_conversion(
    stub_entity_client: EntityClient,
    example_assertion_info: models.AssertionInfoClass,
) -> None:
    """Test basic assertion info conversion with range parameters for smart assertions."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="string_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    assertion_info = assertion_input._create_assertion_info(None)
    example_field_assertion_info = example_assertion_info.fieldAssertion
    assert example_field_assertion_info is not None
    assert isinstance(assertion_info, models.FieldAssertionInfoClass)
    assert isinstance(example_field_assertion_info, models.FieldAssertionInfoClass)
    assert assertion_info.type == example_field_assertion_info.type
    assert assertion_info.entity == example_field_assertion_info.entity
    assert assertion_info.fieldMetricAssertion is not None
    assert example_field_assertion_info.fieldMetricAssertion is not None
    assert assertion_info.fieldMetricAssertion.parameters is not None
    assert example_field_assertion_info.fieldMetricAssertion.parameters is not None
    # Smart assertions use range parameters, not single value
    assert assertion_info.fieldMetricAssertion.parameters.value is None
    assert assertion_info.fieldMetricAssertion.parameters.minValue is not None
    assert assertion_info.fieldMetricAssertion.parameters.maxValue is not None
    # Example fixture uses single value parameter - that's ok for comparison
    assert (
        example_field_assertion_info.fieldMetricAssertion.parameters.value is not None
    )


def test_smart_column_metric_monitor_info_conversion(
    stub_entity_client: EntityClient,
    example_monitor_info: models.MonitorInfoClass,
) -> None:
    """Test monitor info conversion."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="string_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        schedule=models.CronScheduleClass(
            cron="0 0 * * *",
            timezone="America/New_York",
        ),
        detection_mechanism=DetectionMechanism.CHANGED_ROWS_QUERY(
            column_name="column_name"
        ),
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    from datahub.metadata.urns import AssertionUrn

    monitor_info = assertion_input._create_monitor_info(
        assertion_urn=AssertionUrn("urn:li:assertion:123"),
        status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
        schedule=assertion_input._convert_schedule(),
    )
    assert monitor_info.type == example_monitor_info.type
    assert monitor_info.status.mode == example_monitor_info.status.mode
    assert monitor_info.assertionMonitor is not None
    assert example_monitor_info.assertionMonitor is not None
    assert monitor_info.assertionMonitor.assertions[0] is not None
    assert example_monitor_info.assertionMonitor.assertions[0] is not None
    assert monitor_info.assertionMonitor.assertions[0].parameters is not None
    assert example_monitor_info.assertionMonitor.assertions[0].parameters is not None
    assert monitor_info.assertionMonitor.assertions[0].parameters.type is not None
    assert (
        example_monitor_info.assertionMonitor.assertions[0].parameters.type is not None
    )
    assert (
        monitor_info.assertionMonitor.assertions[0].parameters.datasetFieldParameters
        is not None
    )
    assert (
        example_monitor_info.assertionMonitor.assertions[
            0
        ].parameters.datasetFieldParameters
        is not None
    )
    assert (
        monitor_info.assertionMonitor.assertions[
            0
        ].parameters.datasetFieldParameters.sourceType
        is not None
    )
    assert (
        monitor_info.assertionMonitor.assertions[0].schedule.cron
        == example_monitor_info.assertionMonitor.assertions[0].schedule.cron
    )
    assert (
        monitor_info.assertionMonitor.assertions[0].schedule.timezone
        == example_monitor_info.assertionMonitor.assertions[0].schedule.timezone
    )
    assert (
        monitor_info.assertionMonitor.assertions[0].parameters.type
        == example_monitor_info.assertionMonitor.assertions[0].parameters.type
    )
    assert (
        monitor_info.assertionMonitor.assertions[
            0
        ].parameters.datasetFieldParameters.sourceType
        == example_monitor_info.assertionMonitor.assertions[
            0
        ].parameters.datasetFieldParameters.sourceType
    )

    monitor_params = monitor_info.assertionMonitor.assertions[0].parameters
    example_monitor_params = example_monitor_info.assertionMonitor.assertions[
        0
    ].parameters
    assert monitor_params is not None
    assert example_monitor_params is not None
    assert monitor_params.type == example_monitor_params.type
    assert monitor_params.datasetFieldParameters is not None
    assert example_monitor_params.datasetFieldParameters is not None
    assert (
        monitor_params.datasetFieldParameters.sourceType
        == example_monitor_params.datasetFieldParameters.sourceType
    )


def test_smart_column_metric_assertion_metric_type_enum(
    stub_entity_client: EntityClient,
) -> None:
    """Test assertion conversion with MetricType enum using fixed range parameters."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="number_column",
        metric_type=MetricType.MEAN,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    assertion_info = assertion_input._create_assertion_info(None)
    assert isinstance(assertion_info, models.FieldAssertionInfoClass)
    assert assertion_info.fieldMetricAssertion is not None
    assert (
        assertion_info.fieldMetricAssertion.metric == models.FieldMetricTypeClass.MEAN
    )
    assert assertion_info.fieldMetricAssertion.parameters is not None
    # Smart assertions use range parameters with fixed (0, 0) values
    assert assertion_info.fieldMetricAssertion.parameters.value is None
    assert assertion_info.fieldMetricAssertion.parameters.minValue is not None
    assert assertion_info.fieldMetricAssertion.parameters.maxValue is not None
    assert assertion_info.fieldMetricAssertion.parameters.minValue.value == "0"
    assert assertion_info.fieldMetricAssertion.parameters.maxValue.value == "0"
    assert (
        assertion_info.fieldMetricAssertion.parameters.minValue.type
        == models.AssertionStdParameterTypeClass.NUMBER
    )


def test_smart_column_metric_assertion_range_parameters(
    stub_entity_client: EntityClient,
) -> None:
    """Test assertion conversion with fixed range parameters for smart assertions."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="number_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    assertion_info = assertion_input._create_assertion_info(None)
    assert isinstance(assertion_info, models.FieldAssertionInfoClass)
    assert assertion_info.fieldMetricAssertion is not None
    assert assertion_info.fieldMetricAssertion.parameters is not None
    assert assertion_info.fieldMetricAssertion.parameters.minValue is not None
    assert assertion_info.fieldMetricAssertion.parameters.maxValue is not None
    min_val = assertion_info.fieldMetricAssertion.parameters.minValue
    max_val = assertion_info.fieldMetricAssertion.parameters.maxValue
    assert min_val is not None
    assert max_val is not None
    # Smart assertions use fixed (0, 0) range parameters since values are AI inferred
    assert min_val.value == "0"
    assert max_val.value == "0"
    assert min_val.type == models.AssertionStdParameterTypeClass.NUMBER
    assert max_val.type == models.AssertionStdParameterTypeClass.NUMBER


def test_smart_column_metric_assertion_no_parameters(
    stub_entity_client: EntityClient,
) -> None:
    """Test assertion conversion with fixed parameters for smart assertions."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="string_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    assertion_info = assertion_input._create_assertion_info(None)
    assert isinstance(assertion_info, models.FieldAssertionInfoClass)
    assert assertion_info.fieldMetricAssertion is not None
    assert assertion_info.fieldMetricAssertion.parameters is not None
    assert assertion_info.fieldMetricAssertion.parameters.value is None
    # Smart assertions always use fixed (0, 0) criteria_parameters
    assert assertion_info.fieldMetricAssertion.parameters.minValue is not None
    assert assertion_info.fieldMetricAssertion.parameters.minValue.value == "0"
    assert assertion_info.fieldMetricAssertion.parameters.maxValue is not None
    assert assertion_info.fieldMetricAssertion.parameters.maxValue.value == "0"


@pytest.mark.parametrize(
    "column_name,metric_type_str,expected_metric_type",
    [
        # String column metrics (case insensitive)
        ("string_column", "NULL_COUNT", models.FieldMetricTypeClass.NULL_COUNT),
        ("string_column", "null_count", models.FieldMetricTypeClass.NULL_COUNT),
        ("string_column", "Null_Count", models.FieldMetricTypeClass.NULL_COUNT),
        ("string_column", "MAX_LENGTH", models.FieldMetricTypeClass.MAX_LENGTH),
        ("string_column", "max_length", models.FieldMetricTypeClass.MAX_LENGTH),
        ("string_column", "Max_Length", models.FieldMetricTypeClass.MAX_LENGTH),
        ("string_column", "UNIQUE_COUNT", models.FieldMetricTypeClass.UNIQUE_COUNT),
        ("string_column", "unique_count", models.FieldMetricTypeClass.UNIQUE_COUNT),
        ("string_column", "Unique_Count", models.FieldMetricTypeClass.UNIQUE_COUNT),
        # Number column metrics (case insensitive)
        ("number_column", "MEAN", models.FieldMetricTypeClass.MEAN),
        ("number_column", "mean", models.FieldMetricTypeClass.MEAN),
        ("number_column", "Mean", models.FieldMetricTypeClass.MEAN),
        ("number_column", "MEDIAN", models.FieldMetricTypeClass.MEDIAN),
        ("number_column", "median", models.FieldMetricTypeClass.MEDIAN),
        ("number_column", "Median", models.FieldMetricTypeClass.MEDIAN),
    ],
)
def test_smart_column_metric_assertion_string_metric_type_case_insensitive(
    stub_entity_client: EntityClient,
    column_name: str,
    metric_type_str: str,
    expected_metric_type: models.FieldMetricTypeClass,
) -> None:
    """Test that metric_type parameter accepts strings and is case insensitive."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name=column_name,
        metric_type=metric_type_str,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    # Verify the string metric type was correctly converted to the expected enum
    assert assertion_input.metric_type == expected_metric_type

    # Verify assertion info conversion works correctly with string metric types
    assertion_info = assertion_input._create_assertion_info(None)
    assert isinstance(assertion_info, models.FieldAssertionInfoClass)
    assert assertion_info.fieldMetricAssertion is not None
    assert assertion_info.fieldMetricAssertion.metric == expected_metric_type


def test_smart_column_metric_assertion_operator_is_always_between(
    stub_entity_client: EntityClient,
) -> None:
    """Test that smart assertions always use BETWEEN operator regardless of input."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="number_column",
        metric_type=models.FieldMetricTypeClass.MEAN,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    # Verify the operator is always BETWEEN for smart assertions
    assert assertion_input.operator == models.AssertionStdOperatorClass.BETWEEN

    # Verify assertion info conversion works correctly with fixed operator
    assertion_info = assertion_input._create_assertion_info(None)
    assert isinstance(assertion_info, models.FieldAssertionInfoClass)
    assert assertion_info.fieldMetricAssertion is not None
    assert (
        assertion_info.fieldMetricAssertion.operator
        == models.AssertionStdOperatorClass.BETWEEN
    )


def test_smart_column_metric_assertion_operator_type_enum(
    stub_entity_client: EntityClient,
) -> None:
    """Test that smart assertions always use BETWEEN operator."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="number_column",
        metric_type=MetricType.MEAN,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    # Verify smart assertions always use BETWEEN operator
    assert assertion_input.operator == models.AssertionStdOperatorClass.BETWEEN

    # Verify assertion info conversion works correctly with OperatorType enum
    assertion_info = assertion_input._create_assertion_info(None)
    assert isinstance(assertion_info, models.FieldAssertionInfoClass)
    assert assertion_info.fieldMetricAssertion is not None
    assert (
        assertion_info.fieldMetricAssertion.operator
        == models.AssertionStdOperatorClass.BETWEEN
    )


def test_smart_column_metric_assertion_value_type_enum(
    stub_entity_client: EntityClient,
) -> None:
    """Test that smart assertions use range parameters with NUMBER type."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="number_column",
        metric_type=MetricType.MEAN,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    # Verify assertion info conversion works correctly with range parameters
    assertion_info = assertion_input._create_assertion_info(None)
    assert isinstance(assertion_info, models.FieldAssertionInfoClass)
    assert assertion_info.fieldMetricAssertion is not None
    assert assertion_info.fieldMetricAssertion.parameters is not None
    # Smart assertions use range parameters, not single value
    assert assertion_info.fieldMetricAssertion.parameters.value is None
    assert assertion_info.fieldMetricAssertion.parameters.minValue is not None
    assert assertion_info.fieldMetricAssertion.parameters.maxValue is not None
    assert (
        assertion_info.fieldMetricAssertion.parameters.minValue.type
        == models.AssertionStdParameterTypeClass.NUMBER
    )


@pytest.mark.parametrize(
    "detection_mechanism,expected_error_contains",
    [
        (
            DetectionMechanism.INFORMATION_SCHEMA,
            "Detection mechanism type='information_schema' is not supported for smart column metric assertions, please use a supported detection mechanism: all_rows_query, all_rows_query_datahub_dataset_profile, changed_rows_query",
        ),
        (
            DetectionMechanism.HIGH_WATERMARK_COLUMN(column_name="id"),
            "Detection mechanism type='high_watermark_column' column_name='id' additional_filter=None is not supported for smart column metric assertions, please use a supported detection mechanism: all_rows_query, all_rows_query_datahub_dataset_profile, changed_rows_query",
        ),
        (
            DetectionMechanism.QUERY(),
            "Detection mechanism type='query' additional_filter=None is not supported for smart column metric assertions, please use a supported detection mechanism: all_rows_query, all_rows_query_datahub_dataset_profile, changed_rows_query",
        ),
    ],
)
def test_smart_column_metric_assertion_input_detection_mechanism_validation(
    stub_entity_client: StubEntityClient,
    detection_mechanism: DetectionMechanismInputTypes,
    expected_error_contains: str,
) -> None:
    """Test that unsupported detection mechanisms raise SDKUsageError."""
    from datetime import datetime

    from datahub.metadata import schema_classes as models

    with pytest.raises(SDKNotYetSupportedError) as exc_info:
        assertion_input = _SmartColumnMetricAssertionInput(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            entity_client=stub_entity_client,
            column_name="string_column",
            metric_type=models.FieldMetricTypeClass.NULL_COUNT,
            detection_mechanism=detection_mechanism,
            created_by="urn:li:corpuser:test",
            created_at=datetime.now(),
            updated_by="urn:li:corpuser:test",
            updated_at=datetime.now(),
        )
        # Call the method that triggers detection mechanism validation
        assertion_input.to_assertion_and_monitor_entities()

    assert expected_error_contains in str(exc_info.value)


def test_smart_column_metric_assertion_fixed_parameters(
    stub_entity_client: EntityClient,
) -> None:
    """Test that smart assertions always use fixed parameters (0, 0) since they are inferred by AI."""

    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="string_column",
        metric_type=MetricType.NULL_COUNT,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    # Test that criteria_parameters are always (0, 0) for smart assertions
    assert assertion_input.criteria_parameters == (0, 0)

    # Test with a different metric type - should still be (0, 0)
    assertion_input2 = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="number_column",
        metric_type=MetricType.MEAN,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    # Test that criteria_parameters are always (0, 0) for smart assertions
    assert assertion_input2.criteria_parameters == (0, 0)


def test_metric_type_enum_matches_field_metric_type_class() -> None:
    """Test that MetricType enum contains the same items as FieldMetricTypeClass."""
    # Get all values from both enums
    metric_type_values = {item.value for item in MetricType}
    field_metric_type_values = set(get_enum_options(models.FieldMetricTypeClass))

    # Check that all MetricType values exist in FieldMetricTypeClass
    missing_in_schema = metric_type_values - field_metric_type_values
    assert not missing_in_schema, (
        f"MetricType enum contains values not present in FieldMetricTypeClass: {missing_in_schema}. "
        "Please update MetricType enum to match the schema."
    )

    # Check that all FieldMetricTypeClass values exist in MetricType
    missing_in_enum = field_metric_type_values - metric_type_values
    assert not missing_in_enum, (
        f"FieldMetricTypeClass contains values not present in MetricType enum: {missing_in_enum}. "
        "Please update MetricType enum to include all schema values."
    )

    # Verify they are exactly the same
    assert metric_type_values == field_metric_type_values, (
        "MetricType enum and FieldMetricTypeClass should contain exactly the same values"
    )


def test_operator_type_enum_matches_assertion_std_operator_class() -> None:
    """Test that OperatorType enum contains the same items as AssertionStdOperatorClass."""
    # Get all values from both enums
    operator_type_values = {item.value for item in OperatorType}
    assertion_std_operator_values = set(
        get_enum_options(models.AssertionStdOperatorClass)
    )

    # Check that all OperatorType values exist in AssertionStdOperatorClass
    missing_in_schema = operator_type_values - assertion_std_operator_values
    assert not missing_in_schema, (
        f"OperatorType enum contains values not present in AssertionStdOperatorClass: {missing_in_schema}. "
        "Please update OperatorType enum to match the schema."
    )

    # Check that all AssertionStdOperatorClass values exist in OperatorType
    missing_in_enum = assertion_std_operator_values - operator_type_values
    assert not missing_in_enum, (
        f"AssertionStdOperatorClass contains values not present in OperatorType enum: {missing_in_enum}. "
        "Please update OperatorType enum to include all schema values."
    )

    # Verify they are exactly the same
    assert operator_type_values == assertion_std_operator_values, (
        "OperatorType enum and AssertionStdOperatorClass should contain exactly the same values"
    )


def test_smart_column_metric_assertion_tag_name_conversion(
    stub_entity_client: StubEntityClient,
) -> None:
    """Test that tag names are properly converted to URNs in smart column metric assertions."""
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        column_name="string_column",
        metric_type=MetricType.NULL_COUNT,
        entity_client=stub_entity_client,
        tags=["plain_tag", "urn:li:tag:urn_tag"],
        created_by=CorpUserUrn("urn:li:corpuser:test_user"),
        created_at=datetime(2023, 1, 1),
        updated_by=CorpUserUrn("urn:li:corpuser:test_user"),
        updated_at=datetime(2023, 1, 1),
    )

    # Test the internal conversion method directly
    converted_tags = assertion_input._convert_tags()
    assert converted_tags == ["urn:li:tag:plain_tag", "urn:li:tag:urn_tag"]

    # Test the full assertion entity creation
    assertion_entity = assertion_input.to_assertion_entity()
    assert assertion_entity.tags == [
        models.TagAssociationClass(tag="urn:li:tag:plain_tag"),
        models.TagAssociationClass(tag="urn:li:tag:urn_tag"),
    ]
