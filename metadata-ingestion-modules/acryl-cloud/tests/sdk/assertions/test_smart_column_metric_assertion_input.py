"""For testing the _SmartColumnMetricAssertionInput class."""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple, Union

import pytest

from acryl_datahub_cloud.sdk.assertion_input.assertion_input import InferenceSensitivity
from acryl_datahub_cloud.sdk.assertion_input.smart_column_metric_assertion_input import (
    FIELD_METRIC_TYPE_CONFIG,
    AssertionIncidentBehavior,
    DetectionMechanism,
    _SmartColumnMetricAssertionInput,
)
from acryl_datahub_cloud.sdk.entities.assertion import (
    TagsInputType,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.emitter.enum_helpers import get_enum_options
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import CorpUserUrn, DatasetUrn
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
    operator: Union[str, models.AssertionStdOperatorClass]
    value: Optional[Union[str, int, float]] = None
    value_type: Optional[Union[str, models.AssertionStdParameterTypeClass]] = None
    range: Optional[Tuple[Union[str, int, float], Union[str, int, float]]] = None
    range_type: Optional[
        Union[
            Union[str, models.AssertionStdParameterTypeClass],
            Tuple[
                Union[str, models.AssertionStdParameterTypeClass],
                Union[str, models.AssertionStdParameterTypeClass],
            ],
        ]
    ] = None
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
                operator="GREATER_THAN",
                value="10",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_string_column_null_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.UNIQUE_COUNT,
                operator="GREATER_THAN",
                value="10",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_string_column_unique_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.MAX_LENGTH,
                operator="LESS_THAN",
                value="100",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_string_column_max_length",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.MEAN,
                operator="BETWEEN",
                range=("0", "100"),
                range_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_number_column_mean",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.MEDIAN,
                operator="GREATER_THAN",
                value="50",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_number_column_median",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NEGATIVE_COUNT,
                operator="EQUAL_TO",
                value="0",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_number_column_negative_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="boolean_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="IS_TRUE",
                should_raise=False,
            ),
            id="valid_boolean_column_null_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="boolean_column",
                metric_type=models.FieldMetricTypeClass.UNIQUE_COUNT,
                operator="IS_FALSE",
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
                operator="REGEX_MATCH",
                value="test.*",
                value_type=models.AssertionStdParameterTypeClass.STRING,
                should_raise=False,
            ),
            id="valid_string_column_regex_match",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="GREATER_THAN_OR_EQUAL_TO",
                value="0",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_number_column_greater_than_or_equal_to",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="NOT_EQUAL_TO",
                value="0",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
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
                operator="EQUAL_TO",
                value="test",
                value_type=models.AssertionStdParameterTypeClass.STRING,
                should_raise=False,
            ),
            id="valid_string_value_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="EQUAL_TO",
                value=42,
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
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
                operator="BETWEEN",
                range=("a", "z"),
                range_type=models.AssertionStdParameterTypeClass.STRING,
                should_raise=False,
            ),
            id="valid_string_range_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="BETWEEN",
                range=(0, 100),
                range_type=models.AssertionStdParameterTypeClass.NUMBER,
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
                operator="GREATER_THAN",
                value="10",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
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
                operator="GREATER_THAN",
                value="10",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Invalid value for FieldMetricTypeClass: INVALID_METRIC, valid options are",
            ),
            id="invalid_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="INVALID_OPERATOR",
                value="10",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Invalid value for AssertionStdOperatorClass: INVALID_OPERATOR, valid options are",
            ),
            id="invalid_operator",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="GREATER_THAN",
                value="invalid_value",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Invalid value: invalid_value, must be a number",
            ),
            id="invalid_value_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="BETWEEN",
                range=("invalid_start", "100"),
                range_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Invalid value: invalid_start, must be a number",
            ),
            id="invalid_range_value",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="GREATER_THAN",
                value_type=None,
                value=None,  # Not provided, should raise error
                should_raise=True,
                expected_error_should_contain="Value type is required",
            ),
            id="missing_required_value_and_value_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="GREATER_THAN",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                value=None,  # Not provided, should raise error
                should_raise=True,
                expected_error_should_contain="Value parameter is required for the chosen operator",
            ),
            id="missing_required_value",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="BETWEEN",
                should_raise=True,
                expected_error_should_contain="Range is required for operator BETWEEN",
            ),
            id="missing_required_range",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="NULL",
                value="10",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Value parameters should not be provided for operator NULL",
            ),
            id="unexpected_value_for_no_param_operator",
        ),
        # Test cases for date/time columns
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="date_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="NULL",
                should_raise=False,
            ),
            id="valid_date_column_null_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="time_column",
                metric_type=models.FieldMetricTypeClass.UNIQUE_PERCENTAGE,
                operator="NOT_NULL",
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
                operator="NULL",
                should_raise=False,
            ),
            id="valid_null_column_null_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="null_column",
                metric_type=models.FieldMetricTypeClass.UNIQUE_COUNT,
                operator="NOT_NULL",
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
                operator="GREATER_THAN",
                value="10",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Metric type MAX_LENGTH is not allowed for field type NumberTypeClass",
            ),
            id="invalid_number_column_max_length",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.MEAN,
                operator="GREATER_THAN",
                value="10",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Metric type MEAN is not allowed for field type StringTypeClass",
            ),
            id="invalid_string_column_mean",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="boolean_column",
                metric_type=models.FieldMetricTypeClass.MEDIAN,
                operator="IS_TRUE",
                should_raise=True,
                expected_error_should_contain="Metric type MEDIAN is not allowed for field type BooleanTypeClass",
            ),
            id="invalid_boolean_column_median",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="date_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="GREATER_THAN",
                value="0",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Operator GREATER_THAN is not allowed for field type DateTypeClass for column 'date_column'. Allowed operators: NULL, NOT_NULL",
            ),
            id="invalid_date_column_negative_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="time_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="EQUAL_TO",
                value="0",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Operator EQUAL_TO is not allowed for field type TimeTypeClass for column 'time_column'. Allowed operators: NULL, NOT_NULL",
            ),
            id="invalid_time_column_zero_count",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="null_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="GREATER_THAN",
                value="1.0",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Operator GREATER_THAN is not allowed for field type NullTypeClass for column 'null_column'. Allowed operators: NULL, NOT_NULL",
            ),
            id="invalid_null_column_stddev",
        ),
        # Test cases for operator and metric type compatibility
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="IS_TRUE",  # IS_TRUE is not valid for string columns
                should_raise=True,
                expected_error_should_contain="Operator IS_TRUE is not allowed for field type StringTypeClass for column 'string_column'. Allowed operators: NULL, NOT_NULL, EQUAL_TO, IN, GREATER_THAN_OR_EQUAL_TO, REGEX_MATCH, GREATER_THAN, LESS_THAN, BETWEEN",
            ),
            id="invalid_operator_for_metric_type",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.MEAN,
                operator="IS_TRUE",  # IS_TRUE is not valid for MEAN metric
                should_raise=True,
                expected_error_should_contain="Operator IS_TRUE is not allowed for field type NumberTypeClass for column 'number_column'. Allowed operators: GREATER_THAN, LESS_THAN, BETWEEN, NULL, NOT_NULL, EQUAL_TO, IN, GREATER_THAN_OR_EQUAL_TO, NOT_EQUAL_TO",
            ),
            id="invalid_boolean_operator_for_numeric_metric",
        ),
        # Test cases for value and metric type compatibility
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="EQUAL_TO",
                value="abc",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Invalid value: abc, must be a number",
            ),
            id="invalid_string_value_for_numeric_metric",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="EQUAL_TO",
                value="not_a_number",
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Invalid value: not_a_number, must be a number",
            ),
            id="invalid_non_numeric_value_for_numeric_metric",
        ),
        # Test cases for range and metric type compatibility
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="BETWEEN",
                range=(0, 100),
                range_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_numeric_range_for_numeric_metric",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="BETWEEN",
                range=("a", "z"),
                range_type=models.AssertionStdParameterTypeClass.NUMBER,  # This should fail because we're using string values with NUMBER type
                should_raise=True,
                expected_error_should_contain="Invalid value: a, must be a number",
            ),
            id="invalid_string_range_for_numeric_metric",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="BETWEEN",
                range=("not_a_number", 100),
                range_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=True,
                expected_error_should_contain="Invalid value: not_a_number, must be a number",
            ),
            id="invalid_non_numeric_range_for_numeric_metric",
        ),
        # Test cases for value type and metric type compatibility
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="EQUAL_TO",
                value=42,
                value_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_numeric_value_type_for_numeric_metric",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="EQUAL_TO",
                value=42,
                value_type=models.AssertionStdParameterTypeClass.STRING,  # This should fail because we're using a number with STRING type
                should_raise=True,
                expected_error_should_contain="Invalid value: 42, must be a string",
            ),
            id="invalid_numeric_value_for_string_type",
        ),
        # Test cases for range type and metric type compatibility
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="number_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="BETWEEN",
                range=(0, 100),
                range_type=models.AssertionStdParameterTypeClass.NUMBER,
                should_raise=False,
            ),
            id="valid_numeric_range_type_for_numeric_metric",
        ),
        pytest.param(
            SmartColumnMetricAssertionInputTestParams(
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
                column_name="string_column",
                metric_type=models.FieldMetricTypeClass.NULL_COUNT,
                operator="BETWEEN",
                range=(0, 100),
                range_type=models.AssertionStdParameterTypeClass.STRING,  # This should fail because we're using numbers with STRING type
                should_raise=True,
                expected_error_should_contain="Invalid value: 0, must be a string",
            ),
            id="invalid_numeric_range_for_string_type",
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
                operator=params.operator,
                value=params.value,
                value_type=params.value_type,
                range=params.range,
                range_type=params.range_type,
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
            operator=params.operator,
            value=params.value,
            value_type=params.value_type,
            range=params.range,
            range_type=params.range_type,
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


def test_smart_column_metric_assertion_input_conversion(
    stub_entity_client: EntityClient,
    example_assertion_info: models.AssertionInfoClass,
    example_monitor_info: models.MonitorInfoClass,
) -> None:
    """Test that the input is converted to the correct assertion and monitor entities."""
    # Happy path - test basic conversion
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="string_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        operator="GREATER_THAN",
        value="10",
        value_type=models.AssertionStdParameterTypeClass.NUMBER,
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

    # Test assertion info conversion
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
    assert assertion_info.fieldMetricAssertion.parameters.value is not None
    assert (
        example_field_assertion_info.fieldMetricAssertion.parameters.value is not None
    )
    assert (
        assertion_info.fieldMetricAssertion.parameters.value.value
        == example_field_assertion_info.fieldMetricAssertion.parameters.value.value
    )
    assert (
        assertion_info.fieldMetricAssertion.parameters.value.type
        == example_field_assertion_info.fieldMetricAssertion.parameters.value.type
    )

    # Test monitor info conversion
    source_type, field = assertion_input._convert_assertion_source_type_and_field()
    from datahub.metadata.urns import AssertionUrn

    monitor_info = assertion_input._create_monitor_info(
        assertion_urn=AssertionUrn("urn:li:assertion:123"),
        status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
        schedule=assertion_input._convert_schedule(),
        source_type=source_type,
        field=field,
        sensitivity=assertion_input._convert_sensitivity(),
        exclusion_windows=assertion_input._convert_exclusion_windows(),
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

    # Test with range parameters
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="number_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        operator="BETWEEN",
        range=(0, 100),
        range_type=models.AssertionStdParameterTypeClass.NUMBER,
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    # Test assertion info conversion with range
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
    assert min_val.value == "0"
    assert max_val.value == "100"
    assert min_val.type == models.AssertionStdParameterTypeClass.NUMBER
    assert max_val.type == models.AssertionStdParameterTypeClass.NUMBER

    # Test with no parameters
    assertion_input = _SmartColumnMetricAssertionInput(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
        entity_client=stub_entity_client,
        column_name="string_column",
        metric_type=models.FieldMetricTypeClass.NULL_COUNT,
        operator="NULL",
        created_by="urn:li:corpuser:test",
        created_at=datetime.fromtimestamp(1717929600),
        updated_by="urn:li:corpuser:test",
        updated_at=datetime.fromtimestamp(1717929600),
    )

    # Test assertion info conversion with no parameters
    assertion_info = assertion_input._create_assertion_info(None)
    assert isinstance(assertion_info, models.FieldAssertionInfoClass)
    assert assertion_info.fieldMetricAssertion is not None
    assert assertion_info.fieldMetricAssertion.parameters is not None
    assert assertion_info.fieldMetricAssertion.parameters.value is None
    assert assertion_info.fieldMetricAssertion.parameters.minValue is None
    assert assertion_info.fieldMetricAssertion.parameters.maxValue is None

    # Test monitor info conversion
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
