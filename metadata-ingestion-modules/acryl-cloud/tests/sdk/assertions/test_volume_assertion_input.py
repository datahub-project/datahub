from dataclasses import dataclass
from typing import Optional, Type
from unittest.mock import Mock

import pytest

from acryl_datahub_cloud.sdk.assertion_input.volume_assertion_input import (
    VolumeAssertionCondition,
    VolumeAssertionCriteria,
    VolumeAssertionCriteriaInputTypes,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError, SDKUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn

# Create a mock filter to use in tests
_TEST_FILTER = Mock(spec=models.DatasetFilterClass)


@dataclass
class VolumeAssertionCriteriaTestParams:
    """Test parameters for VolumeAssertionCriteria.parse method."""

    input_criteria: VolumeAssertionCriteriaInputTypes
    expected_type: Optional[Type] = None
    expected_value: Optional[VolumeAssertionCriteria] = None
    expected_error: Optional[str] = None
    should_succeed: bool = True


class TestVolumeAssertionCriteriaParse:
    """Comprehensive test suite for VolumeAssertionCriteria.parse method."""

    @pytest.mark.parametrize(
        "test_params",
        [
            # ============ SUCCESSFUL CASES ============
            # Test already instantiated objects that pass isinstance check
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    should_succeed=True,
                ),
                id="already_instantiated_criteria",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(100, 200),
                    ),
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(100, 200),
                    ),
                    should_succeed=True,
                ),
                id="already_instantiated_criteria_range",
            ),
            # ============ SUCCESSFUL DICT PARSING ============
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
                        "parameters": 100,
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    should_succeed=True,
                ),
                id="dict_with_int_parameter",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE",
                        "parameters": 50,
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE,
                        parameters=50,
                    ),
                    should_succeed=True,
                ),
                id="dict_change_with_int_parameter",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_WITHIN_A_RANGE",
                        "parameters": (100, 200),
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(100, 200),
                    ),
                    should_succeed=True,
                ),
                id="dict_between_with_tuple_parameters",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO",
                        "parameters": 500.5,
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO,
                        parameters=500.5,
                    ),
                    should_succeed=True,
                ),
                id="dict_less_than_or_equal_to_with_float",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE",
                        "parameters": 25,
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE,
                        parameters=25,
                    ),
                    should_succeed=True,
                ),
                id="dict_grows_by_at_most_absolute",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE",
                        "parameters": 10,
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE,
                        parameters=10,
                    ),
                    should_succeed=True,
                ),
                id="dict_grows_by_at_least_absolute",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE",
                        "parameters": 75.5,
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE,
                        parameters=75.5,
                    ),
                    should_succeed=True,
                ),
                id="dict_grows_by_at_most_percentage",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE",
                        "parameters": (5, 25),
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE,
                        parameters=(5, 25),
                    ),
                    should_succeed=True,
                ),
                id="dict_grows_within_range_absolute",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE",
                        "parameters": (10.5, 30.2),
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE,
                        parameters=(10.5, 30.2),
                    ),
                    should_succeed=True,
                ),
                id="dict_grows_within_range_percentage",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
                        "parameters": 0,
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=0,
                    ),
                    should_succeed=True,
                ),
                id="dict_with_zero_parameter",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_WITHIN_A_RANGE",
                        "parameters": (100.5, 200.8),
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(100.5, 200.8),
                    ),
                    should_succeed=True,
                ),
                id="dict_range_with_float_parameters",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        "parameters": 100,
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    should_succeed=True,
                ),
                id="dict_with_enum_condition_object",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
                        "parameters": 100,
                        "extra_field": "should_be_ignored",
                    },
                    expected_type=VolumeAssertionCriteria,
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    should_succeed=True,
                ),
                id="dict_with_extra_fields_ignored",
            ),
            # ============ INPUT TYPE VALIDATION FAILURES ============
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria="invalid_string",  # type: ignore[arg-type]
                    expected_error="Volume assertion criteria must be a dict or VolumeAssertionCriteria object, got: <class 'str'>",
                    should_succeed=False,
                ),
                id="invalid_input_type_string",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria=None,  # type: ignore[arg-type]
                    expected_error="Volume assertion criteria must be a dict or VolumeAssertionCriteria object, got: <class 'NoneType'>",
                    should_succeed=False,
                ),
                id="invalid_input_type_none",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria=123,  # type: ignore[arg-type]
                    expected_error="Volume assertion criteria must be a dict or VolumeAssertionCriteria object, got: <class 'int'>",
                    should_succeed=False,
                ),
                id="invalid_input_type_int",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria=[],  # type: ignore[arg-type]
                    expected_error="Volume assertion criteria must be a dict or VolumeAssertionCriteria object, got: <class 'list'>",
                    should_succeed=False,
                ),
                id="invalid_input_type_list",
            ),
            # ============ DICT STRUCTURE VALIDATION FAILURES ============
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "parameters": 100,
                    },
                    expected_error="Volume assertion criteria must include a 'condition' field",
                    should_succeed=False,
                ),
                id="missing_condition_field",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
                    },
                    expected_error="Volume assertion criteria must include a 'parameters' field",
                    should_succeed=False,
                ),
                id="missing_parameters_field",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={},
                    expected_error="Volume assertion criteria must include a 'condition' field",
                    should_succeed=False,
                ),
                id="empty_dict",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "INVALID_CONDITION",
                        "parameters": 100,
                    },
                    expected_error="Invalid condition 'INVALID_CONDITION'. Valid conditions:",
                    should_succeed=False,
                ),
                id="invalid_condition_value",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "row_count_is_greater_than_or_equal_to",  # lowercase
                        "parameters": 100,
                    },
                    expected_error="Invalid condition 'row_count_is_greater_than_or_equal_to'. Valid conditions:",
                    should_succeed=False,
                ),
                id="condition_wrong_case",
            ),
            # ============ PARAMETER VALIDATION FAILURES ============
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
                        "parameters": (100, 200),  # Should be single value, not tuple
                    },
                    expected_error="For condition ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO, parameters must be a single number, not a tuple",
                    should_succeed=False,
                ),
                id="single_value_condition_rejects_tuple_parameters",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_WITHIN_A_RANGE",
                        "parameters": 100,  # Should be tuple, not single value
                    },
                    expected_error="For WITHIN_A_RANGE condition ROW_COUNT_IS_WITHIN_A_RANGE, parameters must be a tuple of two numbers (min_value, max_value)",
                    should_succeed=False,
                ),
                id="within_range_condition_rejects_single_parameters",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
                        "parameters": "invalid_number",
                    },
                    expected_error="For condition ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO, parameters must be a single number",
                    should_succeed=False,
                ),
                id="single_value_condition_with_invalid_string",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
                        "parameters": "150",  # String is not valid parameter type
                    },
                    expected_error="For condition ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO, parameters must be a single number",
                    should_succeed=False,
                ),
                id="string_parameter_fails_validation",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
                        "parameters": None,
                    },
                    expected_error="Volume assertion criteria must include a 'parameters' field",
                    should_succeed=False,
                ),
                id="single_value_condition_with_none_parameter",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_WITHIN_A_RANGE",
                        "parameters": (100,),  # Only one value in tuple
                    },
                    expected_error="For WITHIN_A_RANGE condition ROW_COUNT_IS_WITHIN_A_RANGE, parameters must be a tuple of two numbers (min_value, max_value)",
                    should_succeed=False,
                ),
                id="range_condition_with_single_value_tuple",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_WITHIN_A_RANGE",
                        "parameters": (100, 200, 300),  # Three values in tuple
                    },
                    expected_error="For WITHIN_A_RANGE condition ROW_COUNT_IS_WITHIN_A_RANGE, parameters must be a tuple of two numbers (min_value, max_value)",
                    should_succeed=False,
                ),
                id="range_condition_with_three_value_tuple",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE",
                        "parameters": 100,
                    },
                    expected_error="For WITHIN_A_RANGE condition ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE, parameters must be a tuple of two numbers (min_value, max_value)",
                    should_succeed=False,
                ),
                id="absolute_range_condition_rejects_single_parameter",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE",
                        "parameters": 50,
                    },
                    expected_error="For WITHIN_A_RANGE condition ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE, parameters must be a tuple of two numbers (min_value, max_value)",
                    should_succeed=False,
                ),
                id="percentage_range_condition_rejects_single_parameter",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO",
                        "parameters": (100, 200),
                    },
                    expected_error="For condition ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO, parameters must be a single number, not a tuple",
                    should_succeed=False,
                ),
                id="less_than_or_equal_to_condition_rejects_tuple",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE",
                        "parameters": (10, 20),
                    },
                    expected_error="For condition ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE, parameters must be a single number, not a tuple",
                    should_succeed=False,
                ),
                id="absolute_growth_condition_rejects_tuple",
            ),
            pytest.param(
                VolumeAssertionCriteriaTestParams(
                    input_criteria={
                        "condition": "ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE",
                        "parameters": (30, 50),
                    },
                    expected_error="For condition ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE, parameters must be a single number, not a tuple",
                    should_succeed=False,
                ),
                id="percentage_growth_condition_rejects_tuple",
            ),
        ],
    )
    def test_parse_comprehensive(
        self, test_params: VolumeAssertionCriteriaTestParams
    ) -> None:
        """Comprehensive test for VolumeAssertionCriteria.parse method."""
        if test_params.should_succeed:
            # Test successful cases
            result = VolumeAssertionCriteria.parse(test_params.input_criteria)
            if test_params.expected_type:
                assert isinstance(result, test_params.expected_type)
            # For successful cases, expected_value must be provided
            assert test_params.expected_value is not None, (
                "expected_value must be provided for successful test cases"
            )
            assert result == test_params.expected_value
        else:
            # Test failure cases
            with pytest.raises(SDKUsageError) as exc_info:
                VolumeAssertionCriteria.parse(test_params.input_criteria)

            if test_params.expected_error:
                assert test_params.expected_error in str(exc_info.value)

    def test_parse_dict_copy_behavior(self) -> None:
        """Test that parse method doesn't modify the original dictionary."""
        original_dict = {
            "type": "row_count_total",
            "operator": "GREATER_THAN_OR_EQUAL_TO",
            "extra_field": "should_remain",
        }

        # Keep a copy to compare
        dict_copy = original_dict.copy()

        # This should fail, but shouldn't modify the original dict
        with pytest.raises(SDKUsageError):
            VolumeAssertionCriteria.parse(original_dict)

        # Original dict should be unchanged
        assert original_dict == dict_copy


@dataclass
class VolumeAssertionCriteriaBuildModelTestParams:
    """Test parameters for VolumeAssertionCriteria.build_model_volume_info method."""

    input_criteria: VolumeAssertionCriteria
    input_dataset_urn: str = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
    )
    input_filter: Optional["models.DatasetFilterClass"] = None
    expected_value: Optional["models.VolumeAssertionInfoClass"] = None
    expected_error: Optional[str] = None
    should_succeed: bool = True


class TestVolumeAssertionCriteriaBuildModelVolumeInfo:
    """Test suite for VolumeAssertionCriteria.build_model_volume_info method."""

    @pytest.mark.parametrize(
        "test_params",
        [
            # ============ SUCCESSFUL CASES ============
            # RowCountTotal with single value operators
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="100",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_greater_than_or_equal_to",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO,
                        parameters=500,
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="500",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_less_than_or_equal_to",
            ),
            # RowCountTotal with BETWEEN operator
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(100, 200),
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.BETWEEN,
                            parameters=models.AssertionStdParametersClass(
                                minValue=models.AssertionStdParameterClass(
                                    value="100",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                                maxValue=models.AssertionStdParameterClass(
                                    value="200",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_between",
            ),
            # RowCountChange with absolute kind
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE,
                        parameters=25,
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountChange=models.RowCountChangeClass(
                            type=models.AssertionValueChangeTypeClass.ABSOLUTE,
                            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="25",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_absolute_greater_than_or_equal_to",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE,
                        parameters=50,
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountChange=models.RowCountChangeClass(
                            type=models.AssertionValueChangeTypeClass.ABSOLUTE,
                            operator=models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="50",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_absolute_less_than_or_equal_to",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE,
                        parameters=(5, 25),
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountChange=models.RowCountChangeClass(
                            type=models.AssertionValueChangeTypeClass.ABSOLUTE,
                            operator=models.AssertionStdOperatorClass.BETWEEN,
                            parameters=models.AssertionStdParametersClass(
                                minValue=models.AssertionStdParameterClass(
                                    value="5",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                                maxValue=models.AssertionStdParameterClass(
                                    value="25",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_absolute_between",
            ),
            # RowCountChange with percentage kind
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE,
                        parameters=15,
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountChange=models.RowCountChangeClass(
                            type=models.AssertionValueChangeTypeClass.PERCENTAGE,
                            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="15",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_percentage_greater_than_or_equal_to",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE,
                        parameters=75,
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountChange=models.RowCountChangeClass(
                            type=models.AssertionValueChangeTypeClass.PERCENTAGE,
                            operator=models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="75",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_percentage_less_than_or_equal_to",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE,
                        parameters=(10, 30),
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountChange=models.RowCountChangeClass(
                            type=models.AssertionValueChangeTypeClass.PERCENTAGE,
                            operator=models.AssertionStdOperatorClass.BETWEEN,
                            parameters=models.AssertionStdParametersClass(
                                minValue=models.AssertionStdParameterClass(
                                    value="10",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                                maxValue=models.AssertionStdParameterClass(
                                    value="30",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_percentage_between",
            ),
            # ============ BETWEEN PARAMETER SORTING TESTS ============
            # Test that values are sorted so minValue is always <= maxValue
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(200, 100),  # Provided in reversed order
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.BETWEEN,
                            parameters=models.AssertionStdParametersClass(
                                minValue=models.AssertionStdParameterClass(
                                    value="100",  # Smaller value becomes minValue regardless of position
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                                maxValue=models.AssertionStdParameterClass(
                                    value="200",  # Larger value becomes maxValue regardless of position
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="between_operator_sorts_values_row_count_total_reversed",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE,
                        parameters=(50, 20),  # Provided in reversed order
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountChange=models.RowCountChangeClass(
                            type=models.AssertionValueChangeTypeClass.PERCENTAGE,
                            operator=models.AssertionStdOperatorClass.BETWEEN,
                            parameters=models.AssertionStdParametersClass(
                                minValue=models.AssertionStdParameterClass(
                                    value="20",  # Smaller value becomes minValue regardless of position
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                                maxValue=models.AssertionStdParameterClass(
                                    value="50",  # Larger value becomes maxValue regardless of position
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="between_operator_sorts_values_row_count_change_reversed",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(100, 100),  # Equal values
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.BETWEEN,
                            parameters=models.AssertionStdParametersClass(
                                minValue=models.AssertionStdParameterClass(
                                    value="100",  # Both values are equal
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                                maxValue=models.AssertionStdParameterClass(
                                    value="100",  # Both values are equal
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="between_operator_handles_equal_values",
            ),
            # ============ FILTER TEST CASE ============
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    input_filter=_TEST_FILTER,
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        filter=_TEST_FILTER,
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="100",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_with_filter",
            ),
            # ============ FLOAT PARAMETERS TEST CASES ============
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=100.5,
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="100.5",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_with_float_parameter",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(10.5, 20.8),
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.BETWEEN,
                            parameters=models.AssertionStdParametersClass(
                                minValue=models.AssertionStdParameterClass(
                                    value="10.5",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                                maxValue=models.AssertionStdParameterClass(
                                    value="20.8",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_range_with_float_parameters",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE,
                        parameters=15.7,
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountChange=models.RowCountChangeClass(
                            type=models.AssertionValueChangeTypeClass.PERCENTAGE,
                            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="15.7",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_percentage_with_float_parameter",
            ),
            # ============ EDGE CASE PARAMETERS ============
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=0,
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="0",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_with_zero_parameter",
            ),
            pytest.param(
                VolumeAssertionCriteriaBuildModelTestParams(
                    input_criteria=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(0, 1),
                    ),
                    expected_value=models.VolumeAssertionInfoClass(
                        type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                        entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        rowCountTotal=models.RowCountTotalClass(
                            operator=models.AssertionStdOperatorClass.BETWEEN,
                            parameters=models.AssertionStdParametersClass(
                                minValue=models.AssertionStdParameterClass(
                                    value="0",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                                maxValue=models.AssertionStdParameterClass(
                                    value="1",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_range_with_zero_min",
            ),
        ],
    )
    def test_build_model_volume_info_comprehensive(
        self, test_params: VolumeAssertionCriteriaBuildModelTestParams
    ) -> None:
        """Comprehensive test for VolumeAssertionCriteria.build_model_volume_info method."""
        if test_params.should_succeed:
            # Test successful cases
            result = VolumeAssertionCriteria.build_model_volume_info(
                test_params.input_criteria,
                test_params.input_dataset_urn,
                test_params.input_filter,
            )

            # For successful cases, expected_value must be provided
            assert test_params.expected_value is not None, (
                "expected_value must be provided for successful test cases"
            )
            assert result == test_params.expected_value

            # Validate filter field matches expected
            if test_params.input_filter is not None:
                assert result.filter == test_params.input_filter
            else:
                assert not hasattr(result, "filter") or result.filter is None
        else:
            # Test failure cases
            with pytest.raises(SDKUsageError) as exc_info:
                VolumeAssertionCriteria.build_model_volume_info(
                    test_params.input_criteria,
                    test_params.input_dataset_urn,
                    test_params.input_filter,
                )

            if test_params.expected_error:
                assert test_params.expected_error in str(exc_info.value)


def test_volume_assertion_criteria_validation_errors() -> None:
    """Test that VolumeAssertionCriteria.parse validates parameters correctly."""
    # Test that range conditions require tuple parameters
    with pytest.raises(SDKUsageError) as exc_info:
        VolumeAssertionCriteria.parse(
            {
                "condition": "ROW_COUNT_IS_WITHIN_A_RANGE",
                "parameters": 100,  # Should be tuple for range
            }
        )
    assert (
        "For WITHIN_A_RANGE condition ROW_COUNT_IS_WITHIN_A_RANGE, parameters must be a tuple of two numbers (min_value, max_value)."
        in str(exc_info.value)
    )


@dataclass
class VolumeAssertionCriteriaFromAssertionTestParams:
    """Test parameters for VolumeAssertionDefinition.from_assertion method."""

    input_assertion: Assertion
    expected_value: Optional[VolumeAssertionCriteria] = None
    expected_error: Optional[str] = None
    should_succeed: bool = True


class TestVolumeAssertionCriteriaFromAssertion:
    """Test suite for VolumeAssertionCriteria.from_assertion method."""

    @pytest.mark.parametrize(
        "test_params",
        [
            # ============ SUCCESSFUL CASES ============
            # RowCountTotal with GREATER_THAN_OR_EQUAL_TO operator
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountTotal=models.RowCountTotalClass(
                                operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                                parameters=models.AssertionStdParametersClass(
                                    value=models.AssertionStdParameterClass(
                                        value="100",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_greater_than_or_equal_to_single_value",
            ),
            # RowCountTotal with LESS_THAN_OR_EQUAL_TO operator
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountTotal=models.RowCountTotalClass(
                                operator=models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
                                parameters=models.AssertionStdParametersClass(
                                    value=models.AssertionStdParameterClass(
                                        value="500",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_LESS_THAN_OR_EQUAL_TO,
                        parameters=500,
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_less_than_or_equal_to_single_value",
            ),
            # RowCountTotal with BETWEEN operator
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountTotal=models.RowCountTotalClass(
                                operator=models.AssertionStdOperatorClass.BETWEEN,
                                parameters=models.AssertionStdParametersClass(
                                    minValue=models.AssertionStdParameterClass(
                                        value="100",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                    maxValue=models.AssertionStdParameterClass(
                                        value="200",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_IS_WITHIN_A_RANGE,
                        parameters=(100, 200),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_between_range_values",
            ),
            # RowCountChange with absolute kind and GREATER_THAN_OR_EQUAL_TO operator
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=models.RowCountChangeClass(
                                type=models.AssertionValueChangeTypeClass.ABSOLUTE,
                                operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                                parameters=models.AssertionStdParametersClass(
                                    value=models.AssertionStdParameterClass(
                                        value="25",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_LEAST_ABSOLUTE,
                        parameters=25,
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_absolute_greater_than_or_equal_to_single_value",
            ),
            # RowCountChange with absolute kind and LESS_THAN_OR_EQUAL_TO operator
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=models.RowCountChangeClass(
                                type=models.AssertionValueChangeTypeClass.ABSOLUTE,
                                operator=models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
                                parameters=models.AssertionStdParametersClass(
                                    value=models.AssertionStdParameterClass(
                                        value="50",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_MOST_ABSOLUTE,
                        parameters=50,
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_absolute_less_than_or_equal_to_single_value",
            ),
            # RowCountChange with absolute kind and BETWEEN operator
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=models.RowCountChangeClass(
                                type=models.AssertionValueChangeTypeClass.ABSOLUTE,
                                operator=models.AssertionStdOperatorClass.BETWEEN,
                                parameters=models.AssertionStdParametersClass(
                                    minValue=models.AssertionStdParameterClass(
                                        value="5",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                    maxValue=models.AssertionStdParameterClass(
                                        value="25",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_WITHIN_A_RANGE_ABSOLUTE,
                        parameters=(5, 25),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_absolute_between_range_values",
            ),
            # RowCountChange with percentage kind and GREATER_THAN_OR_EQUAL_TO operator
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=models.RowCountChangeClass(
                                type=models.AssertionValueChangeTypeClass.PERCENTAGE,
                                operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                                parameters=models.AssertionStdParametersClass(
                                    value=models.AssertionStdParameterClass(
                                        value="15",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_LEAST_PERCENTAGE,
                        parameters=15,
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_percentage_greater_than_or_equal_to_single_value",
            ),
            # RowCountChange with percentage kind and LESS_THAN_OR_EQUAL_TO operator
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=models.RowCountChangeClass(
                                type=models.AssertionValueChangeTypeClass.PERCENTAGE,
                                operator=models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
                                parameters=models.AssertionStdParametersClass(
                                    value=models.AssertionStdParameterClass(
                                        value="75",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_BY_AT_MOST_PERCENTAGE,
                        parameters=75,
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_percentage_less_than_or_equal_to_single_value",
            ),
            # RowCountChange with percentage kind and BETWEEN operator
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=models.RowCountChangeClass(
                                type=models.AssertionValueChangeTypeClass.PERCENTAGE,
                                operator=models.AssertionStdOperatorClass.BETWEEN,
                                parameters=models.AssertionStdParametersClass(
                                    minValue=models.AssertionStdParameterClass(
                                        value="10",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                    maxValue=models.AssertionStdParameterClass(
                                        value="30",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_value=VolumeAssertionCriteria(
                        condition=VolumeAssertionCondition.ROW_COUNT_GROWS_WITHIN_A_RANGE_PERCENTAGE,
                        parameters=(10, 30),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_percentage_between_range_values",
            ),
            # ============ ERROR CASES ============
            # Assertion with no info - create mock with info set to None
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Mock(
                        spec=Assertion,
                        urn=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=None,
                    ),
                    expected_error="Assertion urn:li:assertion:test does not have a volume assertion info, which is not supported",
                    should_succeed=False,
                ),
                id="assertion_with_no_info",
            ),
            # Assertion with wrong assertion info type (not VolumeAssertionInfoClass)
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.FreshnessAssertionInfoClass(
                            type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        ),
                    ),
                    expected_error="Assertion urn:li:assertion:test is not a volume assertion",
                    should_succeed=False,
                ),
                id="assertion_with_wrong_info_type_freshness",
            ),
            # Assertion with wrong assertion info type (DatasetAssertionInfo instead of VolumeAssertionInfo)
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.DatasetAssertionInfoClass(
                            dataset="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            scope=models.DatasetAssertionScopeClass.DATASET_ROWS,
                            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                            parameters=models.AssertionStdParametersClass(
                                value=models.AssertionStdParameterClass(
                                    value="100",
                                    type=models.AssertionStdParameterTypeClass.NUMBER,
                                ),
                            ),
                        ),
                    ),
                    expected_error="Assertion urn:li:assertion:test is not a volume assertion",
                    should_succeed=False,
                ),
                id="assertion_with_wrong_info_type_dataset",
            ),
            # Unsupported volume assertion type (hypothetical future type)
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type="UNSUPPORTED_TYPE",  # type: ignore[arg-type]
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                        ),
                    ),
                    expected_error="Unsupported volume assertion type: UNSUPPORTED_TYPE",
                    should_succeed=False,
                ),
                id="unsupported_volume_assertion_type",
            ),
            # RowCountTotal with missing rowCountTotal field
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountTotal=None,  # Missing required field
                        ),
                    ),
                    expected_error="Volume assertion urn:li:assertion:test has ROW_COUNT_TOTAL type but no rowCountTotal",
                    should_succeed=False,
                ),
                id="row_count_total_missing_row_count_total_field",
            ),
            # RowCountChange with missing rowCountChange field
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=None,  # Missing required field
                        ),
                    ),
                    expected_error="Volume assertion urn:li:assertion:test has ROW_COUNT_CHANGE type but no rowCountChange",
                    should_succeed=False,
                ),
                id="row_count_change_missing_row_count_change_field",
            ),
            # RowCountTotal with BETWEEN operator but missing minValue
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountTotal=models.RowCountTotalClass(
                                operator=models.AssertionStdOperatorClass.BETWEEN,
                                parameters=models.AssertionStdParametersClass(
                                    minValue=None,  # Missing for BETWEEN operator
                                    maxValue=models.AssertionStdParameterClass(
                                        value="200",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_error="Volume assertion urn:li:assertion:test has BETWEEN operator but missing min/max values",
                    should_succeed=False,
                ),
                id="row_count_total_between_missing_min_value",
            ),
            # RowCountTotal with BETWEEN operator but missing maxValue
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountTotal=models.RowCountTotalClass(
                                operator=models.AssertionStdOperatorClass.BETWEEN,
                                parameters=models.AssertionStdParametersClass(
                                    minValue=models.AssertionStdParameterClass(
                                        value="100",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                    maxValue=None,  # Missing for BETWEEN operator
                                ),
                            ),
                        ),
                    ),
                    expected_error="Volume assertion urn:li:assertion:test has BETWEEN operator but missing min/max values",
                    should_succeed=False,
                ),
                id="row_count_total_between_missing_max_value",
            ),
            # RowCountTotal with single-value operator but missing value
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountTotal=models.RowCountTotalClass(
                                operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                                parameters=models.AssertionStdParametersClass(
                                    value=None,  # Missing for single-value operator
                                ),
                            ),
                        ),
                    ),
                    expected_error="Volume assertion urn:li:assertion:test has GREATER_THAN_OR_EQUAL_TO operator but missing value",
                    should_succeed=False,
                ),
                id="row_count_total_single_value_missing_value",
            ),
            # RowCountChange with BETWEEN operator but missing minValue
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=models.RowCountChangeClass(
                                type=models.AssertionValueChangeTypeClass.ABSOLUTE,
                                operator=models.AssertionStdOperatorClass.BETWEEN,
                                parameters=models.AssertionStdParametersClass(
                                    minValue=None,  # Missing for BETWEEN operator
                                    maxValue=models.AssertionStdParameterClass(
                                        value="25",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    expected_error="Volume assertion urn:li:assertion:test has BETWEEN operator but missing min/max values",
                    should_succeed=False,
                ),
                id="row_count_change_between_missing_min_value",
            ),
            # RowCountChange with BETWEEN operator but missing maxValue
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=models.RowCountChangeClass(
                                type=models.AssertionValueChangeTypeClass.PERCENTAGE,
                                operator=models.AssertionStdOperatorClass.BETWEEN,
                                parameters=models.AssertionStdParametersClass(
                                    minValue=models.AssertionStdParameterClass(
                                        value="10",
                                        type=models.AssertionStdParameterTypeClass.NUMBER,
                                    ),
                                    maxValue=None,  # Missing for BETWEEN operator
                                ),
                            ),
                        ),
                    ),
                    expected_error="Volume assertion urn:li:assertion:test has BETWEEN operator but missing min/max values",
                    should_succeed=False,
                ),
                id="row_count_change_between_missing_max_value",
            ),
            # RowCountChange with single-value operator but missing value
            pytest.param(
                VolumeAssertionCriteriaFromAssertionTestParams(
                    input_assertion=Assertion(
                        id=AssertionUrn.from_string("urn:li:assertion:test"),
                        info=models.VolumeAssertionInfoClass(
                            type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
                            entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                            rowCountChange=models.RowCountChangeClass(
                                type=models.AssertionValueChangeTypeClass.ABSOLUTE,
                                operator=models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
                                parameters=models.AssertionStdParametersClass(
                                    value=None,  # Missing for single-value operator
                                ),
                            ),
                        ),
                    ),
                    expected_error="Volume assertion urn:li:assertion:test has LESS_THAN_OR_EQUAL_TO operator but missing value",
                    should_succeed=False,
                ),
                id="row_count_change_single_value_missing_value",
            ),
        ],
    )
    def test_from_assertion_comprehensive(
        self, test_params: VolumeAssertionCriteriaFromAssertionTestParams
    ) -> None:
        """Comprehensive test for VolumeAssertionCriteria.from_assertion method."""
        if test_params.should_succeed:
            # Test successful cases
            result = VolumeAssertionCriteria.from_assertion(test_params.input_assertion)

            # For successful cases, expected_value must be provided
            assert test_params.expected_value is not None, (
                "expected_value must be provided for successful test cases"
            )
            assert result == test_params.expected_value
        else:
            # Test failure cases
            with pytest.raises(SDKNotYetSupportedError) as exc_info:
                VolumeAssertionCriteria.from_assertion(test_params.input_assertion)

            if test_params.expected_error:
                assert test_params.expected_error in str(exc_info.value)


def test_parse_volume_assertion_definition_no_mutate_input_dict() -> None:
    """Test that calling VolumeAssertionCriteria.parse() does not mutate the input dictionary."""
    # Create a dictionary that should NOT be mutated by parse() calls
    definition_dict = {
        "condition": "ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO",
        "parameters": 100,
    }
    original_dict = definition_dict.copy()  # Keep a copy to verify no mutation

    # Call parse and verify it succeeds
    result = VolumeAssertionCriteria.parse(definition_dict)
    assert isinstance(result, VolumeAssertionCriteria)
    assert (
        result.condition
        == VolumeAssertionCondition.ROW_COUNT_IS_GREATER_THAN_OR_EQUAL_TO
    )
    assert result.parameters == 100

    # Verify the original dict was not mutated
    assert definition_dict == original_dict, "Original dictionary should not be mutated"
