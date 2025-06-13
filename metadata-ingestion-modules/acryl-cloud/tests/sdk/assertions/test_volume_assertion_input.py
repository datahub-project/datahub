from dataclasses import dataclass
from typing import Any, Optional, Type, Union
from unittest.mock import Mock

import pytest

from acryl_datahub_cloud.sdk.assertion_input.volume_assertion_input import (
    RowCountChange,
    RowCountTotal,
    VolumeAssertionDefinition,
    VolumeAssertionOperator,
    _VolumeAssertionDefinitionTypes,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models

# Create a mock filter to use in tests
_TEST_FILTER = Mock(spec=models.DatasetFilterClass)


@dataclass
class VolumeAssertionDefinitionTestParams:
    """Test parameters for VolumeAssertionDefinition.parse method."""

    input_definition: Union[dict[str, Any], RowCountTotal, RowCountChange]
    expected_type: Optional[Type] = None
    expected_value: Optional[_VolumeAssertionDefinitionTypes] = None
    expected_error: Optional[str] = None
    should_succeed: bool = True


class TestVolumeAssertionDefinitionParse:
    """Comprehensive test suite for VolumeAssertionDefinition.parse method."""

    @pytest.mark.parametrize(
        "test_params",
        [
            # ============ SUCCESSFUL CASES ============
            # Test already instantiated objects that pass isinstance check
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition=RowCountTotal(
                        operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    expected_type=RowCountTotal,
                    expected_value=RowCountTotal(
                        operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    should_succeed=True,
                ),
                id="already_instantiated_row_count_total",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition=RowCountChange(
                        kind="absolute",
                        operator=VolumeAssertionOperator.LESS_THAN_OR_EQUAL_TO,
                        parameters=50,
                    ),
                    expected_type=RowCountChange,
                    expected_value=RowCountChange(
                        kind="absolute",
                        operator=VolumeAssertionOperator.LESS_THAN_OR_EQUAL_TO,
                        parameters=50,
                    ),
                    should_succeed=True,
                ),
                id="already_instantiated_row_count_change",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition=RowCountTotal(
                        operator=VolumeAssertionOperator.BETWEEN,
                        parameters=(100, 200),
                    ),
                    expected_type=RowCountTotal,
                    expected_value=RowCountTotal(
                        operator=VolumeAssertionOperator.BETWEEN,
                        parameters=(100, 200),
                    ),
                    should_succeed=True,
                ),
                id="already_instantiated_row_count_total_between",
            ),
            # ============ SUCCESSFUL DICT PARSING WITH INT AND TUPLE PARAMETERS ============
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "operator": "GREATER_THAN_OR_EQUAL_TO",
                        "parameters": 100,
                    },
                    expected_type=RowCountTotal,
                    expected_value=RowCountTotal(
                        operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
                        parameters=100,
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_with_int_parameter",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_change",
                        "kind": "absolute",
                        "operator": "LESS_THAN_OR_EQUAL_TO",
                        "parameters": 50,
                    },
                    expected_type=RowCountChange,
                    expected_value=RowCountChange(
                        kind="absolute",
                        operator=VolumeAssertionOperator.LESS_THAN_OR_EQUAL_TO,
                        parameters=50,
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_with_int_parameter",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "operator": "BETWEEN",
                        "parameters": (100, 200),
                    },
                    expected_type=RowCountTotal,
                    expected_value=RowCountTotal(
                        operator=VolumeAssertionOperator.BETWEEN,
                        parameters=(100, 200),
                    ),
                    should_succeed=True,
                ),
                id="row_count_total_between_with_tuple_parameters",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_change",
                        "kind": "percent",
                        "operator": "BETWEEN",
                        "parameters": (10, 50),
                    },
                    expected_type=RowCountChange,
                    expected_value=RowCountChange(
                        kind="percent",
                        operator=VolumeAssertionOperator.BETWEEN,
                        parameters=(10, 50),
                    ),
                    should_succeed=True,
                ),
                id="row_count_change_between_with_tuple_parameters",
            ),
            # ============ INPUT TYPE VALIDATION FAILURES ============
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition="invalid_string",  # type: ignore[arg-type]
                    expected_error="Volume assertion definition must be a dict or a volume assertion definition object, got: <class 'str'>",
                    should_succeed=False,
                ),
                id="invalid_input_type_string",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition=None,  # type: ignore[arg-type]
                    expected_error="Volume assertion definition must be a dict or a volume assertion definition object, got: <class 'NoneType'>",
                    should_succeed=False,
                ),
                id="invalid_input_type_none",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition=123,  # type: ignore[arg-type]
                    expected_error="Volume assertion definition must be a dict or a volume assertion definition object, got: <class 'int'>",
                    should_succeed=False,
                ),
                id="invalid_input_type_int",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition=["item1", "item2"],  # type: ignore[arg-type]
                    expected_error="Volume assertion definition must be a dict or a volume assertion definition object, got: <class 'list'>",
                    should_succeed=False,
                ),
                id="invalid_input_type_list",
            ),
            # ============ DICT STRUCTURE VALIDATION FAILURES ============
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "operator": "GREATER_THAN_OR_EQUAL_TO",
                        "parameters": 100,
                    },
                    expected_error="Volume assertion definition must include a 'type' field",
                    should_succeed=False,
                ),
                id="missing_type_field",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "unknown_type",
                        "operator": "GREATER_THAN_OR_EQUAL_TO",
                    },
                    expected_error="Unknown volume assertion type: unknown_type. Supported types: row_count_total, row_count_change",
                    should_succeed=False,
                ),
                id="unknown_assertion_type",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "",
                        "operator": "GREATER_THAN_OR_EQUAL_TO",
                    },
                    expected_error="Unknown volume assertion type: . Supported types: row_count_total, row_count_change",
                    should_succeed=False,
                ),
                id="empty_assertion_type",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "ROW_COUNT_TOTAL",  # Wrong case
                        "operator": "GREATER_THAN_OR_EQUAL_TO",
                        "parameters": 100,
                    },
                    expected_error="Unknown volume assertion type: ROW_COUNT_TOTAL",
                    should_succeed=False,
                ),
                id="case_sensitive_type_validation",
            ),
            # ============ MISSING REQUIRED FIELDS ============
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "parameters": 100,
                    },
                    expected_error="Missing required 'operator' field for row_count_total",
                    should_succeed=False,
                ),
                id="missing_operator_field_row_count_total",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_change",
                        "operator": "GREATER_THAN_OR_EQUAL_TO",
                    },
                    expected_error="Missing required 'parameters' field for row_count_change",
                    should_succeed=False,
                ),
                id="missing_parameters_field_row_count_change",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        # Missing both operator and parameters
                    },
                    expected_error="Missing required 'operator' field for row_count_total",
                    should_succeed=False,
                ),
                id="missing_both_operator_and_parameters",
            ),
            # ============ INVALID OPERATORS ============
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "operator": "GREATER_THAN",
                        "parameters": 100,
                    },
                    expected_error="Invalid operator 'GREATER_THAN'. Valid operators:",
                    should_succeed=False,
                ),
                id="invalid_operator_greater_than",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_change",
                        "operator": "LESS_THAN",
                        "parameters": 100,
                    },
                    expected_error="Invalid operator 'LESS_THAN'. Valid operators:",
                    should_succeed=False,
                ),
                id="invalid_operator_less_than",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "operator": "EQUAL_TO",
                        "parameters": 100,
                    },
                    expected_error="Invalid operator 'EQUAL_TO'. Valid operators:",
                    should_succeed=False,
                ),
                id="invalid_operator_equal_to",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_change",
                        "operator": "CONTAIN",
                        "parameters": 100,
                    },
                    expected_error="Invalid operator 'CONTAIN'. Valid operators:",
                    should_succeed=False,
                ),
                id="invalid_operator_contain",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "operator": "INVALID_OPERATOR",
                        "parameters": 100,
                    },
                    expected_error="Invalid operator 'INVALID_OPERATOR'. Valid operators:",
                    should_succeed=False,
                ),
                id="completely_invalid_operator",
            ),
            # ============ PARAMETER VALIDATION FAILURES ============
            # ============ INT/TUPLE PARAMETER VALIDATION FAILURES ============
            # Wrong parameter type for single-value operators
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "operator": "GREATER_THAN_OR_EQUAL_TO",
                        "parameters": (100, 200),  # Should be single int, not tuple
                    },
                    expected_error="For GREATER_THAN_OR_EQUAL_TO operator in row_count_total, parameters must be a single integer, not a tuple",
                    should_succeed=False,
                ),
                id="single_value_operator_rejects_tuple_parameters",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_change",
                        "kind": "absolute",
                        "operator": "LESS_THAN_OR_EQUAL_TO",
                        "parameters": "100",  # Should be int, not string
                    },
                    expected_error="For row_count_change, parameters must be an integer or a tuple of two integers, got: <class 'str'>",
                    should_succeed=False,
                ),
                id="single_value_operator_rejects_string_parameters",
            ),
            # Wrong parameter type for BETWEEN operator
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "operator": "BETWEEN",
                        "parameters": 100,  # Should be tuple, not int
                    },
                    expected_error="For BETWEEN operator in row_count_total, parameters must be a tuple of two integers (min_value, max_value)",
                    should_succeed=False,
                ),
                id="between_operator_rejects_int_parameters",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_change",
                        "kind": "percent",
                        "operator": "BETWEEN",
                        "parameters": (100,),  # Should be tuple of 2, not 1
                    },
                    expected_error="For BETWEEN operator in row_count_change, parameters must be a tuple of two integers (min_value, max_value)",
                    should_succeed=False,
                ),
                id="between_operator_rejects_single_element_tuple",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "operator": "BETWEEN",
                        "parameters": (100, 200, 300),  # Should be tuple of 2, not 3
                    },
                    expected_error="For BETWEEN operator in row_count_total, parameters must be a tuple of two integers (min_value, max_value)",
                    should_succeed=False,
                ),
                id="between_operator_rejects_three_element_tuple",
            ),
            # ============ OBJECT CONSTRUCTION FAILURES (valid operators but other issues) ============
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_change",
                        "operator": "GREATER_THAN_OR_EQUAL_TO",
                        "parameters": 100,
                        # Missing required 'kind' field for row_count_change
                    },
                    expected_error="Failed to create row_count_change volume assertion:",
                    should_succeed=False,
                ),
                id="row_count_change_missing_kind_field",
            ),
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={
                        "type": "row_count_total",
                        "operator": "BETWEEN",
                        "parameters": (100, 200),
                        "extra_invalid_field": "should_cause_failure",  # Extra field should cause Pydantic validation failure
                    },
                    expected_error="Failed to create row_count_total volume assertion:",
                    should_succeed=False,
                ),
                id="object_construction_fails_with_extra_fields",
            ),
            # ============ INTEGRATION TESTS ============
            pytest.param(
                VolumeAssertionDefinitionTestParams(
                    input_definition={},
                    expected_error="Volume assertion definition must include a 'type' field",
                    should_succeed=False,
                ),
                id="empty_dict_handling",
            ),
        ],
    )
    def test_parse_comprehensive(
        self, test_params: VolumeAssertionDefinitionTestParams
    ) -> None:
        """Comprehensive test for VolumeAssertionDefinition.parse method."""
        if test_params.should_succeed:
            # Test successful cases
            result = VolumeAssertionDefinition.parse(test_params.input_definition)
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
                VolumeAssertionDefinition.parse(test_params.input_definition)

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
            VolumeAssertionDefinition.parse(original_dict)

        # Original dict should be unchanged
        assert original_dict == dict_copy


@dataclass
class VolumeAssertionDefinitionBuildModelTestParams:
    """Test parameters for VolumeAssertionDefinition.build_model_volume_info method."""

    input_definition: Union[RowCountTotal, RowCountChange, Any]
    input_dataset_urn: str = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
    )
    input_filter: Optional["models.DatasetFilterClass"] = None
    expected_value: Optional["models.VolumeAssertionInfoClass"] = None
    expected_error: Optional[str] = None
    should_succeed: bool = True


class TestVolumeAssertionDefinitionBuildModelVolumeInfo:
    """Test suite for VolumeAssertionDefinition.build_model_volume_info method."""

    @pytest.mark.parametrize(
        "test_params",
        [
            # ============ SUCCESSFUL CASES ============
            # RowCountTotal with single value operators
            pytest.param(
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountTotal(
                        operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountTotal(
                        operator=VolumeAssertionOperator.LESS_THAN_OR_EQUAL_TO,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountTotal(
                        operator=VolumeAssertionOperator.BETWEEN,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountChange(
                        kind="absolute",
                        operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountChange(
                        kind="absolute",
                        operator=VolumeAssertionOperator.LESS_THAN_OR_EQUAL_TO,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountChange(
                        kind="absolute",
                        operator=VolumeAssertionOperator.BETWEEN,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountChange(
                        kind="percent",
                        operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountChange(
                        kind="percent",
                        operator=VolumeAssertionOperator.LESS_THAN_OR_EQUAL_TO,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountChange(
                        kind="percent",
                        operator=VolumeAssertionOperator.BETWEEN,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountTotal(
                        operator=VolumeAssertionOperator.BETWEEN,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountChange(
                        kind="percent",
                        operator=VolumeAssertionOperator.BETWEEN,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountTotal(
                        operator=VolumeAssertionOperator.BETWEEN,
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
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=RowCountTotal(
                        operator=VolumeAssertionOperator.GREATER_THAN_OR_EQUAL_TO,
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
            # ============ FAILURE CASES ============
            # Unsupported definition type
            pytest.param(
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition="invalid_definition_type",  # type: ignore[arg-type]
                    expected_error="Unsupported volume assertion definition type",
                    should_succeed=False,
                ),
                id="unsupported_definition_type_string",
            ),
            pytest.param(
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition=None,  # type: ignore[arg-type]
                    expected_error="Unsupported volume assertion definition type",
                    should_succeed=False,
                ),
                id="unsupported_definition_type_none",
            ),
            pytest.param(
                VolumeAssertionDefinitionBuildModelTestParams(
                    input_definition={"type": "unknown"},  # type: ignore[arg-type]
                    expected_error="Unsupported volume assertion definition type",
                    should_succeed=False,
                ),
                id="unsupported_definition_type_dict",
            ),
        ],
    )
    def test_build_model_volume_info_comprehensive(
        self, test_params: VolumeAssertionDefinitionBuildModelTestParams
    ) -> None:
        """Comprehensive test for VolumeAssertionDefinition.build_model_volume_info method."""
        if test_params.should_succeed:
            # Test successful cases
            result = VolumeAssertionDefinition.build_model_volume_info(
                test_params.input_definition,
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
                VolumeAssertionDefinition.build_model_volume_info(
                    test_params.input_definition,
                    test_params.input_dataset_urn,
                    test_params.input_filter,
                )

            if test_params.expected_error:
                assert test_params.expected_error in str(exc_info.value)
