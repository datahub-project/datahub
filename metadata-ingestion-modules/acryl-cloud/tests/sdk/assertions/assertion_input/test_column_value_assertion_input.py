"""Tests for _ColumnValueAssertionInput class."""

from datetime import datetime, timezone

import pytest

from acryl_datahub_cloud.sdk.assertion_input.column_value_assertion_input import (
    FailThresholdType,
    FieldTransformType,
    SqlExpression,
    _ColumnValueAssertionInput,
    _try_parse_fail_threshold_type,
    _try_parse_field_transform_type,
    _validate_fail_threshold_value,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata import schema_classes as models
from tests.sdk.assertions.conftest import StubEntityClient


@pytest.fixture
def stub_entity_client() -> StubEntityClient:
    return StubEntityClient()


class TestFailThresholdTypeParsing:
    """Tests for fail threshold type parsing."""

    def test_parse_count_case_insensitive(self) -> None:
        assert _try_parse_fail_threshold_type("COUNT") == FailThresholdType.COUNT
        assert _try_parse_fail_threshold_type("count") == FailThresholdType.COUNT

    def test_parse_percentage_string(self) -> None:
        result = _try_parse_fail_threshold_type("PERCENTAGE")
        assert result == FailThresholdType.PERCENTAGE

    def test_parse_enum_value(self) -> None:
        result = _try_parse_fail_threshold_type(FailThresholdType.PERCENTAGE)
        assert result == FailThresholdType.PERCENTAGE

    def test_parse_none_defaults_to_count(self) -> None:
        result = _try_parse_fail_threshold_type(None)
        assert result == FailThresholdType.COUNT

    def test_parse_invalid_raises_error(self) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            _try_parse_fail_threshold_type("INVALID")
        assert "Invalid fail threshold type" in str(exc_info.value)


class TestFieldTransformTypeParsing:
    """Tests for field transform type parsing."""

    def test_parse_length_case_insensitive(self) -> None:
        assert (
            _try_parse_field_transform_type("LENGTH")
            == models.FieldTransformTypeClass.LENGTH
        )
        assert (
            _try_parse_field_transform_type("length")
            == models.FieldTransformTypeClass.LENGTH
        )

    def test_parse_enum_value(self) -> None:
        result = _try_parse_field_transform_type(FieldTransformType.LENGTH)
        assert result == models.FieldTransformTypeClass.LENGTH

    def test_parse_none_returns_none(self) -> None:
        result = _try_parse_field_transform_type(None)
        assert result is None

    def test_parse_invalid_raises_error(self) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            _try_parse_field_transform_type("INVALID")
        assert "Invalid field transform type" in str(exc_info.value)


class TestFailThresholdValueValidation:
    """Tests for fail threshold value validation."""

    def test_valid_count_value(self) -> None:
        # Should not raise
        _validate_fail_threshold_value(FailThresholdType.COUNT, 0)
        _validate_fail_threshold_value(FailThresholdType.COUNT, 100)
        _validate_fail_threshold_value(FailThresholdType.COUNT, 1000)

    def test_valid_percentage_value(self) -> None:
        # Should not raise
        _validate_fail_threshold_value(FailThresholdType.PERCENTAGE, 0)
        _validate_fail_threshold_value(FailThresholdType.PERCENTAGE, 50)
        _validate_fail_threshold_value(FailThresholdType.PERCENTAGE, 100)

    def test_negative_value_raises_error(self) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            _validate_fail_threshold_value(FailThresholdType.COUNT, -1)
        assert "must be non-negative" in str(exc_info.value)

    def test_percentage_over_100_raises_error(self) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            _validate_fail_threshold_value(FailThresholdType.PERCENTAGE, 101)
        assert "between 0 and 100" in str(exc_info.value)


class TestColumnValueAssertionInput:
    """Tests for _ColumnValueAssertionInput class."""

    @pytest.fixture
    def base_params(self, stub_entity_client: StubEntityClient) -> dict:
        """Base parameters for creating a column value assertion input."""
        return {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            "entity_client": stub_entity_client,
            "column_name": "string_column",
            "operator": models.AssertionStdOperatorClass.NOT_NULL,
            "created_by": "urn:li:corpuser:test",
            "created_at": datetime.now(timezone.utc),
            "updated_by": "urn:li:corpuser:test",
            "updated_at": datetime.now(timezone.utc),
        }

    def test_create_with_minimum_fields(self, base_params: dict) -> None:
        input_obj = _ColumnValueAssertionInput(**base_params)
        assert input_obj.column_name == "string_column"
        assert input_obj.operator == models.AssertionStdOperatorClass.NOT_NULL
        assert input_obj.fail_threshold_type == FailThresholdType.COUNT
        assert input_obj.fail_threshold_value == 0
        assert input_obj.exclude_nulls is True
        assert input_obj.transform is None

    def test_create_with_all_fields(self, base_params: dict) -> None:
        base_params.update(
            {
                "operator": models.AssertionStdOperatorClass.GREATER_THAN,
                "criteria_parameters": 5,
                "transform": FieldTransformType.LENGTH,
                "fail_threshold_type": FailThresholdType.PERCENTAGE,
                "fail_threshold_value": 5,
                "exclude_nulls": False,
            }
        )
        input_obj = _ColumnValueAssertionInput(**base_params)
        assert input_obj.column_name == "string_column"
        assert input_obj.operator == models.AssertionStdOperatorClass.GREATER_THAN
        assert input_obj.criteria_parameters == 5
        assert input_obj.transform == models.FieldTransformTypeClass.LENGTH
        assert input_obj.fail_threshold_type == FailThresholdType.PERCENTAGE
        assert input_obj.fail_threshold_value == 5
        assert input_obj.exclude_nulls is False

    def test_create_with_range_operator(self, base_params: dict) -> None:
        base_params.update(
            {
                "column_name": "number_column",
                "operator": models.AssertionStdOperatorClass.BETWEEN,
                "criteria_parameters": (0, 100),
            }
        )
        input_obj = _ColumnValueAssertionInput(**base_params)
        assert input_obj.operator == models.AssertionStdOperatorClass.BETWEEN
        assert input_obj.criteria_parameters == (0, 100)

    def test_invalid_operator_for_column_type_raises_error(
        self, base_params: dict
    ) -> None:
        # REGEX_MATCH is not valid for NUMBER columns
        base_params.update(
            {
                "column_name": "number_column",
                "operator": models.AssertionStdOperatorClass.REGEX_MATCH,
                "criteria_parameters": "^[0-9]+$",
            }
        )
        with pytest.raises(SDKUsageError) as exc_info:
            _ColumnValueAssertionInput(**base_params)
        assert "Operator" in str(exc_info.value)
        assert "not allowed" in str(exc_info.value)

    def test_length_transform_only_valid_for_string_columns(
        self, base_params: dict
    ) -> None:
        base_params.update(
            {
                "column_name": "number_column",
                "operator": models.AssertionStdOperatorClass.NOT_NULL,
                "transform": FieldTransformType.LENGTH,
            }
        )
        with pytest.raises(SDKUsageError) as exc_info:
            _ColumnValueAssertionInput(**base_params)
        assert "LENGTH transform is only valid for STRING columns" in str(
            exc_info.value
        )

    def test_transform_allows_operator_valid_for_output_type(
        self, base_params: dict
    ) -> None:
        # NOT_EQUAL_TO is not valid for STRING columns, but is valid for NUMBER
        # LENGTH transform converts STRING -> NUMBER, so this should succeed
        base_params.update(
            {
                "column_name": "string_column",
                "operator": models.AssertionStdOperatorClass.NOT_EQUAL_TO,
                "criteria_parameters": 0,
                "transform": FieldTransformType.LENGTH,
            }
        )
        input_obj = _ColumnValueAssertionInput(**base_params)
        assert input_obj.operator == models.AssertionStdOperatorClass.NOT_EQUAL_TO
        assert input_obj.transform == models.FieldTransformTypeClass.LENGTH
        assert input_obj.criteria_parameters == 0

    def test_value_parameter_required_for_value_operators(
        self, base_params: dict
    ) -> None:
        base_params.update(
            {
                "operator": models.AssertionStdOperatorClass.EQUAL_TO,
                "criteria_parameters": None,
            }
        )
        with pytest.raises(SDKUsageError) as exc_info:
            _ColumnValueAssertionInput(**base_params)
        assert "Single value is required" in str(exc_info.value)

    def test_range_parameter_required_for_range_operators(
        self, base_params: dict
    ) -> None:
        base_params.update(
            {
                "column_name": "number_column",
                "operator": models.AssertionStdOperatorClass.BETWEEN,
                "criteria_parameters": None,
            }
        )
        with pytest.raises(SDKUsageError) as exc_info:
            _ColumnValueAssertionInput(**base_params)
        assert "Range parameters are required" in str(exc_info.value)

    def test_value_parameter_not_allowed_for_no_parameter_operators(
        self, base_params: dict
    ) -> None:
        base_params.update(
            {
                "operator": models.AssertionStdOperatorClass.NOT_NULL,
                "criteria_parameters": "some_value",
            }
        )
        with pytest.raises(SDKUsageError) as exc_info:
            _ColumnValueAssertionInput(**base_params)
        assert "Value parameters should not be provided" in str(exc_info.value)

    def test_create_assertion_info_produces_field_values_assertion(
        self, base_params: dict
    ) -> None:
        input_obj = _ColumnValueAssertionInput(**base_params)
        assertion_info = input_obj._create_assertion_info(filter=None)

        assert isinstance(assertion_info, models.FieldAssertionInfoClass)
        assert assertion_info.type == models.FieldAssertionTypeClass.FIELD_VALUES
        assert assertion_info.fieldValuesAssertion is not None
        assert assertion_info.fieldMetricAssertion is None

    def test_create_assertion_info_with_transform(self, base_params: dict) -> None:
        base_params.update(
            {
                "operator": models.AssertionStdOperatorClass.GREATER_THAN,
                "criteria_parameters": 5,
                "transform": FieldTransformType.LENGTH,
            }
        )
        input_obj = _ColumnValueAssertionInput(**base_params)
        assertion_info = input_obj._create_assertion_info(filter=None)

        assert isinstance(assertion_info, models.FieldAssertionInfoClass)
        assert assertion_info.fieldValuesAssertion is not None
        assert assertion_info.fieldValuesAssertion.transform is not None
        assert (
            assertion_info.fieldValuesAssertion.transform.type
            == models.FieldTransformTypeClass.LENGTH
        )

    def test_create_assertion_info_with_fail_threshold(self, base_params: dict) -> None:
        base_params.update(
            {
                "fail_threshold_type": FailThresholdType.PERCENTAGE,
                "fail_threshold_value": 10,
            }
        )
        input_obj = _ColumnValueAssertionInput(**base_params)
        assertion_info = input_obj._create_assertion_info(filter=None)

        assert isinstance(assertion_info, models.FieldAssertionInfoClass)
        assert assertion_info.fieldValuesAssertion is not None
        assert (
            assertion_info.fieldValuesAssertion.failThreshold.type
            == models.FieldValuesFailThresholdTypeClass.PERCENTAGE
        )
        assert assertion_info.fieldValuesAssertion.failThreshold.value == 10

    def test_create_assertion_info_with_exclude_nulls_false(
        self, base_params: dict
    ) -> None:
        base_params.update({"exclude_nulls": False})
        input_obj = _ColumnValueAssertionInput(**base_params)
        assertion_info = input_obj._create_assertion_info(filter=None)

        assert isinstance(assertion_info, models.FieldAssertionInfoClass)
        assert assertion_info.fieldValuesAssertion is not None
        assert assertion_info.fieldValuesAssertion.excludeNulls is False


class TestSqlExpression:
    """Tests for SqlExpression class."""

    def test_sql_expression_basic_creation(self) -> None:
        sql = "SELECT id FROM valid_customers WHERE active = true"
        expr = SqlExpression(sql)
        assert expr.sql == sql

    def test_sql_expression_empty_raises_error(self) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            SqlExpression("")
        assert "cannot be empty" in str(exc_info.value)

    def test_sql_expression_whitespace_only_raises_error(self) -> None:
        with pytest.raises(SDKUsageError) as exc_info:
            SqlExpression("   ")
        assert "cannot be empty" in str(exc_info.value)


class TestColumnValueAssertionInputWithSql:
    """Tests for _ColumnValueAssertionInput with SQL expressions."""

    @pytest.fixture
    def base_params(self, stub_entity_client: StubEntityClient) -> dict:
        """Base parameters for creating a column value assertion input."""
        return {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)",
            "entity_client": stub_entity_client,
            "column_name": "string_column",
            "operator": models.AssertionStdOperatorClass.IN,
            "created_by": "urn:li:corpuser:test",
            "created_at": datetime.now(timezone.utc),
            "updated_by": "urn:li:corpuser:test",
            "updated_at": datetime.now(timezone.utc),
        }

    def test_create_with_sql_expression(self, base_params: dict) -> None:
        sql_expr = SqlExpression("SELECT id FROM valid_customers WHERE active = true")
        base_params["criteria_parameters"] = sql_expr
        input_obj = _ColumnValueAssertionInput(**base_params)

        assert input_obj.criteria_parameters == sql_expr.sql
        assert input_obj.criteria_type == models.AssertionStdParameterTypeClass.SQL

    def test_sql_expression_with_no_parameter_operator_raises_error(
        self, base_params: dict
    ) -> None:
        base_params.update(
            {
                "operator": models.AssertionStdOperatorClass.NULL,
                "criteria_parameters": SqlExpression("SELECT id FROM customers"),
            }
        )
        with pytest.raises(SDKUsageError) as exc_info:
            _ColumnValueAssertionInput(**base_params)
        assert "SQL expressions cannot be used with operator" in str(exc_info.value)

    def test_sql_expression_with_range_operator_raises_error(
        self, base_params: dict
    ) -> None:
        base_params.update(
            {
                "column_name": "number_column",
                "operator": models.AssertionStdOperatorClass.BETWEEN,
                "criteria_parameters": SqlExpression(
                    "SELECT min_val, max_val FROM ranges"
                ),
            }
        )
        with pytest.raises(SDKUsageError) as exc_info:
            _ColumnValueAssertionInput(**base_params)
        assert "SQL expressions cannot be used with range operator" in str(
            exc_info.value
        )

    def test_create_assertion_info_with_sql_expression(self, base_params: dict) -> None:
        sql_expr = SqlExpression("SELECT id FROM valid_customers")
        base_params["criteria_parameters"] = sql_expr
        input_obj = _ColumnValueAssertionInput(**base_params)
        assertion_info = input_obj._create_assertion_info(filter=None)

        assert isinstance(assertion_info, models.FieldAssertionInfoClass)
        assert assertion_info.fieldValuesAssertion is not None
        params = assertion_info.fieldValuesAssertion.parameters
        assert params is not None
        assert params.value is not None
        assert params.value.value == sql_expr.sql
        assert params.value.type == models.AssertionStdParameterTypeClass.SQL
