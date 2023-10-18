from datahub_monitors.source.utils.sql import setup_field_values_query
from datahub_monitors.types import (
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    FieldTransform,
    FieldTransformType,
    SchemaFieldSpec,
)

DATABASE_STRING = "test_db.public.test_table"


class TestSetupFieldValuesQuery:
    def setup_method(self) -> None:
        self.field = SchemaFieldSpec(
            path="test_column",
            type="STRING",
            nativeType="STRING",
        )
        self.value_parameters = AssertionStdParameters(
            value=AssertionStdParameter(
                value="77", type=AssertionStdParameterType.NUMBER
            )
        )
        self.min_max_value_parameters = AssertionStdParameters(
            minValue=AssertionStdParameter(
                value="100", type=AssertionStdParameterType.NUMBER
            ),
            maxValue=AssertionStdParameter(
                value="200", type=AssertionStdParameterType.NUMBER
            ),
        )

    def test_less_than_no_transform(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.LESS_THAN,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column >= 77
        """
        assert query == expected_query

    def test_less_than_with_transform(self) -> None:
        transform = FieldTransform(type=FieldTransformType.LENGTH)
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.LESS_THAN,
            self.value_parameters,
            True,
            None,
            transform,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE LENGTH(test_column) >= 77
        """
        assert query == expected_query

    def test_less_than_or_equal_no_transform(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.LESS_THAN_OR_EQUAL_TO,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column > 77
        """
        assert query == expected_query

    def test_less_than_or_equal_with_transform(self) -> None:
        transform = FieldTransform(type=FieldTransformType.LENGTH)
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.LESS_THAN_OR_EQUAL_TO,
            self.value_parameters,
            True,
            None,
            transform,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE LENGTH(test_column) > 77
        """
        assert query == expected_query

    def test_greater_than_no_transform(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.GREATER_THAN,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column <= 77
        """
        assert query == expected_query

    def test_greater_than_with_transform(self) -> None:
        transform = FieldTransform(type=FieldTransformType.LENGTH)
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.GREATER_THAN,
            self.value_parameters,
            True,
            None,
            transform,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE LENGTH(test_column) <= 77
        """
        assert query == expected_query

    def test_greater_than_or_equal_no_transform(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column < 77
        """
        assert query == expected_query

    def test_greater_than_or_equal_with_transform(self) -> None:
        transform = FieldTransform(type=FieldTransformType.LENGTH)
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO,
            self.value_parameters,
            True,
            None,
            transform,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE LENGTH(test_column) < 77
        """
        assert query == expected_query

    def test_equal_to(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.EQUAL_TO,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column != 77
        """
        assert query == expected_query

    def test_equal_to_with_string(self) -> None:
        self.value_parameters = AssertionStdParameters(
            value=AssertionStdParameter(
                value="test-string", type=AssertionStdParameterType.STRING
            )
        )
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.EQUAL_TO,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column != 'test-string'
        """
        assert query == expected_query

    def test_equal_to_exclude_nulls(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.EQUAL_TO,
            self.value_parameters,
            False,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE (test_column != 77 OR test_column IS NULL)
        """
        assert query == expected_query

    def test_equal_to_with_last_modified(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.EQUAL_TO,
            self.value_parameters,
            True,
            None,
            None,
            "last_modified > 12345678",
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column != 77 AND last_modified > 12345678
        """
        assert query == expected_query

    def test_not_equal_to(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.NOT_EQUAL_TO,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column = 77
        """
        assert query == expected_query

    def test_not_null(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.NOT_NULL,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column IS NULL
        """
        assert query == expected_query

    def test_contains(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.CONTAIN,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column NOT LIKE '%77%'
        """
        assert query == expected_query

    def test_ends_with(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.END_WITH,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column NOT LIKE '%77'
        """
        assert query == expected_query

    def test_ends_with_bigquery(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "BigQuery",
            self.field,
            AssertionStdOperator.END_WITH,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE NOT ENDS_WITH(test_column, '77')
        """
        assert query == expected_query

    def test_starts_with(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.START_WITH,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column NOT LIKE '77%'
        """
        assert query == expected_query

    def test_starts_with_bigquery(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "BigQuery",
            self.field,
            AssertionStdOperator.START_WITH,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE NOT STARTS_WITH(test_column, '77')
        """
        assert query == expected_query

    def test_regex_match_snowflake(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.REGEX_MATCH,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE NOT REGEXP_LIKE(test_column, '77')
        """
        assert query == expected_query

    def test_regex_match_redshift(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Redshift",
            self.field,
            AssertionStdOperator.REGEX_MATCH,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE NOT REGEXP_COUNT(test_column, '77') > 0
        """
        assert query == expected_query

    def test_regex_match_bigquery(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "BigQuery",
            self.field,
            AssertionStdOperator.REGEX_MATCH,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE NOT REGEXP_CONTAINS(test_column, r'77')
        """
        assert query == expected_query

    def test_in_snowflake(self) -> None:
        self.value_parameters = AssertionStdParameters(
            value=AssertionStdParameter(
                value="[77]", type=AssertionStdParameterType.NUMBER
            )
        )
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.IN,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column NOT IN (77)
        """
        assert query == expected_query

    def test_in_snowflake_string(self) -> None:
        self.value_parameters = AssertionStdParameters(
            value=AssertionStdParameter(
                value='["test-string"]', type=AssertionStdParameterType.STRING
            )
        )
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.IN,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column NOT IN ('test-string')
        """
        assert query == expected_query

    def test_in_others(self) -> None:
        self.value_parameters = AssertionStdParameters(
            value=AssertionStdParameter(
                value="[77, 88]", type=AssertionStdParameterType.NUMBER
            )
        )
        query = setup_field_values_query(
            DATABASE_STRING,
            "Redshift",
            self.field,
            AssertionStdOperator.IN,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE CASE
                            WHEN test_column NOT IN (77, 88) THEN 1
                            ELSE 0
                        END = 1
        """
        assert query == expected_query

    def test_not_in_snowflake(self) -> None:
        self.value_parameters = AssertionStdParameters(
            value=AssertionStdParameter(
                value="[77, 88]", type=AssertionStdParameterType.NUMBER
            )
        )
        query = setup_field_values_query(
            DATABASE_STRING,
            "Snowflake",
            self.field,
            AssertionStdOperator.NOT_IN,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column IN (77, 88)
        """
        assert query == expected_query

    def test_not_in_others(self) -> None:
        self.value_parameters = AssertionStdParameters(
            value=AssertionStdParameter(
                value="[77]", type=AssertionStdParameterType.NUMBER
            )
        )
        query = setup_field_values_query(
            DATABASE_STRING,
            "Redshift",
            self.field,
            AssertionStdOperator.NOT_IN,
            self.value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE CASE
                            WHEN test_column IN (77) THEN 1
                            ELSE 0
                        END = 1
        """
        assert query == expected_query

    def test_between(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Redshift",
            self.field,
            AssertionStdOperator.BETWEEN,
            self.min_max_value_parameters,
            True,
            None,
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column NOT BETWEEN 100 AND 200
        """
        assert query == expected_query

    def test_between_with_transform(self) -> None:
        transform = FieldTransform(type=FieldTransformType.LENGTH)
        query = setup_field_values_query(
            DATABASE_STRING,
            "Redshift",
            self.field,
            AssertionStdOperator.BETWEEN,
            self.min_max_value_parameters,
            True,
            None,
            transform,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE LENGTH(test_column) NOT BETWEEN 100 AND 200
        """
        assert query == expected_query

    def test_between_with_filter(self) -> None:
        query = setup_field_values_query(
            DATABASE_STRING,
            "Redshift",
            self.field,
            AssertionStdOperator.BETWEEN,
            self.min_max_value_parameters,
            True,
            "foo = 'bar'",
            None,
            None,
        )
        expected_query = f"""
            SELECT COUNT(*)
            FROM {DATABASE_STRING}
            WHERE test_column NOT BETWEEN 100 AND 200 AND foo = 'bar'
        """
        assert query == expected_query
