import json
import logging
from typing import List, Optional, Union

from datahub_executor.common.types import (
    AssertionStdOperator,
    AssertionStdParameters,
    FieldTransform,
    FieldTransformType,
    SchemaFieldSpec,
)

from .utils import get_field_value

logger = logging.getLogger(__name__)


class FieldValuesSQLGenerator:
    """
    Set up the SQL query that we use to capture the current number of rows that fail
    our assertion criteria.

    The important thing to note here is that assertion info has always been saved as SUCCESS criteria
    but for this assertion type, we are looking for the number of rows that FAIL, so we have to
    create a SQL query here that is opposite the criteria
    """

    source_name: str

    def _get_transform_string(
        self,
        field: SchemaFieldSpec,
        transform: Optional[FieldTransform],
    ) -> str:
        if transform and transform.type == FieldTransformType.LENGTH:
            return f"LENGTH({field.path})"
        return ""

    def _setup_where_clause_less_than(
        self, field_path: str, field_value: str, transform_string: Optional[str]
    ) -> str:
        return (
            f"{transform_string if transform_string else field_path} >= {field_value}"
        )

    def _setup_where_clause_less_than_or_equal_to(
        self, field_path: str, field_value: str, transform_string: Optional[str]
    ) -> str:
        return f"{transform_string if transform_string else field_path} > {field_value}"

    def _setup_where_clause_greater_than(
        self, field_path: str, field_value: str, transform_string: Optional[str]
    ) -> str:
        return (
            f"{transform_string if transform_string else field_path} <= {field_value}"
        )

    def _setup_where_clause_greater_than_or_equal_to(
        self, field_path: str, field_value: str, transform_string: Optional[str]
    ) -> str:
        return f"{transform_string if transform_string else field_path} < {field_value}"

    def _setup_where_clause_equal_to(self, field_path: str, field_value: str) -> str:
        return f"{field_path} != {field_value}"

    def _setup_where_clause_not_equal_to(
        self, field_path: str, field_value: str
    ) -> str:
        return f"{field_path} = {field_value}"

    def _setup_where_clause_contain(self, field_path: str, parameter_value: str) -> str:
        return f"{field_path} NOT LIKE '%{parameter_value}%'"

    def _setup_where_clause_end_with(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"{field_path} NOT LIKE '%{parameter_value}'"

    def _setup_where_clause_start_with(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"{field_path} NOT LIKE '{parameter_value}%'"

    def _setup_where_clause_regex_match(
        self, field_path: str, parameter_value: str
    ) -> str:
        raise NotImplementedError()

    def _setup_where_clause_in_or_not_in(
        self, operator_value: str, field_path: str, values: List[Union[str, int, float]]
    ) -> str:
        if len(values) == 1:
            if isinstance(values[0], str):
                return f"""CASE
                    WHEN {field_path} {operator_value} ('{values[0]}') THEN 1
                    ELSE 0
                END = 1"""
            else:
                return f"""CASE
                    WHEN {field_path} {operator_value} ({values[0]}) THEN 1
                    ELSE 0
                END = 1"""

        return f"""CASE
                WHEN {field_path} {operator_value} {tuple(values)} THEN 1
                ELSE 0
            END = 1"""

    def _setup_where_clause_null(self, field_path: str) -> str:
        return f"{field_path} IS NOT NULL"

    def _setup_where_clause_not_null(self, field_path: str) -> str:
        return f"{field_path} IS NULL"

    def _setup_where_clause_is_true(self, field_path: str) -> str:
        return f"{field_path} = false"

    def _setup_where_clause_is_false(self, field_path: str) -> str:
        return f"{field_path} = true"

    def _setup_where_clause_between(
        self,
        field_path: str,
        transform_string: Optional[str],
        min_value: str,
        max_value: str,
    ) -> str:
        return f"{transform_string if transform_string else field_path} NOT BETWEEN {min_value} AND {max_value}"

    def _setup_where_clause_single_value(
        self,
        field: SchemaFieldSpec,
        operator: AssertionStdOperator,
        parameters: Optional[AssertionStdParameters],
        transform: Optional[FieldTransform],
    ) -> str:
        assert parameters is not None
        assert parameters.value is not None

        where_clause = ""

        field_value = get_field_value(parameters.value.value, parameters.value.type)
        transform_string = self._get_transform_string(field, transform)

        if operator == AssertionStdOperator.LESS_THAN:
            where_clause = self._setup_where_clause_less_than(
                field.path, field_value, transform_string
            )
        elif operator == AssertionStdOperator.LESS_THAN_OR_EQUAL_TO:
            where_clause = self._setup_where_clause_less_than_or_equal_to(
                field.path, field_value, transform_string
            )
        elif operator == AssertionStdOperator.GREATER_THAN:
            where_clause = self._setup_where_clause_greater_than(
                field.path, field_value, transform_string
            )
        elif operator == AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO:
            where_clause = self._setup_where_clause_greater_than_or_equal_to(
                field.path, field_value, transform_string
            )
        elif operator == AssertionStdOperator.EQUAL_TO:
            where_clause = self._setup_where_clause_equal_to(field.path, field_value)
        elif operator == AssertionStdOperator.NOT_EQUAL_TO:
            where_clause = self._setup_where_clause_not_equal_to(
                field.path, field_value
            )
        elif operator == AssertionStdOperator.CONTAIN:
            where_clause = self._setup_where_clause_contain(
                field.path, parameters.value.value
            )
        elif operator == AssertionStdOperator.END_WITH:
            where_clause = self._setup_where_clause_end_with(
                field.path, parameters.value.value
            )
        elif operator == AssertionStdOperator.START_WITH:
            where_clause = self._setup_where_clause_start_with(
                field.path, parameters.value.value
            )
        elif operator == AssertionStdOperator.REGEX_MATCH:
            where_clause = self._setup_where_clause_regex_match(
                field.path, parameters.value.value
            )
        elif operator in [AssertionStdOperator.IN, AssertionStdOperator.NOT_IN]:
            operator_value = (
                "IN" if operator == AssertionStdOperator.NOT_IN else "NOT IN"
            )
            values = json.loads(parameters.value.value)
            where_clause = self._setup_where_clause_in_or_not_in(
                operator_value, field.path, values
            )

        return where_clause

    def _setup_where_clause_min_and_max_value(
        self,
        field: SchemaFieldSpec,
        operator: AssertionStdOperator,
        parameters: Optional[AssertionStdParameters],
        transform: Optional[FieldTransform],
    ) -> str:
        assert parameters is not None
        assert parameters.min_value is not None
        assert parameters.max_value is not None

        where_clause = ""
        transform_string = self._get_transform_string(field, transform)

        if operator == AssertionStdOperator.BETWEEN:
            where_clause = self._setup_where_clause_between(
                field.path,
                transform_string,
                parameters.min_value.value,
                parameters.max_value.value,
            )

        return where_clause

    def setup_query(
        self,
        database_string: str,
        field: SchemaFieldSpec,
        operator: AssertionStdOperator,
        parameters: Optional[AssertionStdParameters],
        exclude_nulls: bool,
        filter_sql: Optional[str],
        transform: Optional[FieldTransform],
        last_checked: Optional[str],
    ) -> str:
        where_clause = ""

        if operator in [
            AssertionStdOperator.LESS_THAN,
            AssertionStdOperator.LESS_THAN_OR_EQUAL_TO,
            AssertionStdOperator.GREATER_THAN,
            AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO,
            AssertionStdOperator.EQUAL_TO,
            AssertionStdOperator.NOT_EQUAL_TO,
            AssertionStdOperator.CONTAIN,
            AssertionStdOperator.END_WITH,
            AssertionStdOperator.START_WITH,
            AssertionStdOperator.REGEX_MATCH,
            AssertionStdOperator.IN,
            AssertionStdOperator.NOT_IN,
        ]:
            where_clause = self._setup_where_clause_single_value(
                field, operator, parameters, transform
            )

        if operator in [
            AssertionStdOperator.BETWEEN,
        ]:
            where_clause = self._setup_where_clause_min_and_max_value(
                field, operator, parameters, transform
            )

        if operator == AssertionStdOperator.NULL:
            where_clause = self._setup_where_clause_null(field.path)

        if operator == AssertionStdOperator.NOT_NULL:
            where_clause = self._setup_where_clause_not_null(field.path)

        if operator == AssertionStdOperator.IS_TRUE:
            where_clause = self._setup_where_clause_is_true(field.path)

        if operator == AssertionStdOperator.IS_FALSE:
            where_clause = self._setup_where_clause_is_false(field.path)

        # if exclude_nulls = False means we have to add an OR clause to allow these null rows
        if exclude_nulls is False:
            where_string = f"WHERE ({where_clause} OR {field.path} IS NULL)"
        else:
            where_string = f"WHERE {where_clause}"

        if last_checked is not None:
            where_string = f"{where_string} AND {last_checked}"
        if filter_sql is not None:
            where_string = f"{where_string} AND {filter_sql}"

        field_values_query = f"""
            SELECT COUNT(*)
            FROM {database_string}
            {where_string}
        """

        logger.debug(field_values_query)
        return field_values_query
