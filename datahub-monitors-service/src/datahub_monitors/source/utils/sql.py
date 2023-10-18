import json
import logging
from typing import Optional

from datahub_monitors.types import (
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionStdParameterType,
    FieldTransform,
    FieldTransformType,
    SchemaFieldSpec,
)

logger = logging.getLogger(__name__)


def setup_high_watermark_field_value_query(
    column_name: str,
    database_string: str,
    filter_sql: str,
    previous_value: Optional[str],
) -> str:
    filter_sql_part = ""
    if filter_sql:
        if previous_value:
            filter_sql_part = f"AND {filter_sql}"
        else:
            filter_sql_part = f"WHERE {filter_sql}"

    get_value_query = f"""
        SELECT {column_name}
        FROM {database_string}
        {f"WHERE {column_name} >= {previous_value}" if previous_value else ''}
        {filter_sql_part}
        ORDER by {column_name} DESC
        LIMIT 1;
    """
    logger.debug(get_value_query)

    return get_value_query


def setup_high_watermark_row_count_query(
    column_name: str,
    database_string: str,
    filter_sql: str,
    current_field_value: str,
) -> str:
    get_count_query = f"""
        SELECT COUNT(*)
        FROM {database_string}
        WHERE {column_name} = {current_field_value}
        {f"AND {filter_sql}" if filter_sql else ''}
    """
    logger.debug(get_count_query)
    return get_count_query


def setup_row_count_query(
    database_string: str,
    filter_sql: str,
) -> str:
    get_count_query = f"""
        SELECT COUNT(*)
        FROM {database_string}
        {f"WHERE {filter_sql}" if filter_sql else ''}
    """
    logger.debug(get_count_query)
    return get_count_query


def setup_field_values_query(
    database_string: str,
    source_name: str,
    field: SchemaFieldSpec,
    operator: AssertionStdOperator,
    parameters: Optional[AssertionStdParameters],
    exclude_nulls: bool,
    filter_sql: Optional[str],
    transform: Optional[FieldTransform],
    last_checked: Optional[str],
) -> str:
    """
    Set up the SQL query that we use to capture the current number of rows that fail
    our assertion criteria.

    The important thing to note here is that assertion info has always been saved as SUCCESS criteria
    but for this assertion type, we are looking for the number of rows that FAIL, so we have to
    create a SQL query here that is opposite the criteria
    """

    where_clause = ""
    transform_string = (
        f"LENGTH({field.path})"
        if transform and transform.type == FieldTransformType.LENGTH
        else ""
    )

    if parameters and parameters.value:
        field_value = (
            parameters.value.value
            if parameters.value.type == AssertionStdParameterType.NUMBER
            else f"'{parameters.value.value}'"
        )

        if operator == AssertionStdOperator.LESS_THAN:
            where_clause = f"{transform_string if transform_string else field.path} >= {field_value}"
        elif operator == AssertionStdOperator.LESS_THAN_OR_EQUAL_TO:
            where_clause = f"{transform_string if transform_string else field.path} > {field_value}"
        elif operator == AssertionStdOperator.GREATER_THAN:
            where_clause = f"{transform_string if transform_string else field.path} <= {field_value}"
        elif operator == AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO:
            where_clause = f"{transform_string if transform_string else field.path} < {field_value}"
        elif operator == AssertionStdOperator.EQUAL_TO:
            where_clause = f"{field.path} != {field_value}"
        elif operator == AssertionStdOperator.NOT_EQUAL_TO:
            where_clause = f"{field.path} = {field_value}"
        elif operator == AssertionStdOperator.CONTAIN:
            where_clause = f"{field.path} NOT LIKE '%{parameters.value.value}%'"
        elif operator == AssertionStdOperator.END_WITH:
            if source_name == "BigQuery":
                where_clause = (
                    f"NOT ENDS_WITH({field.path}, '{parameters.value.value}')"
                )
            else:
                where_clause = f"{field.path} NOT LIKE '%{parameters.value.value}'"
        elif operator == AssertionStdOperator.START_WITH:
            if source_name == "BigQuery":
                where_clause = (
                    f"NOT STARTS_WITH({field.path}, '{parameters.value.value}')"
                )
            else:
                where_clause = f"{field.path} NOT LIKE '{parameters.value.value}%'"
        elif operator == AssertionStdOperator.REGEX_MATCH:
            if source_name == "Snowflake":
                where_clause = (
                    f"NOT REGEXP_LIKE({field.path}, '{parameters.value.value}')"
                )
            elif source_name == "Redshift":
                where_clause = (
                    f"NOT REGEXP_COUNT({field.path}, '{parameters.value.value}') > 0"
                )
            else:
                where_clause = (
                    f"NOT REGEXP_CONTAINS({field.path}, r'{parameters.value.value}')"
                )
        elif operator in [AssertionStdOperator.IN, AssertionStdOperator.NOT_IN]:
            operator_value = (
                "IN" if operator == AssertionStdOperator.NOT_IN else "NOT IN"
            )
            values = json.loads(parameters.value.value)
            if source_name == "Snowflake":
                if len(values) == 1:
                    if isinstance(values[0], str):
                        where_clause = f"{field.path} {operator_value} ('{values[0]}')"
                    else:
                        where_clause = f"{field.path} {operator_value} ({values[0]})"
                else:
                    where_clause = f"{field.path} {operator_value} {tuple(values)}"
            else:
                if len(values) == 1:
                    if isinstance(values[0], str):
                        where_clause = f"""CASE
                            WHEN {field.path} {operator_value} ('{values[0]}') THEN 1
                            ELSE 0
                        END = 1"""
                    else:
                        where_clause = f"""CASE
                            WHEN {field.path} {operator_value} ({values[0]}) THEN 1
                            ELSE 0
                        END = 1"""
                else:
                    where_clause = f"""CASE
                            WHEN {field.path} {operator_value} {tuple(values)} THEN 1
                            ELSE 0
                        END = 1"""

    if operator == AssertionStdOperator.BETWEEN:
        if parameters and parameters.min_value and parameters.max_value:
            where_clause = f"{transform_string if transform_string else field.path} NOT BETWEEN {parameters.min_value.value} AND {parameters.max_value.value}"

    if operator == AssertionStdOperator.NULL:
        where_clause = f"{field.path} IS NOT NULL"

    if operator == AssertionStdOperator.NOT_NULL:
        where_clause = f"{field.path} IS NULL"

    if operator == AssertionStdOperator.IS_TRUE:
        where_clause = f"{field.path} = false"

    if operator == AssertionStdOperator.IS_FALSE:
        where_clause = f"{field.path} = true"

    # if exclude_nulls = False means we have to add an OR clause to allow these null rows
    if exclude_nulls is False:
        where_string = f"WHERE ({where_clause} OR {field.path} IS NULL)"
    else:
        where_string = f"WHERE {where_clause}"

    # the last_checked filter is what we use to support the "high watermark" filtering for this assertion type
    if last_checked is not None:
        where_string = f"{where_string} AND {last_checked}"

    # allow for an optional user supplied filter to narrow down this query even more.
    if filter_sql is not None:
        where_string = f"{where_string} AND {filter_sql}"

    field_values_query = f"""
            SELECT COUNT(*)
            FROM {database_string}
            {where_string}
        """

    logger.debug(field_values_query)
    return field_values_query
