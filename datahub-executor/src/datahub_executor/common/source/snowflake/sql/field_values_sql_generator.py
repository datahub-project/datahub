from typing import List, Union

from datahub_executor.common.source.sql.field_values_sql_generator import (
    FieldValuesSQLGenerator,
)


class SnowflakeFieldValuesSQLGenerator(FieldValuesSQLGenerator):
    source_name: str = "Snowflake"

    def _setup_where_clause_regex_match(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"NOT REGEXP_LIKE({field_path}, '{parameter_value}')"

    def _setup_where_clause_in_or_not_in(
        self, operator_value: str, field_path: str, values: List[Union[str, int, float]]
    ) -> str:
        if len(values) == 1:
            if isinstance(values[0], str):
                return f"{field_path} {operator_value} ('{values[0]}')"
            else:
                return f"{field_path} {operator_value} ({values[0]})"

        return f"{field_path} {operator_value} {tuple(values)}"
