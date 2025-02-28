from datahub_executor.common.source.sql.field_values_sql_generator import (
    FieldValuesSQLGenerator,
)


class BigQueryFieldValuesSQLGenerator(FieldValuesSQLGenerator):
    source_name: str = "BigQuery"

    def _setup_where_clause_end_with(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"NOT ENDS_WITH({field_path}, '{parameter_value}')"

    def _setup_where_clause_start_with(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"NOT STARTS_WITH({field_path}, '{parameter_value}')"

    def _setup_where_clause_regex_match(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"NOT REGEXP_CONTAINS({field_path}, r'{parameter_value}')"
