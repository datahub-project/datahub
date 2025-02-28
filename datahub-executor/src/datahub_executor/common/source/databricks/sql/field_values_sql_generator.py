from datahub_executor.common.source.sql.field_values_sql_generator import (
    FieldValuesSQLGenerator,
)


class DatabricksFieldValuesSQLGenerator(FieldValuesSQLGenerator):
    source_name: str = "Databricks"

    def _setup_where_clause_regex_match(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"{field_path} NOT RLIKE r'{parameter_value}'"

    def _setup_where_clause_end_with(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"NOT ENDSWITH({field_path}, '{parameter_value}')"

    def _setup_where_clause_start_with(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"NOT STARTSWITH({field_path}, '{parameter_value}')"
