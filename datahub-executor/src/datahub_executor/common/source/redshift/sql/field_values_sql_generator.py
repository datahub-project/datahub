from datahub_executor.common.source.sql.field_values_sql_generator import (
    FieldValuesSQLGenerator,
)


class RedshiftFieldValuesSQLGenerator(FieldValuesSQLGenerator):
    source_name: str = "Redshift"

    def _setup_where_clause_regex_match(
        self, field_path: str, parameter_value: str
    ) -> str:
        return f"NOT REGEXP_COUNT({field_path}, '{parameter_value}') > 0"
