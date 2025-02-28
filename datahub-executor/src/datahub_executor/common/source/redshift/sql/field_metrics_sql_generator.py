from datahub_executor.common.source.sql.field_metrics_sql_generator import (
    FieldMetricsSQLGenerator,
)


class RedshiftFieldMetricsSQLGenerator(FieldMetricsSQLGenerator):
    source_name: str = "Redshift"

    def _setup_metric_query_median(self, field_path: str, database_string: str) -> str:
        return f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {field_path}) FROM {database_string}"
