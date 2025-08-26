from datahub_executor.common.source.sql.field_metrics_sql_generator import (
    FieldMetricsSQLGenerator,
)


class BigQueryFieldMetricsSQLGenerator(FieldMetricsSQLGenerator):
    source_name: str = "BigQuery"

    def _setup_metric_query_median(self, field_path: str, database_string: str) -> str:
        return f"SELECT APPROX_QUANTILES({field_path}, 2)[OFFSET(1)] FROM {database_string}"
