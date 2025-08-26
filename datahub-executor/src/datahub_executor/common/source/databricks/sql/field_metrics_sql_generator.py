from datahub_executor.common.source.sql.field_metrics_sql_generator import (
    FieldMetricsSQLGenerator,
)


class DatabricksFieldMetricsSQLGenerator(FieldMetricsSQLGenerator):
    source_name: str = "Databricks"

    def _setup_metric_query_median(self, field_path: str, database_string: str) -> str:
        return (
            f"SELECT PERCENTILE_APPROX({field_path}, 0.5, 100) FROM {database_string}"
        )
