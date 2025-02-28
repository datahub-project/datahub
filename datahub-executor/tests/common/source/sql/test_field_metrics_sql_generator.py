from datahub_executor.common.source.bigquery.sql.field_metrics_sql_generator import (
    BigQueryFieldMetricsSQLGenerator,
)
from datahub_executor.common.source.databricks.sql.field_metrics_sql_generator import (
    DatabricksFieldMetricsSQLGenerator,
)
from datahub_executor.common.source.redshift.sql.field_metrics_sql_generator import (
    RedshiftFieldMetricsSQLGenerator,
)
from datahub_executor.common.source.snowflake.sql.field_metrics_sql_generator import (
    SnowflakeFieldMetricsSQLGenerator,
)
from datahub_executor.common.types import FieldMetricType, SchemaFieldSpec

DATABASE_STRING = "test_db.public.test_table"


class TestFieldMetricsSQLGenerator:
    def setup_method(self) -> None:
        self.bigquery_sql_generator = BigQueryFieldMetricsSQLGenerator()
        self.redshift_sql_generator = RedshiftFieldMetricsSQLGenerator()
        self.snowflake_sql_generator = SnowflakeFieldMetricsSQLGenerator()
        self.databricks_sql_generator = DatabricksFieldMetricsSQLGenerator()

        self.field = SchemaFieldSpec(
            path="test_column",
            type="STRING",
            nativeType="STRING",
        )

    def test_unique_count(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.UNIQUE_COUNT,
            None,
            None,
        )
        expected_query = (
            f"SELECT COUNT(DISTINCT {self.field.path}) FROM {DATABASE_STRING}"
        )
        assert query == expected_query

    def test_unique_count_with_filter(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.UNIQUE_COUNT,
            "foo = 'bar'",
            None,
        )
        expected_query = f"SELECT COUNT(DISTINCT {self.field.path}) FROM {DATABASE_STRING} WHERE foo = 'bar'"
        assert query == expected_query

    def test_unique_count_with_last_checked(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.UNIQUE_COUNT,
            None,
            "last_modified > 123456789",
        )
        expected_query = f"SELECT COUNT(DISTINCT {self.field.path}) FROM {DATABASE_STRING} WHERE last_modified > 123456789"
        assert query == expected_query

    def test_unique_count_with_filter_and_last_checked(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.UNIQUE_COUNT,
            "foo = 'bar'",
            "last_modified > 123456789",
        )
        expected_query = f"SELECT COUNT(DISTINCT {self.field.path}) FROM {DATABASE_STRING} WHERE foo = 'bar' AND last_modified > 123456789"
        assert query == expected_query

    def test_unique_percentage(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.UNIQUE_PERCENTAGE,
            None,
            None,
        )
        expected_query = f"SELECT COUNT(DISTINCT {self.field.path}) * 100.0 / COUNT(*) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_unique_percentage_with_filter_and_last_checked(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.UNIQUE_PERCENTAGE,
            None,
            None,
        )
        expected_query = f"SELECT COUNT(DISTINCT {self.field.path}) * 100.0 / COUNT(*) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_null_count(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.NULL_COUNT,
            None,
            None,
        )
        expected_query = (
            f"SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} IS NULL"
        )
        assert query == expected_query

    def test_null_percentage(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.NULL_PERCENTAGE,
            None,
            None,
        )
        expected_query = f"SELECT (SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} IS NULL) * 100.0 / COUNT(*) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_null_percentage_with_filter_and_last_checked(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.NULL_PERCENTAGE,
            "foo = 'bar'",
            "last_modified > 123456789",
        )
        expected_query = f"SELECT (SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} IS NULL AND foo = 'bar' AND last_modified > 123456789) * 100.0 / COUNT(*) FROM {DATABASE_STRING} WHERE foo = 'bar' AND last_modified > 123456789"
        assert query == expected_query

    def test_min(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.MIN,
            None,
            None,
        )
        expected_query = f"SELECT MIN({self.field.path}) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_max(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.MAX,
            None,
            None,
        )
        expected_query = f"SELECT MAX({self.field.path}) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_mean(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.MEAN,
            None,
            None,
        )
        expected_query = f"SELECT AVG({self.field.path}) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_median_snowflake(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.MEDIAN,
            None,
            None,
        )
        expected_query = f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {self.field.path}) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_median_bigquery(self) -> None:
        query = self.bigquery_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.MEDIAN,
            None,
            None,
        )
        expected_query = f"SELECT APPROX_QUANTILES({self.field.path}, 2)[OFFSET(1)] FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_median_redshift(self) -> None:
        query = self.redshift_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.MEDIAN,
            None,
            None,
        )
        expected_query = f"SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {self.field.path}) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_median_databricks(self) -> None:
        query = self.databricks_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.MEDIAN,
            None,
            None,
        )
        expected_query = f"SELECT PERCENTILE_APPROX({self.field.path}, 0.5, 100) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_stddev(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.STDDEV,
            None,
            None,
        )
        expected_query = f"SELECT STDDEV({self.field.path}) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_negative_count(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.NEGATIVE_COUNT,
            None,
            None,
        )
        expected_query = (
            f"SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} < 0"
        )
        assert query == expected_query

    def test_negative_percentage(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.NEGATIVE_PERCENTAGE,
            None,
            None,
        )
        expected_query = f"SELECT (SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} < 0) * 100.0 / COUNT(*) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_negative_percentage_with_filter_and_last_checked(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.NEGATIVE_PERCENTAGE,
            "foo = 'bar'",
            "last_modified > 123456789",
        )
        expected_query = f"SELECT (SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} < 0 AND foo = 'bar' AND last_modified > 123456789) * 100.0 / COUNT(*) FROM {DATABASE_STRING} WHERE foo = 'bar' AND last_modified > 123456789"
        assert query == expected_query

    def test_zero_count(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.ZERO_COUNT,
            None,
            None,
        )
        expected_query = (
            f"SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} = 0"
        )
        assert query == expected_query

    def test_zero_percentage(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.ZERO_PERCENTAGE,
            None,
            None,
        )
        expected_query = f"SELECT (SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} = 0) * 100.0 / COUNT(*) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_zero_percentage_with_filter_and_last_checked(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.ZERO_PERCENTAGE,
            "foo = 'bar'",
            "last_modified > 123456789",
        )
        expected_query = f"SELECT (SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} = 0 AND foo = 'bar' AND last_modified > 123456789) * 100.0 / COUNT(*) FROM {DATABASE_STRING} WHERE foo = 'bar' AND last_modified > 123456789"
        assert query == expected_query

    def test_min_length(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.MIN_LENGTH,
            None,
            None,
        )
        expected_query = f"SELECT MIN(LENGTH({self.field.path})) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_max_length(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.MAX_LENGTH,
            None,
            None,
        )
        expected_query = f"SELECT MAX(LENGTH({self.field.path})) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_empty_count(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.EMPTY_COUNT,
            None,
            None,
        )
        expected_query = f"SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} IS NOT NULL AND TRIM({self.field.path}) = ''"
        assert query == expected_query

    def test_empty_count_with_filter_and_last_checked(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.EMPTY_COUNT,
            "foo = 'bar'",
            "last_modified > 123456789",
        )
        expected_query = f"SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} IS NOT NULL AND TRIM({self.field.path}) = '' AND foo = 'bar' AND last_modified > 123456789"
        assert query == expected_query

    def test_empty_percentage(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.EMPTY_PERCENTAGE,
            None,
            None,
        )
        expected_query = f"SELECT (SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} IS NOT NULL AND TRIM({self.field.path}) = '') * 100.0 / COUNT(*) FROM {DATABASE_STRING}"
        assert query == expected_query

    def test_empty_percentage_with_filter_and_last_checked(self) -> None:
        query = self.snowflake_sql_generator.setup_query(
            DATABASE_STRING,
            self.field,
            FieldMetricType.EMPTY_PERCENTAGE,
            "foo = 'bar'",
            "last_modified > 123456789",
        )
        expected_query = f"SELECT (SELECT COUNT(*) FROM {DATABASE_STRING} WHERE {self.field.path} IS NOT NULL AND TRIM({self.field.path}) = '' AND foo = 'bar' AND last_modified > 123456789) * 100.0 / COUNT(*) FROM {DATABASE_STRING} WHERE foo = 'bar' AND last_modified > 123456789"
        assert query == expected_query
