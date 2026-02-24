from typing import Any, List, Optional, Union

from datahub_executor.common.assertion.types import AssertionDatabaseParams
from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.source.source import Source
from datahub_executor.common.source.types import DatabaseParams, SourceOperationParams
from datahub_executor.common.types import (
    DatasetFilter,
    DatasetFilterType,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    EntityEvent,
    FieldMetricType,
    SchemaFieldSpec,
)


class _FakeSource(Source):
    def __init__(self) -> None:
        super().__init__(Connection("conn", "platform"))
        self.source_name = "Fake"
        self.last_filter_sql: Optional[str] = None

    def _execute_fetchall_query_internal(self, query: str) -> List[Any]:
        return []

    def _get_database_string(
        self, params: Union[DatabaseParams, SourceOperationParams]
    ) -> str:
        return "db.schema.table"

    def _convert_value_for_comparison(self, column_value: str, column_type: str) -> str:
        return column_value

    def _get_audit_log_operation_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        return []

    def _get_dataset_last_updated_events(
        self, operation_params: SourceOperationParams
    ) -> List[EntityEvent]:
        return []

    def _get_field_last_updated_events(
        self, operation_params: SourceOperationParams, parameters: dict
    ) -> List[EntityEvent]:
        return []

    def _get_num_rows_via_stats_table(
        self, database_params: DatabaseParams
    ) -> Optional[int]:
        return 1

    def _get_num_rows_via_count(
        self, database_params: DatabaseParams, filter_sql: str
    ) -> Optional[int]:
        self.last_filter_sql = filter_sql
        return 42

    def _get_single_value_from_custom_sql(self, custom_sql: str) -> Union[int, float]:
        return 1

    def _get_supported_high_watermark_column_types(self) -> List[str]:
        return []

    def _get_supported_high_watermark_date_and_time_types(self) -> List[str]:
        return []

    def _get_high_watermark_field_value(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        previous_value: Optional[str],
    ) -> Optional[str]:
        return None

    def _get_high_watermark_row_count(
        self,
        column_name: str,
        column_type: str,
        operation_params: SourceOperationParams,
        filter_sql: str,
        current_field_value: str,
    ) -> Optional[int]:
        return None

    def _build_tz_bucket_boundary_expression(
        self, expression: str, normalized_interval: str, timezone_name: str
    ) -> str:
        return expression

    def _convert_millis_to_bucket_timestamp(self, millis: int) -> str:
        return str(millis)

    def get_field_metric_value(
        self,
        entity_urn: str,
        field: SchemaFieldSpec,
        metric: FieldMetricType,
        database_parameters: AssertionDatabaseParams,
        filter: Optional[DatasetFilter],
        prev_high_watermark_value: Optional[str],
        changed_rows_field: Optional[SchemaFieldSpec],
        runtime_parameters: Optional[dict[str, Any]] = None,
    ) -> float:
        return 0.0


def test_bucket_filter_is_appended_to_existing_filter() -> None:
    source = _FakeSource()
    result = source.get_row_count(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.b.c,PROD)",
        AssertionDatabaseParams(qualified_name="a.b.c", table_name="c"),
        DatasetVolumeAssertionParameters(source_type=DatasetVolumeSourceType.QUERY),
        {
            "type": DatasetFilterType.SQL,
            "sql": "id > 0",
            "bucket": {
                "timestamp_field_path": "event_ts",
                "bucket_interval_unit": "DAY",
                "bucket_start_time_ms": 1000,
                "bucket_end_time_ms": 2000,
                "timezone": "UTC",
            },
        },
    )

    assert result == 42
    assert source.last_filter_sql is not None
    assert "(id > 0)" in source.last_filter_sql
    assert "event_ts >= 1000 AND event_ts < 2000" in source.last_filter_sql


def test_bucket_filter_is_only_filter_when_no_existing_filter() -> None:
    source = _FakeSource()
    result = source.get_row_count(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.b.c,PROD)",
        AssertionDatabaseParams(qualified_name="a.b.c", table_name="c"),
        DatasetVolumeAssertionParameters(source_type=DatasetVolumeSourceType.QUERY),
        {
            "bucket": {
                "timestamp_field_path": "event_ts",
                "bucket_interval_unit": "DAY",
                "bucket_start_time_ms": 1000,
                "bucket_end_time_ms": 2000,
                "timezone": "UTC",
            },
        },
    )

    assert result == 42
    assert source.last_filter_sql == "event_ts >= 1000 AND event_ts < 2000"
