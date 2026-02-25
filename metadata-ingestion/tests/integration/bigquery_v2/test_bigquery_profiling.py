from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import patch

import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import BigqueryTable
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.discovery import (
    PartitionDiscovery,
)
from datahub.ingestion.source.bigquery_v2.profiling.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.profiling.query_executor import QueryExecutor
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    validate_and_filter_expressions,
    validate_bigquery_identifier,
)


def make_config(**profiling_overrides: Any) -> BigQueryV2Config:
    return BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project-123456",
            "profiling": {"enabled": True, **profiling_overrides},
        }
    )


def make_table(
    name: str = "test_table",
    rows_count: Optional[int] = 10000,
    external: bool = False,
    max_partition_id: Optional[str] = None,
    **kwargs: Any,
) -> BigqueryTable:
    now = datetime.now(timezone.utc)
    return BigqueryTable(
        name=name,
        comment="",
        rows_count=rows_count,
        size_in_bytes=1_000_000 if rows_count else None,
        last_altered=now - timedelta(days=1),
        created=now - timedelta(days=30),
        external=external,
        max_partition_id=max_partition_id,
        **kwargs,
    )


def test_profiling_modules_integration():
    config = make_config(
        profile_external_tables=True,
        partition_profiling_enabled=True,
        use_sampling=True,
        sample_size=10000,
        profiling_row_limit=50000,
    )
    report = BigQueryV2Report()
    profiler = BigqueryProfiler(config, report)

    assert isinstance(profiler.partition_discovery, PartitionDiscovery)
    assert isinstance(profiler.query_executor, QueryExecutor)

    dates = profiler.partition_discovery._get_strategic_candidate_dates()
    assert len(dates) == 2
    assert dates[0][0] >= dates[1][0]

    assert profiler.query_executor.get_effective_timeout() > 0

    assert validate_bigquery_identifier("test_table") == "`test_table`"
    filters = ["`date` = '2023-01-01'", "`id` > 100"]
    assert validate_and_filter_expressions(filters) == filters


def test_profiler_staleness_check():
    config = make_config(skip_stale_tables=True, staleness_threshold_days=30)
    profiler = BigqueryProfiler(config, BigQueryV2Report())

    now = datetime.now(timezone.utc)
    fresh = BigqueryTable(
        name="fresh",
        comment="",
        rows_count=1000,
        size_in_bytes=1_000_000,
        last_altered=now - timedelta(hours=1),
        created=now - timedelta(days=1),
    )
    assert profiler._should_skip_profiling_due_to_staleness(fresh) is False

    stale = BigqueryTable(
        name="stale",
        comment="",
        rows_count=1000,
        size_in_bytes=1_000_000,
        last_altered=now - timedelta(days=60),
        created=now - timedelta(days=90),
    )
    assert profiler._should_skip_profiling_due_to_staleness(stale) is True


def test_batch_kwargs_sampling_threshold():
    """Below sample_size → no TABLESAMPLE; above sample_size → TABLESAMPLE added."""
    config = make_config(use_sampling=True, sample_size=1000, profiling_row_limit=10000)
    profiler = BigqueryProfiler(config, BigQueryV2Report())

    with patch.object(
        profiler.partition_discovery,
        "get_required_partition_filters",
        return_value=["`date` = '2023-12-25'"],
    ):
        small = profiler.get_batch_kwargs(
            make_table(rows_count=500), "test_dataset", "test-project-123456"
        )
        assert "TABLESAMPLE" not in small["custom_sql"]
        assert "LIMIT" in small["custom_sql"]

        large = profiler.get_batch_kwargs(
            make_table(rows_count=50_000), "test_dataset", "test-project-123456"
        )
        assert "TABLESAMPLE SYSTEM" in large["custom_sql"]


def test_batch_kwargs_rejects_invalid_identifier():
    config = make_config()
    profiler = BigqueryProfiler(config, BigQueryV2Report())

    with patch.object(
        profiler.partition_discovery, "get_required_partition_filters", return_value=[]
    ):
        profiler.get_batch_kwargs(
            make_table(), "valid_dataset", "valid-project-123"
        )  # no raise

        with pytest.raises(ValueError, match="Invalid dataset identifier"):
            profiler.get_batch_kwargs(
                make_table(), "invalid;dataset", "valid-project-123"
            )


def test_partition_discovery_strategic_dates():
    discovery = PartitionDiscovery(make_config())
    dates = discovery._get_strategic_candidate_dates()

    assert len(dates) == 2
    assert dates[0][0] >= dates[1][0]
    descriptions = [d for _, d in dates]
    assert any("today" in d.lower() for d in descriptions)
    assert any("yesterday" in d.lower() for d in descriptions)


def test_query_executor_sql_building():
    executor = QueryExecutor(make_config())

    assert (
        executor.build_safe_custom_sql("test-project-123", "dataset", "table")
        == "SELECT * FROM `test-project-123`.`dataset`.`table`"
    )
    assert (
        executor.build_safe_custom_sql(
            "test-project-123", "dataset", "table", where_clause="`date` = '2023-01-01'"
        )
        == "SELECT * FROM `test-project-123`.`dataset`.`table` WHERE `date` = '2023-01-01'"
    )
    assert (
        executor.build_safe_custom_sql(
            "test-project-123", "dataset", "table", limit=1000
        )
        == "SELECT * FROM `test-project-123`.`dataset`.`table` LIMIT 1000"
    )


def test_partition_filters_from_max_partition_id():
    """max_partition_id on the table object is the cheapest discovery path (no queries needed).

    Verifies that a valid YYYYMMDD max_partition_id is converted directly to a DATE filter,
    and that sentinel values (__NULL__, __UNPARTITIONED__) are treated as "no partition info".
    """
    discovery = PartitionDiscovery(make_config())

    filters = discovery._get_partition_filters_from_max_partition_id(
        make_table(max_partition_id="20241115"),
        required_columns=["event_date"],
        column_types={"event_date": "DATE"},
    )
    assert filters is not None and len(filters) == 1
    assert "2024-11-15" in filters[0]

    for sentinel in ("__NULL__", "__UNPARTITIONED__", "__STREAMING_UNPARTITIONED__"):
        assert (
            discovery._get_partition_filters_from_max_partition_id(
                make_table(max_partition_id=sentinel),
                required_columns=["event_date"],
                column_types={"event_date": "DATE"},
            )
            is None
        ), f"Expected None for sentinel {sentinel!r}"


def test_max_partition_id_used_before_information_schema():
    """When max_partition_id is present it should short-circuit INFORMATION_SCHEMA queries."""
    discovery = PartitionDiscovery(make_config())

    query_calls: list = []

    def tracking_execute(query: str, job_config: Any, context: str) -> list:
        query_calls.append(query)
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return [SimpleNamespace(column_name="run_date", data_type="DATE")]
        return []

    table = make_table(max_partition_id="20241201")
    table.partition_info = SimpleNamespace(  # type: ignore[assignment]
        type_="DAY",
        field="run_date",
        fields=["run_date"],
        require_partition_filter=True,
    )

    filters = discovery.get_required_partition_filters(
        table, "proj", "ds", tracking_execute
    )

    assert filters is not None and len(filters) == 1
    assert "2024-12-01" in filters[0]
    assert not any("INFORMATION_SCHEMA.PARTITIONS" in q for q in query_calls)


def test_partition_detection_via_query_error():
    """When INFORMATION_SCHEMA returns nothing and the table has require_partition_filter,
    BigQuery raises an error whose text names the required columns.  The profiler extracts
    those column names from the error message and continues discovery.

    Real BigQuery error format: "filter over column(s) 'event_date'"  (column in quotes).
    """
    discovery = PartitionDiscovery(make_config())

    bq_error = (
        "Error 400: Cannot query over table 'my-project.ds.t' without a filter "
        "that can be used for partition elimination. "
        "Required filter over column(s) 'event_date'."
    )

    def execute(query: str, job_config: Any, context: str) -> list:
        if "INFORMATION_SCHEMA" in query:
            return []
        if "COUNT(*)" in query and "LIMIT" in query:
            raise Exception(bq_error)
        if "GROUP BY" in query:
            return [SimpleNamespace(val=date(2024, 11, 20), record_count=5000)]
        return []

    filters = discovery.get_required_partition_filters(
        make_table(name="t"), "my-project", "ds", execute
    )

    assert filters is not None and len(filters) > 0
    assert "event_date" in " ".join(filters)


def test_unpartitioned_table_returns_empty_list():
    """An unpartitioned table has no require_partition_filter; probe query succeeds and
    get_required_partition_filters should return [] (empty, not None).
    """
    discovery = PartitionDiscovery(make_config())

    def execute(query: str, job_config: Any, context: str) -> list:
        return [SimpleNamespace(cnt=42)]

    filters = discovery.get_required_partition_filters(
        make_table(name="unpartitioned"), "proj", "ds", execute
    )
    assert filters == []


def test_all_value_queries_fail_returns_is_not_null_fallback():
    """When the partition column is known (from the BigQuery error) but every query for
    specific values fails, the function returns IS NOT NULL as a last-resort fallback
    rather than None.  IS NOT NULL lets profiling proceed; a None would skip it entirely.
    """
    discovery = PartitionDiscovery(make_config())

    partition_err = (
        "Cannot query over table 'my-project.ds.t' without a filter. "
        "Required filter over column(s) 'event_date'."
    )

    def execute(query: str, job_config: Any, context: str) -> list:
        if "INFORMATION_SCHEMA" in query:
            return []
        if "COUNT(*)" in query:
            raise Exception(partition_err)
        return []

    filters = discovery.get_required_partition_filters(
        make_table(name="t"), "my-project", "ds", execute
    )

    assert filters is not None and len(filters) == 1
    assert "event_date" in filters[0]
    assert "IS NOT NULL" in filters[0]


def test_non_date_partition_columns_find_most_frequent_value():
    """For tables partitioned by a non-date INT64 or STRING column (region_id, feed, etc.)
    get_required_partition_filters should return a concrete equality filter rather than
    a useless IS NOT NULL placeholder.
    """
    discovery = PartitionDiscovery(make_config())

    def execute(query: str, job_config: Any, context: str) -> list:
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return [SimpleNamespace(column_name="region_id", data_type="INT64")]
        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            return []
        if "GROUP BY" in query and "region_id" in query:
            return [SimpleNamespace(val=42, record_count=9000)]
        return []

    table = make_table(name="region_partitioned")
    table.partition_info = SimpleNamespace(  # type: ignore[assignment]
        type_="RANGE",
        field="region_id",
        fields=["region_id"],
        require_partition_filter=True,
    )

    filters = discovery.get_required_partition_filters(
        table, "my-project", "ds", execute
    )

    assert filters is not None and len(filters) == 1
    assert "IS NOT NULL" not in filters[0]
    assert "region_id" in filters[0]
    assert "42" in filters[0]


def test_compound_partition_date_plus_string():
    """A compound partition (DATE + STRING) should yield one filter per column.
    The date filter is a real date value; the string filter is the most-common value
    found in the data scoped to that date.
    """
    discovery = PartitionDiscovery(make_config())

    def execute(query: str, job_config: Any, context: str) -> list:
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return [
                SimpleNamespace(column_name="event_date", data_type="DATE"),
                SimpleNamespace(column_name="feed", data_type="STRING"),
            ]
        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            return []
        if "GROUP BY" in query and "event_date" in query and "feed" not in query:
            return [SimpleNamespace(val=date(2024, 11, 20), record_count=5000)]
        if "GROUP BY" in query and "feed" in query:
            return [SimpleNamespace(val="pp_tse", record_count=3000)]
        if "SELECT 1" in query:
            return [SimpleNamespace(cnt=1)]
        return []

    table = make_table(name="compound_partitioned")
    table.partition_info = SimpleNamespace(  # type: ignore[assignment]
        type_="DAY",
        field="event_date",
        fields=["event_date", "feed"],
        require_partition_filter=True,
    )

    filters = discovery.get_required_partition_filters(
        table, "my-project", "ds", execute
    )

    assert filters is not None and len(filters) == 2
    filter_str = " ".join(filters)
    assert "2024-11-20" in filter_str
    assert "pp_tse" in filter_str


def test_compound_partition_non_date_query_failure_still_returns_date_filter():
    """When the non-date column query fails during compound partition discovery, the
    date filter should still be returned so profiling can proceed (with a less
    targeted scan for the string column rather than crashing entirely).
    """
    discovery = PartitionDiscovery(make_config())

    def execute(query: str, job_config: Any, context: str) -> list:
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return [
                SimpleNamespace(column_name="event_date", data_type="DATE"),
                SimpleNamespace(column_name="feed", data_type="STRING"),
            ]
        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            return []
        if "GROUP BY" in query and "event_date" in query and "feed" not in query:
            return [SimpleNamespace(val=date(2024, 11, 20), record_count=5000)]
        if "GROUP BY" in query and "feed" in query:
            raise RuntimeError("Simulated BigQuery error for non-date column")
        if "SELECT 1" in query:
            return [SimpleNamespace(cnt=1)]
        return []

    table = make_table(name="compound_partitioned_partial_failure")
    table.partition_info = SimpleNamespace(  # type: ignore[assignment]
        type_="DAY",
        field="event_date",
        fields=["event_date", "feed"],
        require_partition_filter=True,
    )

    filters = discovery.get_required_partition_filters(
        table, "my-project", "ds", execute
    )

    # Should not crash; date filter must be present even if feed filter couldn't be built
    assert filters is not None
    assert any("event_date" in f or "2024-11-20" in f for f in filters)


def test_batch_kwargs_safety_limit_for_large_unsampled_table():
    """Without sampling enabled or an explicit row limit, a table with >1M rows should
    get a 100k safety LIMIT in the custom_sql to avoid accidentally full-scanning it.
    """
    config = make_config(use_sampling=False, profiling_row_limit=0)
    profiler = BigqueryProfiler(config, BigQueryV2Report())

    with patch.object(
        profiler.partition_discovery, "get_required_partition_filters", return_value=[]
    ):
        kwargs = profiler.get_batch_kwargs(
            make_table(name="big_table", rows_count=2_000_000),
            "ds",
            "test-project-123456",
        )

    assert "LIMIT 100000" in kwargs["custom_sql"]
    assert "TABLESAMPLE" not in kwargs["custom_sql"]


def test_batch_kwargs_sampling_with_partition_filter():
    """When both sampling and a partition filter are active the custom_sql should contain
    both TABLESAMPLE and WHERE.
    """
    config = make_config(use_sampling=True, sample_size=5000)
    profiler = BigqueryProfiler(config, BigQueryV2Report())

    with patch.object(
        profiler.partition_discovery,
        "get_required_partition_filters",
        return_value=["`event_date` = '2024-11-20'"],
    ):
        kwargs = profiler.get_batch_kwargs(
            make_table(name="events", rows_count=500_000), "ds", "test-project-123456"
        )

    assert "TABLESAMPLE SYSTEM" in kwargs["custom_sql"]
    assert "WHERE" in kwargs["custom_sql"]
    assert "event_date" in kwargs["custom_sql"]


def test_external_table_deferred_in_get_profile_request():
    """External tables must be flagged for deferred partition discovery instead of
    having discovery run synchronously in get_profile_request.
    """
    config = make_config(profile_external_tables=True, partition_profiling_enabled=True)
    profiler = BigqueryProfiler(config, BigQueryV2Report())
    table = make_table(name="ext_table", external=True)

    result = profiler.get_profile_request(table, "ds", "test-project-123456")

    assert result is not None
    assert getattr(result, "needs_partition_discovery", False) is True
    assert getattr(result, "bq_table", None) is table


def test_partition_discovery_cache_avoids_repeat_info_schema_queries():
    """The dataset-level partition metadata cache should be populated with a single
    INFORMATION_SCHEMA.COLUMNS query for the whole dataset, so per-table calls hit
    the cache and don't issue further queries.
    """
    profiler = BigqueryProfiler(make_config(), BigQueryV2Report())

    call_count = 0

    def execute(query: str, job_config: Any, context: str) -> list:
        nonlocal call_count
        if "INFORMATION_SCHEMA.COLUMNS" in query and "is_partitioning_column" in query:
            call_count += 1
            return [
                SimpleNamespace(
                    table_name="table_a", column_name="event_date", data_type="DATE"
                ),
                SimpleNamespace(
                    table_name="table_b", column_name="run_date", data_type="DATE"
                ),
            ]
        return []

    with patch.object(profiler.query_executor, "execute_query_safely", new=execute):
        profiler._populate_partition_metadata_cache("test-project-123456", "ds")
        profiler._populate_partition_metadata_cache("test-project-123456", "ds")

    assert call_count == 1
    cache = profiler._partition_metadata_cache[("test-project-123456", "ds")]
    assert cache["table_a"]["partition_columns"] == ["event_date"]
    assert "table_b" in cache


def test_partition_filter_validation_rejects_injection():
    """Partition filters that contain SQL injection patterns must be rejected by
    validate_and_filter_expressions before they reach the custom_sql.
    """
    from datahub.ingestion.source.bigquery_v2.profiling.security import (
        validate_and_filter_expressions,
    )

    dangerous = [
        "`date` = '2024-01-01'; DROP TABLE users",
        "`col` = val /*comment*/",
        "1=1 UNION SELECT * FROM secrets",
    ]
    safe = ["`event_date` = '2024-11-20'", "`region_id` = 42"]

    result = validate_and_filter_expressions(dangerous + safe, "test")

    for expr in dangerous:
        assert expr not in result
    for expr in safe:
        assert expr in result


def test_information_schema_partitions_path():
    """Verify the INFORMATION_SCHEMA.PARTITIONS path: the most recent non-empty partition
    ID is picked and converted to a filter, without scanning actual table data.
    """
    discovery = PartitionDiscovery(make_config())

    def execute(query: str, job_config: Any, context: str) -> list:
        if "INFORMATION_SCHEMA.COLUMNS" in query and "is_partitioning_column" in query:
            return [SimpleNamespace(column_name="event_date", data_type="DATE")]
        if "INFORMATION_SCHEMA.PARTITIONS" in query:
            return [
                SimpleNamespace(partition_id="20241120", total_rows=10000),
                SimpleNamespace(partition_id="20241119", total_rows=8000),
            ]
        if "SELECT 1" in query:
            return [SimpleNamespace(cnt=1)]
        return []

    table = make_table(name="t")
    table.partition_info = SimpleNamespace(  # type: ignore[assignment]
        type_="DAY",
        field="event_date",
        fields=["event_date"],
        require_partition_filter=True,
    )

    filters = discovery.get_required_partition_filters(
        table, "my-project", "ds", execute
    )

    assert filters is not None and len(filters) > 0
    assert "2024-11-20" in " ".join(filters)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
