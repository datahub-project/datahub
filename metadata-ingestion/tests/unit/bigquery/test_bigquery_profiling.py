from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import patch

import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryTable,
    PartitionInfo,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.discovery import (
    PartitionDiscovery,
)
from datahub.ingestion.source.bigquery_v2.profiling.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    validate_and_filter_expressions,
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


def make_partition_info(type_: str, field: str, fields: List[str]) -> SimpleNamespace:
    # The tests only read type/field/fields/columns/require_partition_filter off
    # partition_info, so a lightweight SimpleNamespace stands in for PartitionInfo.
    return SimpleNamespace(
        type=type_,
        field=field,
        fields=fields,
        columns=None,
        require_partition_filter=True,
    )


def test_partition_discovery_strategic_dates():
    discovery = PartitionDiscovery(make_config())
    dates = discovery._get_strategic_candidate_dates()

    assert len(dates) == 2
    assert dates[0][0] >= dates[1][0]
    descriptions = [d for _, d in dates]
    assert any("today" in d.lower() for d in descriptions)
    assert any("yesterday" in d.lower() for d in descriptions)


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
    table.partition_info = make_partition_info("DAY", "run_date", ["run_date"])  # type: ignore[assignment]

    filters = discovery.get_required_partition_filters(
        table, "proj", "ds", tracking_execute
    )

    assert filters is not None and len(filters) == 1
    assert "2024-12-01" in filters[0]
    assert not any("INFORMATION_SCHEMA.PARTITIONS" in q for q in query_calls)


def test_partition_detection_via_query_error():
    """When the INFORMATION_SCHEMA.COLUMNS lookup is unavailable and the table has
    require_partition_filter, BigQuery raises an error whose text names the required
    columns.  The profiler falls back to a probe query, extracts those column names from
    the error message, and continues discovery.

    Real BigQuery error format: "filter over column(s) 'event_date'"  (column in quotes).
    """
    discovery = PartitionDiscovery(make_config())

    bq_error = (
        "Error 400: Cannot query over table 'my-project.ds.t' without a filter "
        "that can be used for partition elimination. "
        "Required filter over column(s) 'event_date'."
    )

    def execute(query: str, job_config: Any, context: str) -> list:
        # A successful-but-empty COLUMNS result is authoritative ("unpartitioned"), so the
        # probe is only reached when the COLUMNS lookup itself fails.
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            raise Exception("COLUMNS unavailable")
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
        # An authoritative (successful) COLUMNS lookup with no partition columns means the
        # table is genuinely unpartitioned; the probe is skipped and [] is returned.
        if "INFORMATION_SCHEMA" in query:
            return []
        return [SimpleNamespace(cnt=42)]

    filters = discovery.get_required_partition_filters(
        make_table(name="unpartitioned"), "test-project-123456", "ds", execute
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
        # COLUMNS lookup fails, so the probe runs and its error names the column; every
        # value query then fails, exercising the IS NOT NULL last-resort fallback.
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            raise Exception("COLUMNS unavailable")
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
    table.partition_info = make_partition_info("RANGE", "region_id", ["region_id"])  # type: ignore[assignment]

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
    table.partition_info = make_partition_info(  # type: ignore[assignment]
        "DAY", "event_date", ["event_date", "feed"]
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
    table.partition_info = make_partition_info(  # type: ignore[assignment]
        "DAY", "event_date", ["event_date", "feed"]
    )

    filters = discovery.get_required_partition_filters(
        table, "my-project", "ds", execute
    )

    # Should not crash; date filter must be present even if feed filter couldn't be built
    assert filters is not None
    assert any("event_date" in f or "2024-11-20" in f for f in filters)


def test_partition_filter_validation_rejects_injection():
    """Partition filters that contain SQL injection patterns must be rejected by
    validate_and_filter_expressions before they reach the custom_sql.
    """
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
    table.partition_info = make_partition_info("DAY", "event_date", ["event_date"])  # type: ignore[assignment]

    filters = discovery.get_required_partition_filters(
        table, "my-project", "ds", execute
    )

    assert filters is not None and len(filters) > 0
    assert "2024-11-20" in " ".join(filters)


def test_partition_columns_from_table_info_preserves_order_and_dedups():
    """Composite partition columns are positional, so declared order must be preserved
    (not sorted) and duplicates collapsed.
    """
    discovery = PartitionDiscovery(make_config())
    table = make_table(
        partition_info=PartitionInfo(fields=("region", "event_date", "region"))
    )

    columns = discovery._get_partition_columns_from_table_info(table)

    assert columns == ["region", "event_date"]


def test_inconclusive_detection_skips_partitioned_table():
    """When the COLUMNS lookup fails and the probe errors, the partition state is unknown.
    The table must be skipped (None), not treated as unpartitioned ([]).
    """

    def execute(query: str, job_config: Any, context: str) -> list:
        raise RuntimeError("INFORMATION_SCHEMA unavailable")

    class ProbeErrorDiscovery(PartitionDiscovery):
        def _probe_required_partition_columns(self, *args: Any, **kwargs: Any):
            return set(), "query timed out"

    discovery = ProbeErrorDiscovery(make_config())

    filters = discovery.get_required_partition_filters(
        make_table(name="unknown_state"), "proj", "ds", execute
    )

    assert filters is None


def test_authoritative_empty_columns_skips_probe():
    """A successful, empty COLUMNS result is definitive (unpartitioned), so the probe
    fallback must not run and the table is profiled unfiltered ([]).
    """

    def execute(query: str, job_config: Any, context: str) -> list:
        return []

    class ProbeGuardDiscovery(PartitionDiscovery):
        def _probe_required_partition_columns(self, *args: Any, **kwargs: Any):
            raise AssertionError(
                "probe must not run after an authoritative COLUMNS result"
            )

    discovery = ProbeGuardDiscovery(make_config())

    filters = discovery.get_required_partition_filters(
        make_table(name="authoritative_unpartitioned"),
        "test-project-123456",
        "ds",
        execute,
    )

    assert filters == []


def test_date_components_without_year_marked_incomplete():
    """A month/day component without a year can't pin a single partition, so the
    hierarchy must be flagged incomplete (not silently dropped as a complete empty set).
    """
    discovery = PartitionDiscovery(make_config())

    def execute(query: str, job_config: Any, context: str) -> list:
        raise AssertionError("no query should run when year is absent")

    result = discovery._process_date_components_hierarchically(
        {"year": None, "month": "month", "day": None},
        "`p`.`d`.`t`",
        execute,
        {},
        {},
    )

    assert result.filters == []
    assert result.incomplete is True


def test_value_filter_ranges_for_temporal_columns():
    """A discovered MAX value on a DATETIME/TIMESTAMP column must produce a half-open
    range covering its partition, not an equality to a single instant.
    """
    discovery = PartitionDiscovery(make_config())
    table = make_table(partition_info=PartitionInfo(fields=("ts",), type="DAY"))

    ts_filter = discovery._value_filter(
        table, "ts", datetime(2025, 1, 15, 23, 59, 58), "TIMESTAMP"
    )
    assert ">=" in ts_filter and "<" in ts_filter

    # A non-temporal column keeps a plain equality.
    region_filter = discovery._value_filter(table, "region", "emea", "STRING")
    assert region_filter == "`region` = 'emea'"


def test_ingestion_time_partition_datetime_override_applies():
    """_PARTITIONTIME is absent from INFORMATION_SCHEMA.COLUMNS, so column_types is empty;
    the configured partition_datetime must still apply by inferring the pseudo-column type.
    """
    discovery = PartitionDiscovery(
        make_config(partition_datetime=datetime(2025, 1, 15))
    )
    table = make_table(
        partition_info=PartitionInfo(fields=("_PARTITIONTIME",), type="DAY")
    )

    filters = discovery._get_partition_datetime_override_filters(
        table, {"_PARTITIONTIME"}, {}
    )

    assert filters is not None
    assert len(filters) == 1
    assert "_PARTITIONTIME" in filters[0]
    assert ">=" in filters[0]


def test_first_complete_row_requires_coexisting_tuple():
    """A composite partition filter must come from a single co-occurring row, not the
    first non-null value of each column picked across different rows (which could
    fabricate a tuple that never exists together)."""
    rows = [
        SimpleNamespace(a=1, b=None),
        SimpleNamespace(a=None, b=2),
    ]
    # No single row has both columns populated -> no safe composite tuple.
    assert PartitionDiscovery._first_complete_row(["a", "b"], rows) is None

    rows_with_complete = rows + [SimpleNamespace(a=3, b=4)]
    assert PartitionDiscovery._first_complete_row(["a", "b"], rows_with_complete) == {
        "a": 3,
        "b": 4,
    }


def test_direct_discovery_timestamp_value_yields_range():
    """A TIMESTAMP value discovered from the latest row must widen to a half-open range
    covering the whole day, not an equality to the single instant."""
    discovery = PartitionDiscovery(make_config())
    table = make_table(name="ts_partitioned")
    table.partition_info = make_partition_info("DAY", "ts", ["ts"])  # type: ignore[assignment]

    filters = discovery._filters_from_partition_values(
        table,
        {"ts": datetime(2024, 11, 20, 15, 30, 0)},
        {"ts": "TIMESTAMP"},
    )

    assert len(filters) == 1
    assert ">=" in filters[0] and "<" in filters[0]
    assert "2024-11-20" in filters[0] and "2024-11-21" in filters[0]


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
    """Unpartitioned sampling threshold: at/below sample_size the table is profiled in
    full (no custom_sql), above sample_size TABLESAMPLE is applied. Sampling only ever
    happens on this unpartitioned path — the partition path never samples (see
    test_batch_kwargs_sampling_with_partition_filter).
    """
    config = make_config(use_sampling=True, sample_size=1000, profiling_row_limit=10000)
    profiler = BigqueryProfiler(config, BigQueryV2Report())

    with patch.object(
        profiler.partition_discovery,
        "get_required_partition_filters",
        return_value=[],
    ):
        small = profiler.get_batch_kwargs(
            make_table(rows_count=500), "test_dataset", "test-project-123456"
        )
        assert "custom_sql" not in small

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


def test_batch_kwargs_partition_spec_classification():
    """A single-partition scan carries the partition key (PARTITION); an unpartitioned
    table and a window-widened scan do not (QUERY/FULL_TABLE), so the reported
    partitionSpec matches the data actually scanned."""
    # Windowing off, single partition column -> the scan is exactly one partition, so
    # the partition key is set.
    config = make_config(partition_datetime_window_days=None)
    profiler = BigqueryProfiler(config, BigQueryV2Report())
    with patch.object(
        profiler.partition_discovery,
        "get_required_partition_filters",
        return_value=["`date` = '2023-12-25'"],
    ):
        partitioned = profiler.get_batch_kwargs(
            make_table(rows_count=1000, max_partition_id="20231225"),
            "test_dataset",
            "test-project-123456",
        )
        assert partitioned.get("partition") == "20231225"

    # No partition filters and no partition id -> unpartitioned; no partition key.
    with patch.object(
        profiler.partition_discovery,
        "get_required_partition_filters",
        return_value=[],
    ):
        unpartitioned = profiler.get_batch_kwargs(
            make_table(rows_count=500),
            "test_dataset",
            "test-project-123456",
        )
        assert "partition" not in unpartitioned

    # A partition id exists but discovery produced no usable filter, so the profile is
    # a full-table/sample/limit scan. Labeling it with the partition id would
    # misdescribe the data, so the key must be omitted.
    with patch.object(
        profiler.partition_discovery,
        "get_required_partition_filters",
        return_value=[],
    ):
        empty_predicate = profiler.get_batch_kwargs(
            make_table(rows_count=1000, max_partition_id="20231225"),
            "test_dataset",
            "test-project-123456",
        )
        assert "partition" not in empty_predicate

    # Windowing on (default) widens the scan to a range, so labeling it with the single
    # anchor partition id would misdescribe the data — the key must be omitted.
    windowed_config = make_config(partition_datetime_window_days=30)
    windowed_profiler = BigqueryProfiler(windowed_config, BigQueryV2Report())
    with patch.object(
        windowed_profiler.partition_discovery,
        "get_required_partition_filters",
        return_value=["`date` = '2023-12-25'"],
    ):
        windowed = windowed_profiler.get_batch_kwargs(
            make_table(rows_count=1000, max_partition_id="20231225"),
            "test_dataset",
            "test-project-123456",
        )
        assert "partition" not in windowed


def test_predicate_scans_single_partition_ignores_operators_in_literals():
    """A range operator (or BETWEEN) that only appears inside a quoted STRING/Hive
    partition value must not be mistaken for a range predicate, so an exact equality
    scan keeps its single-partition label."""
    single = BigqueryProfiler._predicate_scans_single_partition
    # Equalities whose literal contains a comparison operator / BETWEEN are still one
    # partition each.
    assert single("`country` = 'a>b'")
    assert single("`region` = 'x<y'")
    assert single("`label` = 'BETWEEN us and them'")
    # Genuine generated ranges span several partitions.
    assert not single("`date` >= '2025-01-01' AND `date` < '2026-01-01'")
    assert not single("`ts` BETWEEN '2025-01-01' AND '2025-02-01'")
    # A same-day (zero-width) range still scans exactly one partition.
    assert single("`date` >= '2025-01-01' AND `date` <= '2025-01-01'")


def test_sample_percent_guards_zero_sample_size():
    """A zero/non-positive sample size or empty table must not emit
    TABLESAMPLE SYSTEM (0 PERCENT), which BigQuery rejects."""
    profiler = BigqueryProfiler(make_config(sample_size=0), BigQueryV2Report())
    assert profiler._sample_percent(1000) == 100.0

    profiler = BigqueryProfiler(make_config(sample_size=1000), BigQueryV2Report())
    assert profiler._sample_percent(0) == 100.0
    assert 0 < profiler._sample_percent(10000) <= 100.0


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
    """With a partition filter the custom_sql must apply the WHERE but must NOT emit
    TABLESAMPLE, even when sampling is enabled. BigQuery samples whole-table blocks before
    the WHERE and sizes the percentage from the whole-table row count, so a small target
    partition of a large table could come back empty; the downstream profiler samples the
    materialized partition instead.
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

    assert "TABLESAMPLE" not in kwargs["custom_sql"]
    assert "WHERE" in kwargs["custom_sql"]
    assert "event_date" in kwargs["custom_sql"]


def test_external_table_deferred_in_get_workunits():
    """External tables must be routed to deferred partition discovery (wrapped in a
    DeferredExternalTable), not profiled synchronously in the main loop.
    """
    config = make_config(profile_external_tables=True, partition_profiling_enabled=True)
    profiler = BigqueryProfiler(config, BigQueryV2Report())
    table = make_table(name="ext_table", external=True)

    captured: dict = {}

    def fake_generate(profile_requests, deferred_external, **kwargs):
        captured["profile_requests"] = list(profile_requests)
        captured["deferred_external"] = list(deferred_external)
        return []

    with (
        patch.object(
            profiler.partition_discovery,
            "get_required_partition_filters",
            return_value=["`event_date` = '2024-11-20'"],
        ),
        patch.object(
            profiler,
            "generate_profile_workunits_with_deferred_partitions",
            side_effect=fake_generate,
        ),
    ):
        list(profiler.get_workunits("test-project-123456", {"ds": [table]}))

    assert captured["profile_requests"] == []
    assert len(captured["deferred_external"]) == 1
    deferred = captured["deferred_external"][0]
    assert deferred.bq_table is table
    assert deferred.db_name == "test-project-123456"
    assert deferred.schema_name == "ds"


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
