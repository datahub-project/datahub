import time
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, List, Optional

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    RANGE_PARTITION_NAME,
    BigqueryTable,
    PartitionInfo,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.discovery import (
    PartitionDiscovery,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.info_schema import (
    InfoSchemaQueries,
)
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

    # A DATETIME/TIMESTAMP partition column holds every instant in the day, so the same
    # YYYYMMDD id must become a half-open day range, not an equality to the day boundary
    # (which would exclude all rows after midnight).
    dt_filters = discovery._get_partition_filters_from_max_partition_id(
        make_table(max_partition_id="20241115"),
        required_columns=["event_ts"],
        column_types={"event_ts": "DATETIME"},
    )
    assert dt_filters == [
        "`event_ts` >= '2024-11-15 00:00:00' AND `event_ts` < '2024-11-16 00:00:00'"
    ]

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
        # The require-filter probe is `SELECT 1 FROM ... LIMIT` (not COUNT(*), and not
        # the verify query which selects `exists_check`); it raises the partition-filter
        # error whose text names the required columns.
        if "SELECT 1 FROM" in query and "exists_check" not in query:
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
    """An unpartitioned table's INFORMATION_SCHEMA.COLUMNS lookup succeeds and returns no
    partitioning columns. That authoritative-empty result means genuinely unpartitioned,
    so get_required_partition_filters returns [] (empty, not None).
    """
    discovery = PartitionDiscovery(make_config())

    # Return a real (empty) result shape: no partitioning-column rows. Earlier this mock
    # returned an object lacking `column_name`, so the COLUMNS read raised AttributeError
    # and the test only passed via the swallowed-error fallback, never exercising the
    # authoritative-empty path it documents.
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
        # value query then fails, exercising the IS NOT NULL last-resort fallback. The
        # probe is `SELECT 1 FROM ... LIMIT` (not the verify query with `exists_check`).
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            raise Exception("COLUMNS unavailable")
        if "INFORMATION_SCHEMA" in query:
            return []
        if "SELECT 1 FROM" in query and "exists_check" not in query:
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


def test_max_partition_id_infers_pseudo_column_type_for_hourly_range():
    """max_partition_id on an ingestion-time HOUR table partitions on _PARTITIONTIME, which
    is absent from INFORMATION_SCHEMA.COLUMNS (empty column_types). The zero-scan fast path
    must infer the TIMESTAMP pseudo-type so the id widens to a half-open hour range instead
    of a date-truncated point equality.
    """
    discovery = PartitionDiscovery(make_config())
    table = make_table(name="ingestion_hourly", max_partition_id="2024011513")
    table.partition_info = make_partition_info(  # type: ignore[assignment]
        "HOUR", "_PARTITIONTIME", ["_PARTITIONTIME"]
    )

    filters = discovery._get_partition_filters_from_max_partition_id(
        table, ["_PARTITIONTIME"], {}
    )

    assert filters == [
        "`_PARTITIONTIME` >= TIMESTAMP('2024-01-15 13:00:00') "
        "AND `_PARTITIONTIME` < TIMESTAMP('2024-01-15 14:00:00')"
    ]


def test_date_named_string_column_reaches_strategic_dates():
    """A STRING/INT64 column with a date-like *name* (event_date) must still reach the
    strategic-date fallback after direct discovery fails — that path builds a typed
    equality and is the only pruning fallback on require-filter / Hive-style tables. The
    old gate skipped it whenever the type was known.
    """

    class StrategicOnlyDiscovery(PartitionDiscovery):
        def _get_partition_column_types(self, *args: Any, **kwargs: Any):
            return {"event_date": "STRING"}

        def _get_partition_info_from_table_query(self, *args: Any, **kwargs: Any):
            return None

        def _test_date_candidate(self, *args: Any, **kwargs: Any):
            return ["`event_date` = '2025-01-15'"]

        def _get_partitions_with_sampling(self, *args: Any, **kwargs: Any):
            return None

    discovery = StrategicOnlyDiscovery(make_config())
    table = make_table(name="hive_style")

    def execute(query: str, job_config: Any, context: str) -> list:
        return []

    filters = discovery._find_real_partition_values(
        table, "test-project-123456", "ds", ["event_date"], execute
    )

    assert filters == ["`event_date` = '2025-01-15'"]


def test_strategic_candidates_prefer_latest_over_completion_order():
    # Both today and yesterday have data, but the yesterday probe finishes first. The
    # results must be consumed in candidate preference order (today first), so today
    # wins. Completion-order (as_completed) selection would wrongly return yesterday.
    class OrderingDiscovery(PartitionDiscovery):
        def _get_partition_column_types(self, *args: Any, **kwargs: Any):
            return {"event_date": "DATE"}

        def _get_partition_info_from_table_query(self, *args: Any, **kwargs: Any):
            return None

        def _test_date_candidate(self, *args: Any, **kwargs: Any):
            description = args[4]
            if description == "today":
                # Make today deliberately finish *after* yesterday.
                time.sleep(0.1)
                return ["`event_date` = '2025-01-16'"]
            return ["`event_date` = '2025-01-15'"]

        def _get_partitions_with_sampling(self, *args: Any, **kwargs: Any):
            return None

    discovery = OrderingDiscovery(make_config())
    table = make_table(name="date_tbl")

    filters = discovery._find_real_partition_values(
        table, "test-project-123456", "ds", ["event_date"], lambda *a, **k: []
    )

    assert filters == ["`event_date` = '2025-01-16'"]


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


def test_schema_fallback_preserves_ordinal_column_order():
    """The INFORMATION_SCHEMA.COLUMNS fallback must return partition columns in
    ordinal_position order (as the query yields them), not sorted alphabetically — a
    composite key is positional and reordering would bind values to the wrong columns.
    """
    discovery = PartitionDiscovery(make_config())

    # Rows arrive in ordinal_position order (region, then event_date). Alphabetical
    # sorting would swap them to (event_date, region).
    def execute(query: str, job_config: Any, context: str) -> list:
        return [
            SimpleNamespace(column_name="region"),
            SimpleNamespace(column_name="event_date"),
        ]

    columns, authoritative = discovery._get_partition_columns_from_schema(
        make_table(name="composite"), "test-project-123456", "ds", execute
    )

    assert authoritative is True
    assert columns == ["region", "event_date"]


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
    range covering its whole partition unit, not an equality to a single instant. This
    is the shared range logic that _test_date_candidate's strategic path also delegates
    to, so the exact-bounds assertions here guard both discovery paths.
    """
    discovery = PartitionDiscovery(make_config())
    table = make_table(partition_info=PartitionInfo(fields=("ts",), type="DAY"))

    ts_filter = discovery._value_filter(
        table, "ts", datetime(2025, 1, 15, 23, 59, 58), "TIMESTAMP"
    )
    assert ts_filter == (
        "`ts` >= TIMESTAMP('2025-01-15 00:00:00') "
        "AND `ts` < TIMESTAMP('2025-01-16 00:00:00')"
    )

    # A DATE column floors to the day and bounds the next day exclusively.
    date_filter = discovery._value_filter(
        table, "d", datetime(2025, 1, 15, 12, 0, 0), "DATE"
    )
    assert date_filter == "`d` >= '2025-01-15' AND `d` < '2025-01-16'"

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


def test_sampling_uses_known_columns_when_info_schema_unavailable():
    """When INFORMATION_SCHEMA.COLUMNS and DDL are both unavailable, sampling must still
    run using the partition columns the caller already resolved (from partition_info or
    the require-filter probe error), rather than bailing out.
    """
    discovery = PartitionDiscovery(make_config())
    table = make_table(name="sampled")

    def execute(query: str, job_config: Any, context: str) -> list:
        # No INFORMATION_SCHEMA / DDL access; only the date-sample and verify queries work.
        if "INFORMATION_SCHEMA" in query or "DDL" in query:
            raise Exception("metadata unavailable")
        if "ORDER BY" in query:  # LATEST_BY_DATE_SAMPLE
            return [SimpleNamespace(event_date=date(2024, 11, 20))]
        if "SELECT 1" in query:  # _verify_partition_has_data
            return [SimpleNamespace(cnt=1)]
        return []

    filters = discovery._get_partitions_with_sampling(
        table,
        "my-project",
        "ds",
        execute,
        known_columns=["event_date"],
        known_column_types={"event_date": "DATE"},
    )

    assert filters is not None and len(filters) == 1
    assert "event_date" in filters[0]
    assert "2024-11-20" in filters[0]


def test_strategic_candidate_path_emits_half_open_range_for_timestamp():
    """The strategic-candidate discovery path (_test_date_candidate) must delegate a
    TIMESTAMP partition column to the same half-open range logic as direct discovery, so
    a candidate date yields a full-day range rather than an equality to a single instant.
    """

    class NoEnhanceDiscovery(PartitionDiscovery):
        def _verify_partition_has_data(self, *args: Any, **kwargs: Any) -> bool:
            return True

        def _enhance_partition_filters_with_actual_values(
            self, table, project, schema, required_columns, filters, *args, **kwargs
        ):
            # Isolate the candidate-filter construction from the co-occurrence enhancement.
            return filters

    discovery = NoEnhanceDiscovery(make_config())
    table = make_table(partition_info=PartitionInfo(fields=("event_ts",), type="DAY"))

    def execute(query: str, job_config: Any, context: str) -> list:
        return []

    result = discovery._test_date_candidate(
        table,
        "test-project-123456",
        "ds",
        datetime(2025, 1, 15, 8, 30, 0, tzinfo=timezone.utc),
        "today",
        ["event_ts"],
        {"event_ts": "TIMESTAMP"},
        execute,
    )

    assert result == [
        "`event_ts` >= TIMESTAMP('2025-01-15 00:00:00+00:00') "
        "AND `event_ts` < TIMESTAMP('2025-01-16 00:00:00+00:00')"
    ]


def test_range_partition_uses_max_bucket_not_most_recently_modified():
    """INFORMATION_SCHEMA.PARTITIONS is ordered by last-modified, not bucket value. For a
    RANGE partition the lower-bound scan `col >= floor` must anchor on the MAX bucket floor
    (nothing exists above it, so it can't over-select) rather than the most-recently
    modified mid-range bucket, whose `>=` would also pull in every higher bucket.
    """
    info = InfoSchemaQueries(report=None)
    table = make_table(
        name="ranged",
        partition_info=PartitionInfo(fields=("bucket",), type=RANGE_PARTITION_NAME),
    )

    def execute(query: str, job_config: Any, context: str) -> list:
        # The numeric-max query orders by SAFE_CAST(partition_id AS INT64) DESC and returns
        # the true top bucket (300). The modified-ordered fetch deliberately OMITS 300
        # (the rarely-modified top bucket that falls outside its LIMIT) so this test only
        # passes when the dedicated max-bucket query is actually used — the pre-fix
        # fallback of max-over-modified-rows would resolve 200, not 300.
        if "SAFE_CAST(partition_id AS INT64) DESC" in query:
            return [SimpleNamespace(partition_id="300")]
        return [
            SimpleNamespace(partition_id="100"),
            SimpleNamespace(partition_id="200"),
            SimpleNamespace(partition_id="150"),
        ]

    def verify(*args: Any, **kwargs: Any) -> bool:
        return True

    filters = info.get_partition_filters_from_information_schema(
        table,
        "test-project-123456",
        "ds",
        ["bucket"],
        execute,
        verify,
        {"bucket": "INT64"},
    )

    assert filters == ["`bucket` >= 300"]
