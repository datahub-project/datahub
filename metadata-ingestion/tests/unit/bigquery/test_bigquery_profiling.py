from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Optional

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
        return []

    filters = discovery.get_required_partition_filters(
        make_table(name="unpartitioned"), "test-project-123456", "ds", execute
    )
    assert filters == []


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


def test_partition_discovery_strategic_dates():
    discovery = PartitionDiscovery(make_config())
    dates = discovery._get_strategic_candidate_dates()

    assert len(dates) == 2
    assert dates[0][0] >= dates[1][0]
    descriptions = [d for _, d in dates]
    assert any("today" in d.lower() for d in descriptions)
    assert any("yesterday" in d.lower() for d in descriptions)


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
        # Rows in last-modified order: the mid bucket (100) was touched most recently,
        # but 300 is the true maximum populated bucket.
        return [
            SimpleNamespace(partition_id="100"),
            SimpleNamespace(partition_id="300"),
            SimpleNamespace(partition_id="200"),
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
