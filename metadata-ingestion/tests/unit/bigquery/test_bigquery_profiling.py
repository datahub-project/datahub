from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryProfilingConfig,
    BigQueryV2Config,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
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
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig


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
        def _probe_required_partition_columns(
            self, *args: Any, **kwargs: Any
        ) -> Tuple[List[str], Optional[str]]:
            return [], "query timed out"

    discovery = ProbeErrorDiscovery(make_config())

    filters = discovery.get_required_partition_filters(
        make_table(name="unknown_state"), "test-project-123456", "ds", execute
    )

    assert filters is None


def test_authoritative_empty_columns_skips_probe():
    """A successful, empty COLUMNS result is definitive (unpartitioned), so the probe
    fallback must not run and the table is profiled unfiltered ([]).
    """

    def execute(query: str, job_config: Any, context: str) -> list:
        return []

    class ProbeGuardDiscovery(PartitionDiscovery):
        def _probe_required_partition_columns(
            self, *args: Any, **kwargs: Any
        ) -> Tuple[List[str], Optional[str]]:
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
            self,
            table: BigqueryTable,
            project: str,
            schema: str,
            required_columns: List[str],
            initial_filters: List[str],
            *args: Any,
            **kwargs: Any,
        ) -> Optional[List[str]]:
            # Isolate the candidate-filter construction from the co-occurrence enhancement.
            return initial_filters

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
        table, ["_PARTITIONTIME"], {}
    )

    assert filters is not None
    assert len(filters) == 1
    assert "_PARTITIONTIME" in filters[0]
    assert ">=" in filters[0]


def test_failed_columns_lookup_with_clean_probe_skips_table():
    """When the COLUMNS lookup fails but the fallback probe runs cleanly (no
    require_partition_filter error), the partition state is still unknown: a partitioned
    table with require_partition_filter=FALSE probes cleanly too. The table must be
    skipped (None), never treated as unpartitioned ([]) and full-scanned.
    """

    def execute(query: str, job_config: Any, context: str) -> list:
        raise RuntimeError("INFORMATION_SCHEMA unavailable")

    # Default _probe_required_partition_columns stub returns ([], None): a clean probe
    # that discovered no columns. That must not be read as "unpartitioned".
    discovery = PartitionDiscovery(make_config())

    filters = discovery.get_required_partition_filters(
        make_table(name="failed_columns_clean_probe"),
        "test-project-123456",
        "ds",
        execute,
    )

    assert filters is None


def test_probe_fallback_preserves_column_order():
    """When the COLUMNS lookup fails and the probe recovers columns from BigQuery's
    require-filter error, the coordinator must keep the error's column order — a composite
    partition key is positional, so sorting alphabetically would bind partition-id
    components to the wrong columns downstream.
    """
    seen_columns: List[List[str]] = []

    class OrderedProbeDiscovery(PartitionDiscovery):
        def _probe_required_partition_columns(
            self, *args: Any, **kwargs: Any
        ) -> Tuple[List[str], Optional[str]]:
            # BigQuery lists the columns in this order in the require-filter error;
            # alphabetical sorting would swap them to (event_date, region).
            return ["region", "event_date"], "requires filter over column(s)"

        def _get_partition_column_types(
            self,
            table: BigqueryTable,
            project: str,
            schema: str,
            partition_columns: List[str],
            *args: Any,
            **kwargs: Any,
        ) -> Dict[str, str]:
            seen_columns.append(list(partition_columns))
            return {}

    def execute(query: str, job_config: Any, context: str) -> list:
        raise RuntimeError("INFORMATION_SCHEMA unavailable")

    discovery = OrderedProbeDiscovery(make_config())
    discovery.get_required_partition_filters(
        make_table(name="composite_probe"), "test-project-123456", "ds", execute
    )

    assert seen_columns
    assert seen_columns[0] == ["region", "event_date"]


def test_ddl_columns_survive_type_lookup_failure():
    """A failed INFORMATION_SCHEMA type lookup must not drop the DDL-extracted partition
    columns: an empty dict is indistinguishable from an unpartitioned table and would
    trigger a full scan. The column is kept with an unknown ("") type instead.
    """
    discovery = PartitionDiscovery(make_config())

    def execute(query: str, job_config: Any, context: str) -> list:
        raise RuntimeError("INFORMATION_SCHEMA.COLUMNS unavailable")

    table = make_table(
        name="ddl_partitioned",
        ddl="CREATE TABLE ds.ddl_partitioned (event_date DATE) PARTITION BY event_date",
    )

    result = discovery.get_partition_columns_from_ddl(
        table, "test-project-123456", "ds", execute
    )

    assert result == {"event_date": ""}


def test_fallback_uses_range_for_temporal_column_not_full_scan():
    """When discovery falls back for a DATE/DATETIME/TIMESTAMP partition column, it must
    prune to a single recent partition with a granularity-aware half-open range rather than
    emit an IS NOT NULL full scan. Temporal columns are the most common partition type and
    the exact scan this feature exists to avoid; previously fallback_date was only applied
    to year/month/day component columns and real temporal columns silently got IS NOT NULL.
    """
    discovery = PartitionDiscovery(make_config())

    filters = discovery._get_fallback_partition_filters(
        make_table(name="daily_events"),
        "test-project-123456",
        "ds",
        ["event_ts"],
        {"event_ts": "TIMESTAMP"},
    )

    # Pin the exact single-day bounds (not just "some range exists"): a regression that
    # widened the window (e.g. to 7 days or a year) would still contain ">=" and "<" but
    # would break the single-partition prune this test documents. Bounds are derived the
    # same way _get_fallback_partition_filters does: yesterday (UTC), floored to the day.
    lower_day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    upper_day = lower_day + timedelta(days=1)
    assert filters == [
        f"`event_ts` >= TIMESTAMP('{lower_day} 00:00:00+00:00') "
        f"AND `event_ts` < TIMESTAMP('{upper_day} 00:00:00+00:00')"
    ]


def test_configured_iso_string_fallback_widens_temporal_partition():
    """A user-configured fallback_partition_values entry for a DATETIME/TIMESTAMP partition
    arrives as an ISO string. It must be normalized to a datetime and widened to the whole
    partition (half-open range), not emitted as a point equality that matches a single
    instant. A compact partition id (all-digit) must keep create_safe_filter's compact-id
    handling and must not be misread as a date.
    """
    discovery = PartitionDiscovery(
        make_config(fallback_partition_values={"event_ts": "2025-01-15"})
    )
    table = make_table(partition_info=PartitionInfo(fields=("event_ts",), type="DAY"))

    widened = discovery._create_fallback_filter_for_column(
        table, "event_ts", datetime(2025, 1, 15, tzinfo=timezone.utc), "TIMESTAMP"
    )
    # A date-only ISO string parses to a naive datetime, so the literal carries no offset;
    # the point is the half-open day range (not an equality to a single instant).
    assert widened == (
        "`event_ts` >= TIMESTAMP('2025-01-15 00:00:00') "
        "AND `event_ts` < TIMESTAMP('2025-01-16 00:00:00')"
    )

    # A compact partition id is all-digit (no ISO separators): it must not be parsed as a
    # date, so it stays on create_safe_filter's compact-id path (an equality), not widened.
    compact = discovery._parse_iso_temporal("20250115")
    assert compact is None

    # BigQuery DATETIME is timezone-naive: an offset-bearing string must not be widened
    # (the range builder would drop the offset and floor the wrong wall clock). It defers
    # to create_safe_filter, which rejects tz-bearing DATETIME, so the column falls back to
    # IS NOT NULL rather than a mis-floored range.
    naive_dt = discovery._value_filter(table, "dt", "2025-01-15T10:00:00", "DATETIME")
    assert ">=" in naive_dt and "<" in naive_dt
    # A tz-bearing DATETIME config value is not widened into a naive range; it defers to
    # create_safe_filter (which rejects it), so the column falls back to IS NOT NULL.
    discovery_tz = PartitionDiscovery(
        make_config(fallback_partition_values={"dt": "2025-01-15T10:00:00+05:00"})
    )
    tz_result = discovery_tz._create_fallback_filter_for_column(
        make_table(), "dt", datetime(2025, 1, 15), "DATETIME"
    )
    assert "IS NOT NULL" in tz_result


def test_value_filter_widens_date_string_at_month_granularity():
    """A DATE partition column on a MONTH-granularity table holds whole-month partitions,
    so a configured DATE string must be widened to the month's half-open range rather than
    emitted as a single-day equality that matches only one day of the partition.
    """
    discovery = PartitionDiscovery(make_config())
    table = make_table(partition_info=PartitionInfo(fields=("d",), type="MONTH"))

    result = discovery._value_filter(table, "d", "2025-01-15", "DATE")

    assert result == "`d` >= '2025-01-01' AND `d` < '2025-02-01'"


def test_partition_datetime_override_rejects_offset_bearing_datetime():
    """A configured partition_datetime carrying a UTC offset can't be expressed on a
    timezone-naive DATETIME column without silently dropping the offset (and flooring the
    wrong wall clock). The override must route through _value_filter's shared rejection so
    it is ignored with a warning instead of producing a mis-floored range.
    """
    report = BigQueryV2Report()
    offset_dt = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone(timedelta(hours=5)))
    discovery = PartitionDiscovery(make_config(partition_datetime=offset_dt), report)

    filters = discovery._get_partition_datetime_override_filters(
        make_table(name="tz_override"), ["dt"], {"dt": "DATETIME"}
    )

    assert filters is None
    assert any(
        (w.title or "") == "partition_datetime not applied" for w in report.warnings
    )


def test_fallback_date_component_unknown_type_scans_all():
    """A date-component column (e.g. `year`) with no known type can't be given a typed
    literal — an untyped `year = '2026'` string is rejected against an INT64 column — so
    the fallback must degrade to a full scan (IS NOT NULL) rather than emit that literal.
    """
    discovery = PartitionDiscovery(make_config())

    result = discovery._create_fallback_filter_for_column(
        make_table(name="component_no_type"), "year", datetime(2026, 3, 1), ""
    )

    assert result == "`year` IS NOT NULL"


def test_partition_column_types_backfills_pseudo_columns():
    """INFORMATION_SCHEMA.COLUMNS never lists the ingestion-time pseudo-columns, so
    get_partition_column_types must backfill their fixed BigQuery types
    (_PARTITIONTIME -> TIMESTAMP) rather than leaving the pseudo-column untyped, which
    would force a string point-equality instead of a typed half-open range downstream.
    """
    info_schema = InfoSchemaQueries()

    def execute(query: str, job_config: Any, context: str) -> list:
        return [SimpleNamespace(column_name="region", data_type="STRING")]

    types = info_schema.get_partition_column_types(
        make_table(name="ingestion_time"),
        "test-project-123456",
        "ds",
        ["region", "_PARTITIONTIME"],
        execute,
    )

    assert types == {"region": "STRING", "_PARTITIONTIME": "TIMESTAMP"}


def test_partition_column_types_keeps_pseudo_columns_on_lookup_failure():
    """The pseudo-column types are fixed and query-independent, so a failed
    INFORMATION_SCHEMA.COLUMNS lookup must still return them — only the real column's
    type is lost, not _PARTITIONTIME/_PARTITIONDATE.
    """
    info_schema = InfoSchemaQueries()

    def failing_execute(query: str, job_config: Any, context: str) -> list:
        raise RuntimeError("COLUMNS query timed out")

    types = info_schema.get_partition_column_types(
        make_table(name="ingestion_time"),
        "test-project-123456",
        "ds",
        ["region", "_PARTITIONTIME"],
        failing_execute,
    )

    assert types == {"_PARTITIONTIME": "TIMESTAMP"}


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


def test_date_column_hour_granularity_degrades_to_day():
    """A DATE partition column cannot express an hour. If the table's partition granularity
    is HOUR, an hourly range would floor both bounds to the same YYYY-MM-DD and match zero
    rows; the filter must degrade to a full-day range (mirroring FilterBuilder's guard).
    """
    discovery = PartitionDiscovery(make_config())
    table = make_table(
        partition_info=PartitionInfo(fields=("event_date",), type="HOUR")
    )

    result = discovery._value_filter(
        table, "event_date", datetime(2025, 1, 15, 10, 30), "DATE"
    )

    assert result == "`event_date` >= '2025-01-15' AND `event_date` < '2025-01-16'"


def test_guessed_fallback_date_emits_warning():
    """Narrowing a temporal partition to a guessed fallback date (yesterday) without
    verifying it holds rows can yield a zero-row profile for infrequently-loaded tables,
    so the fallback must warn operators to pin a known-populated partition.
    """
    report = BigQueryV2Report()
    discovery = PartitionDiscovery(make_config(), report)

    filters = discovery._get_fallback_partition_filters(
        make_table(name="weekly_events"),
        "test-project-123456",
        "ds",
        ["event_ts"],
        {"event_ts": "TIMESTAMP"},
    )

    # The column is still pruned to a range (not a full scan)...
    assert filters and ">=" in filters[0] and "<" in filters[0]
    # ...but the guess is surfaced so operators know the profile may be empty.
    assert any("guessed a fallback date" in (w.title or "") for w in report.warnings)


def test_unresolved_date_column_keeps_is_not_null_placeholder():
    """In a composite key, a date column that the date path could not resolve to a concrete
    value must keep its IS NOT NULL placeholder (so a require_partition_filter table still
    gets a predicate for it) and be reported as unresolved, rather than being dropped.
    """
    report = BigQueryV2Report()

    class TypedDiscovery(PartitionDiscovery):
        def _get_partition_column_types(
            self, *args: Any, **kwargs: Any
        ) -> Dict[str, str]:
            return {"event_date": "DATE", "other_date": "DATE"}

    discovery = TypedDiscovery(make_config(), report)

    def execute(query: str, job_config: Any, context: str) -> list:
        return []

    result = discovery._enhance_partition_filters_with_actual_values(
        make_table(name="composite_dates"),
        "test-project-123456",
        "ds",
        ["event_date", "other_date"],
        # event_date resolved; other_date only had the IS NOT NULL placeholder.
        ["`event_date` = '2025-01-15'", "`other_date` IS NOT NULL"],
        execute,
    )

    assert result is not None
    assert "`event_date` = '2025-01-15'" in result
    assert "`other_date` IS NOT NULL" in result
    assert any("fell back to full scan" in (w.title or "") for w in report.warnings)


def test_partition_fetch_job_config_applies_timeout_and_byte_cap():
    """partition_fetch_timeout must actually reach the fetch jobs (as job_timeout_ms), and
    partition_fetch_max_bytes_billed must cap bytes billed only when configured.
    """
    capped = PartitionDiscovery(
        make_config(partition_fetch_timeout=45, partition_fetch_max_bytes_billed=1024)
    )
    job_config = capped._partition_fetch_job_config()
    # The BigQuery client round-trips job_timeout_ms as a string; compare numerically.
    assert int(job_config.job_timeout_ms) == 45000
    assert int(job_config.maximum_bytes_billed) == 1024

    uncapped = PartitionDiscovery(make_config(partition_fetch_timeout=10))
    job_config = uncapped._partition_fetch_job_config()
    assert int(job_config.job_timeout_ms) == 10000
    assert job_config.maximum_bytes_billed is None


def test_profiling_field_accepts_ge_profiling_config_instance():
    """A caller may build the config in code and pass a GEProfilingConfig instance for
    `profiling`. Retyping the field to the BigQueryProfilingConfig subclass must not break
    that: the before-validator coerces the instance to a dict so re-validation succeeds.
    """
    config = BigQueryV2Config.parse_obj(
        {
            "project_id": "test-project-123456",
            "profiling": GEProfilingConfig(enabled=True, profile_table_level_only=True),
        }
    )

    assert isinstance(config.profiling, BigQueryProfilingConfig)
    assert config.profiling.enabled is True
    assert config.profiling.profile_table_level_only is True
