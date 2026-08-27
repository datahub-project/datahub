from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Optional

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryTable,
    PartitionInfo,
)
from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.discovery import (
    PartitionDiscovery,
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
