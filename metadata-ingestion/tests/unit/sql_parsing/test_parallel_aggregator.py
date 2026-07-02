"""Equivalence tests for per-session-safe parallel SQL parsing in SqlParsingAggregator.

The correctness bar: parallel output MUST be byte-identical to serial output
(no lineage loss). Each test runs the same scripted stream of queries through a
serial aggregator (feature off) and a parallel aggregator (feature on, wrapped in
``parallel_sql_parsing_scope()``), then asserts the emitted MCPs are equal after
normalizing ordering.
"""

from datetime import datetime, timezone
from typing import Iterator, List, Union
from unittest.mock import patch

import pytest
import time_machine

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.parallel_sql_parser import ParallelParserUnavailable
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    PreparsedQuery,
    QueryLogSetting,
    QueryMetadata,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sql_parsing_common import QueryType

_StreamItem = Union[ObservedQuery, PreparsedQuery]

# Patch CPU detection to a high value so that explicitly-requested worker counts
# (workers=2, workers=4) are never clamped in constrained CI environments.
# Individual tests that exercise clamping/detection-fallback override this via
# monkeypatch on ``datahub.utilities.cpu_detection.get_available_cpu_count``.
_DETECTED_CPUS_DEFAULT = 32


@pytest.fixture(autouse=True)
def _pin_cpu_detection() -> Iterator[None]:
    with patch(
        "datahub.utilities.cpu_detection.get_available_cpu_count",
        return_value=_DETECTED_CPUS_DEFAULT,
    ):
        yield


# Freeze wall-clock so audit stamps that fall back to datetime.now() (for
# downstream tables with no explicit timestamp) are identical across the serial
# and parallel runs being compared.
FROZEN_TIME = "2024-02-06T01:23:45Z"


def _ts(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _make_aggregator(
    *,
    use_parallel: bool,
    workers: int = 2,
    query_log: QueryLogSetting = QueryLogSetting.DISABLED,
) -> SqlParsingAggregator:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=query_log,
        use_parallel_sql_parsing=use_parallel,
        sql_parsing_workers=workers if use_parallel else None,
    )
    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.bar").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )
    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.upstream1").urn(),
        {"a": "int", "b": "int"},
    )
    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.upstream2").urn(),
        {"a": "int", "c": "int"},
    )
    return aggregator


def _mcp_key(mcp: object) -> tuple:
    return (
        str(getattr(mcp, "entityUrn", None)),
        str(getattr(mcp, "aspectName", None)),
        str(getattr(mcp, "aspect", None)),
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def _run_serial(items: List[_StreamItem]) -> list:
    aggregator = _make_aggregator(use_parallel=False)
    for item in items:
        aggregator.add(item)
    mcps = list(aggregator.gen_metadata())
    aggregator.close()
    return sorted(mcps, key=_mcp_key)


@time_machine.travel(FROZEN_TIME, tick=False)
def _run_parallel(items: List[_StreamItem], workers: int = 2) -> tuple:
    aggregator = _make_aggregator(use_parallel=True, workers=workers)
    with aggregator.parallel_sql_parsing_scope():
        for item in items:
            aggregator.add(item)
    mcps = list(aggregator.gen_metadata())
    report = aggregator.report
    aggregator.close()
    return sorted(mcps, key=_mcp_key), report


def _assert_equivalent(items: List[_StreamItem]) -> tuple:
    serial = _run_serial(items)
    parallel, report = _run_parallel(items)
    serial_keys = [_mcp_key(m) for m in serial]
    parallel_keys = [_mcp_key(m) for m in parallel]
    assert parallel_keys == serial_keys
    return parallel, report


def test_equivalence_no_temp_tables() -> None:
    """Core no-loss proof: a mix of observed + preparsed queries across sessions,
    no temp tables. Parallel output must equal serial output."""
    items: List[_StreamItem] = [
        ObservedQuery(
            query="create table foo as select a, b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s1",
            timestamp=_ts(10),
        ),
        ObservedQuery(
            query="insert into downstream (a, b) select a, b from upstream1",
            default_db="dev",
            default_schema="public",
            session_id="s2",
            timestamp=_ts(20),
        ),
        PreparsedQuery(
            query_id=None,
            query_text="select a, c from upstream2",
            upstreams=[DatasetUrn("redshift", "dev.public.upstream2").urn()],
            downstream=DatasetUrn("redshift", "dev.public.derived").urn(),
            timestamp=_ts(30),
            session_id="s1",
        ),
        ObservedQuery(
            query="create table baz as select a, 2*b as b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s3",
            timestamp=_ts(40),
        ),
    ]
    _assert_equivalent(items)


def test_equivalence_temp_producer_consumer_single_session() -> None:
    """A temp-table-creating query followed by a consumer in the same session.

    Mirrors the temp-table shapes in test_sql_aggregator.py. Proves the
    PartitionExecutor per-session ordering guarantee: the temp producer must be
    applied before the consumer is classified/parsed, so the consumer resolves
    the temp table and lineage flows through to bar.
    """
    items: List[_StreamItem] = [
        ObservedQuery(
            query="create temp table foo as select a, b+c as c from bar",
            default_db="dev",
            default_schema="public",
            session_id="session2",
            timestamp=_ts(10),
        ),
        ObservedQuery(
            query="create table foo_session2 as select * from foo",
            default_db="dev",
            default_schema="public",
            session_id="session2",
            timestamp=_ts(20),
        ),
    ]
    _assert_equivalent(items)


def test_equivalence_multiple_sessions_interleaved() -> None:
    """Queries from multiple sessions (including _MISSING_SESSION) interleaved."""
    items: List[_StreamItem] = [
        ObservedQuery(
            query="create temp table foo as select a, b+c as c from bar",
            default_db="dev",
            default_schema="public",
            session_id="sessionA",
            timestamp=_ts(10),
        ),
        ObservedQuery(
            query="create table foo as select a, 2*b as b from bar",
            default_db="dev",
            default_schema="public",
            session_id="sessionB",
            timestamp=_ts(15),
        ),
        # No session_id -> _MISSING_SESSION_ID
        ObservedQuery(
            query="insert into downstream (a, b) select a, b from upstream1",
            default_db="dev",
            default_schema="public",
            timestamp=_ts(18),
        ),
        ObservedQuery(
            query="create table foo_a as select * from foo",
            default_db="dev",
            default_schema="public",
            session_id="sessionA",
            timestamp=_ts(20),
        ),
        ObservedQuery(
            query="insert into downstream (a, c) select a, c from upstream2",
            default_db="dev",
            default_schema="public",
            timestamp=_ts(25),
        ),
        ObservedQuery(
            query="create table baz as select a, 2*b as b from bar",
            default_db="dev",
            default_schema="public",
            session_id="sessionB",
            timestamp=_ts(30),
        ),
    ]
    _assert_equivalent(items)


def test_fallback_to_serial_on_pool_creation_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ParallelSqlParser construction raises ParallelParserUnavailable the
    scope silently falls back to serial, produces output identical to a plain
    serial run, and the report records the fallback."""

    monkeypatch.setattr(
        "datahub.sql_parsing.sql_parsing_aggregator.ParallelSqlParser",
        lambda **kwargs: (_ for _ in ()).throw(
            ParallelParserUnavailable("test: multiprocessing unavailable")
        ),
    )

    items: List[_StreamItem] = [
        ObservedQuery(
            query="create table foo as select a, b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s1",
            timestamp=_ts(10),
        ),
        ObservedQuery(
            query="insert into downstream (a, b) select a, b from upstream1",
            default_db="dev",
            default_schema="public",
            session_id="s2",
            timestamp=_ts(20),
        ),
    ]

    serial = _run_serial(items)
    parallel, report = _run_parallel(items)
    assert [_mcp_key(m) for m in parallel] == [_mcp_key(m) for m in serial]
    assert report.sql_parsing_fell_back_to_serial is True


def _logged_query_tuples(aggregator: SqlParsingAggregator) -> list:
    """Snapshot the aggregator's logged queries as comparable tuples, sorted so
    that concurrent-append ordering does not affect the comparison."""
    entries = [
        (
            lq.query,
            lq.session_id,
            lq.timestamp,
            lq.user,
            lq.default_db,
            lq.default_schema,
        )
        for lq in aggregator._logged_queries
    ]
    return sorted(entries, key=repr)


@time_machine.travel(FROZEN_TIME, tick=False)
def _run_with_query_log(items: List[_StreamItem], *, use_parallel: bool) -> tuple:
    aggregator = _make_aggregator(
        use_parallel=use_parallel,
        query_log=QueryLogSetting.STORE_ALL,
    )
    if use_parallel:
        with aggregator.parallel_sql_parsing_scope():
            for item in items:
                aggregator.add(item)
    else:
        for item in items:
            aggregator.add(item)
    mcps = sorted(list(aggregator.gen_metadata()), key=_mcp_key)
    logged = _logged_query_tuples(aggregator)
    report = aggregator.report
    aggregator.close()
    return mcps, logged, report


def test_equivalence_with_query_logging() -> None:
    """Covers C1: with STORE_ALL query logging, the parallel path appends to the
    shared FileBackedList query log from worker threads. Serial and parallel must
    produce identical MCPs AND an identical set of logged queries (no lost or
    corrupted log entries)."""
    items: List[_StreamItem] = [
        ObservedQuery(
            query="create table foo as select a, b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s1",
            timestamp=_ts(10),
        ),
        ObservedQuery(
            query="insert into downstream (a, b) select a, b from upstream1",
            default_db="dev",
            default_schema="public",
            session_id="s2",
            timestamp=_ts(20),
        ),
        ObservedQuery(
            query="insert into downstream (a, c) select a, c from upstream2",
            default_db="dev",
            default_schema="public",
            session_id="s3",
            timestamp=_ts(25),
        ),
        ObservedQuery(
            query="create table baz as select a, 2*b as b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s4",
            timestamp=_ts(40),
        ),
    ]

    serial_mcps, serial_logged, serial_report = _run_with_query_log(
        items, use_parallel=False
    )
    parallel_mcps, parallel_logged, parallel_report = _run_with_query_log(
        items, use_parallel=True
    )

    assert [_mcp_key(m) for m in parallel_mcps] == [_mcp_key(m) for m in serial_mcps]
    assert len(parallel_logged) == len(serial_logged)
    assert parallel_logged == serial_logged
    assert parallel_report.num_sql_parsed == serial_report.num_sql_parsed


def _make_stress_items() -> List[_StreamItem]:
    """Dozens of sessions, several queries each: a mix of temp producer/consumer
    sessions and plain non-temp sessions, interleaved by timestamp."""
    items: List[_StreamItem] = []
    ts = 0
    num_temp_sessions = 12
    num_plain_sessions = 12

    # Interleave: for each "round", emit one query from several sessions.
    for round_idx in range(3):
        for s in range(num_plain_sessions):
            ts += 1
            # Distinct output table per (session, round) so there are no
            # cross-session query-fingerprint collisions whose "latest timestamp"
            # would be apply-order-dependent (that would make even a correct
            # serial run non-deterministic vs parallel). Lineage still flows from
            # a shared upstream, exercising the real parse path.
            upstream = "upstream1" if round_idx % 2 == 0 else "upstream2"
            cols = "a, b" if round_idx % 2 == 0 else "a, c"
            items.append(
                ObservedQuery(
                    query=(
                        f"create table out_{s}_{round_idx} as "
                        f"select {cols} from {upstream}"
                    ),
                    default_db="dev",
                    default_schema="public",
                    session_id=f"plain_{s}",
                    timestamp=_ts(ts),
                )
            )
        for s in range(num_temp_sessions):
            ts += 1
            if round_idx == 0:
                # Temp producer.
                items.append(
                    ObservedQuery(
                        query="create temp table foo as select a, b+c as c from bar",
                        default_db="dev",
                        default_schema="public",
                        session_id=f"temp_{s}",
                        timestamp=_ts(ts),
                    )
                )
            else:
                # Temp consumer.
                items.append(
                    ObservedQuery(
                        query=f"create table foo_{s}_{round_idx} as select * from foo",
                        default_db="dev",
                        default_schema="public",
                        session_id=f"temp_{s}",
                        timestamp=_ts(ts),
                    )
                )
    return items


@pytest.mark.parametrize("run_idx", range(3))
def test_high_volume_many_sessions_stress(run_idx: int) -> None:
    """Covers C2/I1/I2: a higher-volume, many-session interleaved stream with a
    mix of temp and non-temp sessions and workers>=2. Asserts:

    - serial-vs-parallel MCP equivalence (no lineage loss),
    - report.num_sql_parsed matches the serial count (no lost/double accounting),
    - report.num_queries_parsed_in_parallel equals the number of non-temp
      observed queries that were actually pool-parsed (> 0), proving the
      observability contract.

    Repeated a few times via parametrization since races are timing-dependent.
    """
    items = _make_stress_items()

    serial = _run_serial(items)
    parallel, report = _run_parallel(items, workers=4)

    assert [_mcp_key(m) for m in parallel] == [_mcp_key(m) for m in serial]

    serial_report = _make_aggregator(use_parallel=False)
    for item in items:
        serial_report.add(item)
    list(serial_report.gen_metadata())
    expected_num_sql_parsed = serial_report.report.num_sql_parsed
    serial_report.close()

    assert report.num_sql_parsed == expected_num_sql_parsed

    # A query is pool-parsed iff its session had no temp tables registered at the
    # time it was classified. That is every plain_* query, plus the temp
    # producer (the "create temp table foo" runs before the temp table exists in
    # its session). The temp *consumers* parse inline because their session then
    # holds an in-memory temp schema that cannot ship to a worker.
    num_pool_parsed_expected = sum(
        1
        for item in items
        if isinstance(item, ObservedQuery)
        and (
            (item.session_id or "").startswith("plain_")
            or item.query.strip().lower().startswith("create temp table")
        )
    )
    assert report.num_queries_parsed_in_parallel > 0
    assert report.num_queries_parsed_in_parallel == num_pool_parsed_expected


def test_add_to_query_map_latest_timestamp_is_max_not_last_written() -> None:
    """_add_to_query_map must keep the maximum timestamp regardless of insertion order.

    When the parallel path applies per-session results, the same query fingerprint
    may be merged in a non-chronological order across sessions.  Before the fix,
    ``current.latest_timestamp = new.latest_timestamp or current.latest_timestamp``
    was last-writer-wins, so a later write with an EARLIER timestamp silently
    replaced the correct (higher) value — a serial-vs-parallel divergence.

    Regression test: insert LATER timestamp first, then EARLIER; assert the stored
    value is the LATER one (the maximum).
    """
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
    )

    later_ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    earlier_ts = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)

    fingerprint = "test-fingerprint-order-independence"

    first = QueryMetadata(
        query_id=fingerprint,
        formatted_query_string="SELECT 1",
        session_id="session_a",
        query_type=QueryType.UNKNOWN,
        lineage_type=models.DatasetLineageTypeClass.TRANSFORMED,
        latest_timestamp=later_ts,
        actor=None,
        upstreams=[],
        column_lineage=[],
        column_usage={},
        confidence_score=1.0,
    )
    second = QueryMetadata(
        query_id=fingerprint,
        formatted_query_string="SELECT 1",
        session_id="session_b",
        query_type=QueryType.UNKNOWN,
        lineage_type=models.DatasetLineageTypeClass.TRANSFORMED,
        latest_timestamp=earlier_ts,
        actor=None,
        upstreams=[],
        column_lineage=[],
        column_usage={},
        confidence_score=1.0,
    )

    # Insert later-timestamp first, then earlier-timestamp second.
    # After the fix the stored value must be the maximum (later_ts).
    aggregator._add_to_query_map(first)
    aggregator._add_to_query_map(second)

    stored = aggregator._query_map[fingerprint]
    assert stored.latest_timestamp == later_ts, (
        f"Expected the maximum timestamp ({later_ts}), "
        f"got {stored.latest_timestamp} — last-writer-wins bug is present."
    )

    aggregator.close()


def test_feature_off_is_default() -> None:
    """Feature is off by default and the parallel scope is a no-op."""
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
    )
    assert aggregator.report.sql_parsing_parallel_enabled is False
    # The scope must be a no-op when the feature is off.
    with aggregator.parallel_sql_parsing_scope():
        aggregator.add_observed_query(
            ObservedQuery(
                query="create table foo as select a, b from bar",
                default_db="dev",
                default_schema="public",
            )
        )
    mcps = list(aggregator.gen_metadata())
    aggregator.close()
    assert len(mcps) > 0
    assert aggregator.report.sql_parsing_parallel_enabled is False


# ---------------------------------------------------------------------------
# CPU-detection / clamping tests
# ---------------------------------------------------------------------------

_SIMPLE_ITEMS: List[_StreamItem] = [
    ObservedQuery(
        query="create table foo as select a, b from bar",
        default_db="dev",
        default_schema="public",
        session_id="s1",
        timestamp=datetime(2024, 2, 6, 1, 23, 45, tzinfo=timezone.utc),
    ),
    ObservedQuery(
        query="insert into downstream (a, b) select a, b from upstream1",
        default_db="dev",
        default_schema="public",
        session_id="s2",
        timestamp=datetime(2024, 2, 6, 1, 23, 46, tzinfo=timezone.utc),
    ),
]


@time_machine.travel(FROZEN_TIME, tick=False)
def test_clamped_worker_count_report_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """When sql_parsing_workers exceeds detected capacity the pool is built with
    the clamped count, report.sql_parsing_workers_clamped is True, and
    report.sql_parsing_cpu_detected reflects the detection result.

    Output must still equal the serial run (correctness not affected by clamping).
    """
    detected = 2

    monkeypatch.setattr(
        "datahub.utilities.cpu_detection.get_available_cpu_count",
        lambda: detected,
    )

    # Request far more workers than the detected capacity allows.
    requested_workers = 16

    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
        use_parallel_sql_parsing=True,
        sql_parsing_workers=requested_workers,
    )
    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.bar").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )
    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.upstream1").urn(),
        {"a": "int", "b": "int"},
    )

    with aggregator.parallel_sql_parsing_scope():
        for item in _SIMPLE_ITEMS:
            aggregator.add(item)

    parallel_mcps = sorted(list(aggregator.gen_metadata()), key=_mcp_key)
    report = aggregator.report
    aggregator.close()

    # Workers clamped: detected=2 → usable=1 (reserve 1) → workers=1 (clamped from 16).
    assert report.sql_parsing_workers_clamped is True
    assert report.sql_parsing_cpu_detected == detected

    # Parallel path still produces correct output.
    serial_mcps = _run_serial(_SIMPLE_ITEMS)
    assert [_mcp_key(m) for m in parallel_mcps] == [_mcp_key(m) for m in serial_mcps]


@time_machine.travel(FROZEN_TIME, tick=False)
def test_detection_serial_fallback_one_cpu(monkeypatch: pytest.MonkeyPatch) -> None:
    """When fewer than 2 CPUs are detected the scope falls back to serial execution.

    Asserts:
    - output equals a plain serial run,
    - report.sql_parsing_fell_back_to_serial is True,
    - report.sql_parsing_cpu_detected reflects the single-CPU detection.
    """
    monkeypatch.setattr(
        "datahub.utilities.cpu_detection.get_available_cpu_count",
        lambda: 1,
    )

    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
        use_parallel_sql_parsing=True,
        sql_parsing_workers=None,
    )
    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.bar").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )
    aggregator._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.upstream1").urn(),
        {"a": "int", "b": "int"},
    )

    with aggregator.parallel_sql_parsing_scope():
        for item in _SIMPLE_ITEMS:
            aggregator.add(item)

    parallel_mcps = sorted(list(aggregator.gen_metadata()), key=_mcp_key)
    report = aggregator.report
    aggregator.close()

    assert report.sql_parsing_fell_back_to_serial is True
    assert report.sql_parsing_cpu_detected == 1

    serial_mcps = _run_serial(_SIMPLE_ITEMS)
    assert [_mcp_key(m) for m in parallel_mcps] == [_mcp_key(m) for m in serial_mcps]
