"""Equivalence tests for per-session-safe parallel SQL parsing in SqlParsingAggregator.

The correctness bar: parallel output MUST be byte-identical to serial output
(no lineage loss). Each test runs the same scripted stream of queries through a
serial aggregator (feature off) and a parallel aggregator (feature on, wrapped in
``parallel_sql_parsing_scope()``), then asserts the emitted MCPs are equal after
normalizing ordering.
"""

import concurrent.futures.process
import dataclasses
import itertools
import json
from datetime import datetime, timezone
from typing import List, Optional, Union
from unittest import mock

import pytest
import time_machine

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn, DatasetUrn
from datahub.sql_parsing.parallel_sql_parser import (
    ParallelParserUnavailable,
    ParallelSqlParser,
    ParseOutcome,
)
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    PreparsedQuery,
    QueryLogSetting,
    QueryMetadata,
    SqlParsingAggregator,
    TableRename,
    TableSwap,
)
from datahub.sql_parsing.sql_parsing_common import QueryType

_StreamItem = Union[ObservedQuery, PreparsedQuery, TableRename, TableSwap]

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

    reason = "test: multiprocessing unavailable"
    monkeypatch.setattr(
        "datahub.sql_parsing.sql_parsing_aggregator.ParallelSqlParser",
        lambda **kwargs: (_ for _ in ()).throw(ParallelParserUnavailable(reason)),
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
    assert report.sql_parsing_fell_back_to_serial_reason == reason


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


@time_machine.travel(FROZEN_TIME, tick=False)
def test_teardown_without_scope_exit_matches_scoped_run() -> None:
    """A connector that forgets to exit the parallel scope (never runs the
    ``with`` block's __exit__) must still produce correct output and leak no
    worker pool: close() defensively tears down via _teardown_parallel (R6)."""
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

    expected = _run_serial(items)

    aggregator = _make_aggregator(use_parallel=True, workers=2)
    scope = aggregator.parallel_sql_parsing_scope()
    scope.__enter__()
    for item in items:
        aggregator.add(item)
    # Deliberately do NOT call scope.__exit__ — simulate a forgotten `with` exit.
    mcps = sorted(list(aggregator.gen_metadata()), key=_mcp_key)

    assert [_mcp_key(m) for m in mcps] == [_mcp_key(m) for m in expected]

    # close() must defensively tear down the still-open parallel machinery.
    aggregator.close()
    assert aggregator._partition_executor is None
    assert aggregator._parallel_parser is None
    assert aggregator._parallel_active is False


def test_equivalence_rare_item_interleaved() -> None:
    """A TableRename (and a TableSwap) interleaved with observed queries in the
    same session. Exercises the flush-then-apply rare-item path: parallel output
    must equal serial (R6)."""
    rename = TableRename(
        original_urn=DatasetUrn("redshift", "dev.public.bar").urn(),
        new_urn=DatasetUrn("redshift", "dev.public.bar_renamed").urn(),
        session_id="s1",
        timestamp=_ts(15),
    )
    swap = TableSwap(
        urn1=DatasetUrn("redshift", "dev.public.upstream1").urn(),
        urn2=DatasetUrn("redshift", "dev.public.upstream2").urn(),
        session_id="s1",
        timestamp=_ts(25),
    )
    items: List[_StreamItem] = [
        ObservedQuery(
            query="create table foo as select a, b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s1",
            timestamp=_ts(10),
        ),
        rename,
        ObservedQuery(
            query="insert into downstream (a, b) select a, b from upstream1",
            default_db="dev",
            default_schema="public",
            session_id="s1",
            timestamp=_ts(20),
        ),
        swap,
        ObservedQuery(
            query="create table baz as select a, 2*b as b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s1",
            timestamp=_ts(30),
        ),
    ]
    _assert_equivalent(items)


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


def _query_statement_values(mcps: list) -> List[str]:
    """Extract every QueryProperties.statement.value (the formatted query text)
    emitted across the MCPs, sorted for order-independent comparison."""
    values: List[str] = []
    for mcp in mcps:
        aspect = getattr(mcp, "aspect", None)
        if isinstance(aspect, models.QueryPropertiesClass):
            values.append(aspect.statement.value)
    return sorted(values)


@time_machine.travel(FROZEN_TIME, tick=False)
def test_equivalence_formatted_query_text_identical() -> None:
    """The formatted query text in the emitted query aspects must be byte-identical
    between serial (formats on main thread) and parallel (formats in worker) with
    format_queries=True (the default)."""
    items = _observed_no_temp_items()

    serial = _run_serial(items)
    parallel, _ = _run_parallel(items)

    serial_values = _query_statement_values(serial)
    parallel_values = _query_statement_values(parallel)

    assert serial_values, "expected at least one query statement aspect"
    assert parallel_values == serial_values
    # Sanity: formatting actually happened (multi-line pretty-print), so this is a
    # meaningful assertion rather than comparing two unformatted strings.
    assert any("\n" in value for value in serial_values)


def _observed_no_temp_items() -> List[_StreamItem]:
    return [
        ObservedQuery(
            query="create table foo as select a, b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s1",
            timestamp=_ts(10),
        ),
        ObservedQuery(
            query="create table baz as select a, 2*b as b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s2",
            timestamp=_ts(20),
        ),
    ]


@time_machine.travel(FROZEN_TIME, tick=False)
def test_parallel_apply_does_not_reformat_on_main_thread() -> None:
    """On the parallel (non-temp) path the worker pre-formats the query, so the
    main-thread _maybe_format_query must NOT be called again for those queries."""
    items = _observed_no_temp_items()

    aggregator = _make_aggregator(use_parallel=True, workers=2)
    assert aggregator.format_queries is True
    with mock.patch.object(
        aggregator,
        "_maybe_format_query",
        wraps=aggregator._maybe_format_query,
    ) as spy:
        with aggregator.parallel_sql_parsing_scope():
            for item in items:
                aggregator.add(item)
        list(aggregator.gen_metadata())
    aggregator.close()

    assert spy.call_count == 0


@time_machine.travel(FROZEN_TIME, tick=False)
def test_serial_apply_still_formats_on_main_thread() -> None:
    """The serial path must still format on the main thread (behavior unchanged)."""
    items = _observed_no_temp_items()

    aggregator = _make_aggregator(use_parallel=False)
    assert aggregator.format_queries is True
    with mock.patch.object(
        aggregator,
        "_maybe_format_query",
        wraps=aggregator._maybe_format_query,
    ) as spy:
        for item in items:
            aggregator.add(item)
        list(aggregator.gen_metadata())
    aggregator.close()

    assert spy.call_count > 0


def test_add_to_query_map_actor_tracks_latest_timestamp() -> None:
    """actor/session_id must follow the record with the later timestamp,
    regardless of insertion order.

    Same fingerprint added with (later-ts, actorA) then (earlier-ts, actorB)
    must keep actorA both ways.
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
    actor_a = CorpUserUrn("actor_a")
    actor_b = CorpUserUrn("actor_b")

    fingerprint = "test-actor-order-independence"

    def _make_meta(ts, actor, session):
        return QueryMetadata(
            query_id=fingerprint,
            formatted_query_string="SELECT 1",
            session_id=session,
            query_type=QueryType.UNKNOWN,
            lineage_type=models.DatasetLineageTypeClass.TRANSFORMED,
            latest_timestamp=ts,
            actor=actor,
            upstreams=[],
            column_lineage=[],
            column_usage={},
            confidence_score=1.0,
        )

    # Forward order: later first, earlier second
    aggregator._add_to_query_map(_make_meta(later_ts, actor_a, "session_a"))
    aggregator._add_to_query_map(_make_meta(earlier_ts, actor_b, "session_b"))
    stored = aggregator._query_map[fingerprint]
    assert stored.actor == actor_a, "actor should track the later-timestamp record"

    # Now clear and test reverse order: earlier first, later second
    del aggregator._query_map[fingerprint]
    aggregator._add_to_query_map(_make_meta(earlier_ts, actor_b, "session_b"))
    aggregator._add_to_query_map(_make_meta(later_ts, actor_a, "session_a"))
    stored = aggregator._query_map[fingerprint]
    assert stored.actor == actor_a, (
        "actor should track the later-timestamp record (reversed order)"
    )

    aggregator.close()


def _query_meta(
    *,
    fingerprint: str,
    formatted: str = "SELECT 1",
    session: str = "s0",
    ts: Optional[datetime],
    actor: Optional[Union[CorpUserUrn, CorpGroupUrn]],
    used_temp_tables: bool = False,
    upstreams: Optional[List[str]] = None,
) -> QueryMetadata:
    return QueryMetadata(
        query_id=fingerprint,
        formatted_query_string=formatted,
        session_id=session,
        query_type=QueryType.UNKNOWN,
        lineage_type=models.DatasetLineageTypeClass.TRANSFORMED,
        latest_timestamp=ts,
        actor=actor,
        upstreams=upstreams if upstreams is not None else [],
        column_lineage=[],
        column_usage={},
        confidence_score=1.0,
        used_temp_tables=used_temp_tables,
    )


def _reduce_query_map(metas: List[QueryMetadata]) -> QueryMetadata:
    """Feed ``metas`` (all sharing one fingerprint) through a fresh aggregator's
    ``_add_to_query_map`` in the given order and return the stored record."""
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
    )
    for meta in metas:
        aggregator._add_to_query_map(dataclasses.replace(meta))
    return aggregator._query_map[metas[0].query_id]


def test_add_to_query_map_actor_selection_is_order_and_grouping_independent() -> None:
    """BLOCKER regression: actor selection must be associative.

    Records share a fingerprint. The timestamp winner (A) has actor=None while
    two earlier records carry distinct actors. Serial semantics attribute the
    query to the actor of the latest-timestamp record that HAS an actor (B).
    A previous coalescing rule (``winner.actor or loser.actor``) fed the
    synthesized actor back into the winner ranking, so three-way grouping could
    yield B's actor in one order and C's in another. Every permutation must now
    resolve to the same actor.
    """
    fp = "assoc-actor"
    actor_b = CorpUserUrn("user_b")
    actor_c = CorpUserUrn("user_c")
    ts_a = datetime(2024, 1, 3, tzinfo=timezone.utc)  # latest, no actor
    ts_b = datetime(2024, 1, 2, tzinfo=timezone.utc)  # middle, actor_b
    ts_c = datetime(2024, 1, 1, tzinfo=timezone.utc)  # earliest, actor_c
    a = _query_meta(fingerprint=fp, ts=ts_a, actor=None)
    b = _query_meta(fingerprint=fp, ts=ts_b, actor=actor_b)
    c = _query_meta(fingerprint=fp, ts=ts_c, actor=actor_c)

    def _order_key(perm: tuple) -> tuple:
        return tuple(m.latest_timestamp.day if m.latest_timestamp else 0 for m in perm)

    results = {
        _order_key(perm): _reduce_query_map(list(perm)).actor
        for perm in itertools.permutations([a, b, c])
    }
    assert set(results.values()) == {actor_b}, (
        f"actor selection depends on merge order: {results}"
    )


def test_add_to_query_map_temp_lineage_authority_is_commutative() -> None:
    """BLOCKER regression: temp-table lineage authority must be symmetric.

    When one record used temp tables and the other did not, the temp-derived
    lineage is authoritative regardless of arrival order (the old one-directional
    early-return only preserved it when the EXISTING record was the temp one)."""
    fp = "commute-temp"
    temp_up = ["urn:li:dataset:(urn:li:dataPlatform:redshift,temp_up,PROD)"]
    plain_up = ["urn:li:dataset:(urn:li:dataPlatform:redshift,plain_up,PROD)"]
    older = datetime(2024, 1, 1, tzinfo=timezone.utc)
    newer = datetime(2024, 1, 2, tzinfo=timezone.utc)

    # Older temp record vs newer non-temp record: temp lineage wins both orders.
    temp_older = _query_meta(
        fingerprint=fp, ts=older, actor=None, used_temp_tables=True, upstreams=temp_up
    )
    plain_newer = _query_meta(
        fingerprint=fp, ts=newer, actor=None, used_temp_tables=False, upstreams=plain_up
    )
    assert _reduce_query_map([temp_older, plain_newer]).upstreams == temp_up
    assert _reduce_query_map([plain_newer, temp_older]).upstreams == temp_up

    # Newer temp record vs older non-temp record: temp lineage still wins.
    temp_newer = _query_meta(
        fingerprint=fp, ts=newer, actor=None, used_temp_tables=True, upstreams=temp_up
    )
    plain_older = _query_meta(
        fingerprint=fp, ts=older, actor=None, used_temp_tables=False, upstreams=plain_up
    )
    assert _reduce_query_map([temp_newer, plain_older]).upstreams == temp_up
    assert _reduce_query_map([plain_older, temp_newer]).upstreams == temp_up


def test_add_to_query_map_does_not_drop_known_actor_for_none() -> None:
    """The later-timestamp record having actor=None must NOT wipe a
    previously-known actor.

    This is the serial-path regression: queries arrive in ascending-timestamp
    order, so a trailing actor-less duplicate is always the timestamp winner. If
    the winner's actor overwrites unconditionally, the known actor is lost and
    Redshift usage/lineage actors regress to the default ingestion user.

    The merged actor must coalesce (winner.actor or loser.actor), so both
    insertion orders keep the real actor.
    """
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
    )

    earlier_ts = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
    later_ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    real_actor = CorpUserUrn("real_actor")
    fingerprint = "test-actor-not-dropped"

    # Ascending order (serial): known actor first, actor-less winner second.
    aggregator._add_to_query_map(
        _query_meta(fingerprint=fingerprint, ts=earlier_ts, actor=real_actor)
    )
    aggregator._add_to_query_map(
        _query_meta(fingerprint=fingerprint, ts=later_ts, actor=None)
    )
    stored = aggregator._query_map[fingerprint]
    assert stored.actor == real_actor, (
        "known actor must not be dropped when the timestamp winner has actor=None"
    )

    # Reversed order: actor-less winner first, known actor second.
    del aggregator._query_map[fingerprint]
    aggregator._add_to_query_map(
        _query_meta(fingerprint=fingerprint, ts=later_ts, actor=None)
    )
    aggregator._add_to_query_map(
        _query_meta(fingerprint=fingerprint, ts=earlier_ts, actor=real_actor)
    )
    stored = aggregator._query_map[fingerprint]
    assert stored.actor == real_actor, (
        "known actor must not be dropped regardless of insertion order"
    )

    aggregator.close()


def test_add_to_query_map_representative_text_is_order_independent() -> None:
    """The stored formatted_query_string must deterministically come from the
    MAX-timestamp record, not the last-written one.

    Query fingerprints generalize literals, so two records with the same
    fingerprint can carry different formatted text. The stored text must be
    tied to the timestamp winner so serial and parallel produce byte-identical
    output regardless of which cross-session task finishes last.
    """
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
    )

    earlier_ts = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
    later_ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    fingerprint = "test-representative-text"
    earlier_text = "SELECT * FROM t WHERE ts = 34"
    later_text = "SELECT * FROM t WHERE ts = 38"

    def _earlier() -> QueryMetadata:
        return _query_meta(
            fingerprint=fingerprint, formatted=earlier_text, ts=earlier_ts, actor=None
        )

    def _later() -> QueryMetadata:
        return _query_meta(
            fingerprint=fingerprint, formatted=later_text, ts=later_ts, actor=None
        )

    def _run(order: List[QueryMetadata]) -> str:
        if fingerprint in aggregator._query_map:
            del aggregator._query_map[fingerprint]
        for meta in order:
            aggregator._add_to_query_map(meta)
        return aggregator._query_map[fingerprint].formatted_query_string

    # Build fresh metadata per run: the map stores a reference on fresh insert
    # and merges mutate it in place, so reusing objects across runs would alias.
    ascending = _run([_earlier(), _later()])
    descending = _run([_later(), _earlier()])

    assert ascending == later_text, (
        "stored text must be the max-timestamp record's text (ascending order)"
    )
    assert descending == later_text, (
        "stored text must be the max-timestamp record's text (descending order)"
    )

    aggregator.close()


def test_add_to_query_map_equal_timestamp_tie_break_is_deterministic() -> None:
    """When timestamps are equal (or both None), the winner is chosen by a
    stable tie-break (lexicographically-greater formatted text), so the merged
    result is identical regardless of insertion order."""
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
    )

    same_ts = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    fingerprint = "test-tie-break"
    text_a = "SELECT a FROM t"
    text_b = "SELECT b FROM t"  # lexicographically greater -> deterministic winner

    def _a() -> QueryMetadata:
        return _query_meta(
            fingerprint=fingerprint, formatted=text_a, ts=same_ts, actor=None
        )

    def _b() -> QueryMetadata:
        return _query_meta(
            fingerprint=fingerprint, formatted=text_b, ts=same_ts, actor=None
        )

    def _run(order: List[QueryMetadata]) -> str:
        if fingerprint in aggregator._query_map:
            del aggregator._query_map[fingerprint]
        for meta in order:
            aggregator._add_to_query_map(meta)
        return aggregator._query_map[fingerprint].formatted_query_string

    forward = _run([_a(), _b()])
    reverse = _run([_b(), _a()])

    assert forward == reverse, "equal-timestamp merge must be order-independent"
    assert forward == text_b, (
        "tie-break winner must be the lexicographically greater text"
    )

    aggregator.close()


# ---------------------------------------------------------------------------
# Worker / pool infra-failure handling. These protect the no-lineage-loss
# guarantee: when a worker process dies or the pool cannot be initialized, the
# aggregator must reparse the affected query inline (serially) rather than
# silently dropping it, and must fall the rest of the run back to serial.
# ---------------------------------------------------------------------------


def _infra_error_items() -> List[_StreamItem]:
    """A mix of non-temp observed queries across sessions (no temp tables, so
    every one would normally go to a worker via parse_one)."""
    return [
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
            timestamp=_ts(30),
        ),
        ObservedQuery(
            query="create table baz as select a, 2*b as b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s1",
            timestamp=_ts(40),
        ),
    ]


@time_machine.travel(FROZEN_TIME, tick=False)
def test_worker_infra_failure_falls_back_to_serial_no_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``parse_one`` returns an infra-failure outcome (worker died /
    BrokenProcessPool), the aggregator must reparse that query inline and mark
    the pool broken. Output must be byte-identical to a fully-serial run, and
    infra failures must NOT inflate ``num_observed_queries_failed``."""
    items = _infra_error_items()
    serial = _run_serial(items)

    real_parse_one = ParallelSqlParser.parse_one

    def failing_parse_one(self: ParallelSqlParser, task: object) -> ParseOutcome:
        # Simulate a dead worker / broken pool on the very first parse and
        # stay broken thereafter (sticky), exactly like a real pool death.
        self.pool_broke.set()
        return ParseOutcome(
            result=None,
            error=repr(
                concurrent.futures.process.BrokenProcessPool("simulated worker death")
            ),
        )

    monkeypatch.setattr(ParallelSqlParser, "parse_one", failing_parse_one)

    aggregator = _make_aggregator(use_parallel=True, workers=2)
    with aggregator.parallel_sql_parsing_scope():
        for item in items:
            aggregator.add(item)
    parallel = sorted(list(aggregator.gen_metadata()), key=_mcp_key)
    report = aggregator.report
    aggregator.close()

    assert [_mcp_key(m) for m in parallel] == [_mcp_key(m) for m in serial]
    assert report.sql_parsing_pool_broke is True
    # Infra failures were reparsed inline, not counted as parse failures.
    assert report.num_observed_queries_failed == 0

    # Sanity: real_parse_one still points at the original (monkeypatch restores).
    assert real_parse_one is not None


@time_machine.travel(FROZEN_TIME, tick=False)
def test_pool_init_failure_falls_back_to_serial_no_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pool-init failure surfaces as a BrokenProcessPool raised from the first
    ``parse_one`` (outside the constructor's ParallelParserUnavailable path).
    The whole run must fall back to serial with identical output and no drops."""
    items = _infra_error_items()
    serial = _run_serial(items)

    def raising_parse_one(self: ParallelSqlParser, task: object) -> ParseOutcome:
        # Mirror the real parse_one: on BrokenProcessPool it sets pool_broke and
        # returns an error outcome rather than propagating.
        self.pool_broke.set()
        return ParseOutcome(
            result=None,
            error=repr(
                concurrent.futures.process.BrokenProcessPool("worker init failed")
            ),
        )

    monkeypatch.setattr(ParallelSqlParser, "parse_one", raising_parse_one)

    aggregator = _make_aggregator(use_parallel=True, workers=2)
    with aggregator.parallel_sql_parsing_scope():
        for item in items:
            aggregator.add(item)
    parallel = sorted(list(aggregator.gen_metadata()), key=_mcp_key)
    report = aggregator.report
    aggregator.close()

    assert [_mcp_key(m) for m in parallel] == [_mcp_key(m) for m in serial]
    assert report.sql_parsing_pool_broke is True
    assert report.num_observed_queries_failed == 0


@time_machine.travel(FROZEN_TIME, tick=False)
def test_worker_parse_time_metric_populated() -> None:
    """After a real parallel run, worker parse time must be visible in the
    dedicated accumulator (it was previously invisible to sql_parsing_timer)."""
    items = _observed_no_temp_items()

    aggregator = _make_aggregator(use_parallel=True, workers=2)
    with aggregator.parallel_sql_parsing_scope():
        for item in items:
            aggregator.add(item)
    list(aggregator.gen_metadata())
    report = aggregator.report
    aggregator.close()

    assert report.num_queries_parsed_in_parallel > 0
    assert report.parallel_sql_parsing_time_seconds > 0.0


@time_machine.travel(FROZEN_TIME, tick=False)
def test_submit_time_broken_pool_falls_back_to_serial_no_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """H1: when the executor's ``submit`` itself raises BrokenProcessPool (the
    pool is already broken at dispatch time), parse_one must convert it to an
    error outcome + set pool_broke — NOT let it propagate to
    _process_observed_task's outer except (which would count it as a parse
    failure and skip inline recovery). Output must be identical to serial and no
    query dropped."""
    from concurrent.futures.process import BrokenProcessPool

    items = _infra_error_items()
    serial = _run_serial(items)

    real_submit = ParallelSqlParser._ensure_executor

    def _patched_ensure(self: ParallelSqlParser) -> object:
        executor = real_submit(self)
        # Patch submit on the live executor exactly once so the first dispatch
        # hits a submit-time BrokenProcessPool, then stays broken (sticky).
        if getattr(executor, "_h1_patched", False) is False:

            def _boom_submit(*args: object, **kwargs: object) -> object:
                raise BrokenProcessPool("boom at submit")

            executor.submit = _boom_submit  # type: ignore[assignment]
            executor._h1_patched = True  # type: ignore[attr-defined]
        return executor

    monkeypatch.setattr(ParallelSqlParser, "_ensure_executor", _patched_ensure)

    aggregator = _make_aggregator(use_parallel=True, workers=2)
    with aggregator.parallel_sql_parsing_scope():
        for item in items:
            aggregator.add(item)
    parallel = sorted(list(aggregator.gen_metadata()), key=_mcp_key)
    report = aggregator.report
    aggregator.close()

    assert [_mcp_key(m) for m in parallel] == [_mcp_key(m) for m in serial]
    assert report.sql_parsing_pool_broke is True
    # Submit-time infra failure must be reparsed inline, not counted as a parse
    # failure, and must not land in the infra-failure bucket either (parse_one
    # swallowed it into an error outcome, so recovery is clean).
    assert report.num_observed_queries_failed == 0


# ---------------------------------------------------------------------------
# H2 — mixed naive/aware timestamps must not crash the merge (serial path too)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("use_parallel", [False, True])
def test_add_to_query_map_mixed_tz_timestamps_no_crash(use_parallel: bool) -> None:
    """A duplicate fingerprint whose two records carry a naive datetime and a
    UTC-aware datetime must merge without a TypeError, in BOTH insertion orders
    and BOTH serial and parallel modes. The later instant wins deterministically."""
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
        use_parallel_sql_parsing=use_parallel,
        sql_parsing_workers=2 if use_parallel else None,
    )

    # One naive datetime and one UTC-aware datetime for the SAME fingerprint.
    # Comparing them with raw `>` raises TypeError; make_ts_millis normalizes
    # both to epoch millis so the merge is safe. They are set years apart so the
    # ordering (aware-2024 later than naive-2020) is unambiguous regardless of
    # the machine's local timezone that make_ts_millis uses for the naive value.
    naive_earlier = datetime(2020, 1, 1, 6, 0, 0)  # naive
    aware_later = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)  # aware
    fingerprint = "test-mixed-tz"
    naive_text = "SELECT naive"
    aware_text = "SELECT aware"

    def _naive() -> QueryMetadata:
        return _query_meta(
            fingerprint=fingerprint, formatted=naive_text, ts=naive_earlier, actor=None
        )

    def _aware() -> QueryMetadata:
        return _query_meta(
            fingerprint=fingerprint, formatted=aware_text, ts=aware_later, actor=None
        )

    def _run(order: List[QueryMetadata]) -> QueryMetadata:
        if fingerprint in aggregator._query_map:
            del aggregator._query_map[fingerprint]
        for meta in order:
            aggregator._add_to_query_map(meta)  # must not raise TypeError
        return aggregator._query_map[fingerprint]

    forward = _run([_naive(), _aware()])
    reverse = _run([_aware(), _naive()])

    # The later instant (aware 12:00) wins deterministically in both orders.
    assert forward.formatted_query_string == aware_text
    assert reverse.formatted_query_string == aware_text
    assert forward.latest_timestamp == aware_later
    assert reverse.latest_timestamp == aware_later

    aggregator.close()


# ---------------------------------------------------------------------------
# M1 — tie-break must be TOTAL: equal ts + equal text picks a stable winner
# ---------------------------------------------------------------------------


def test_add_to_query_map_total_tie_break_on_equal_ts_and_text() -> None:
    """When two cross-session records share the same timestamp AND identical
    formatted text, actor/session selection must still be deterministic (driven
    by session_id then actor), not by arrival order."""
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
    )

    same_ts = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    same_text = "SELECT a FROM t"
    fingerprint = "test-total-tie-break"
    actor_a = CorpUserUrn("actor_a")
    actor_b = CorpUserUrn("actor_b")

    def _a() -> QueryMetadata:
        return _query_meta(
            fingerprint=fingerprint,
            formatted=same_text,
            session="session_a",
            ts=same_ts,
            actor=actor_a,
        )

    def _b() -> QueryMetadata:
        return _query_meta(
            fingerprint=fingerprint,
            formatted=same_text,
            session="session_b",
            ts=same_ts,
            actor=actor_b,
        )

    def _run(order: List[QueryMetadata]) -> QueryMetadata:
        if fingerprint in aggregator._query_map:
            del aggregator._query_map[fingerprint]
        for meta in order:
            aggregator._add_to_query_map(meta)
        return aggregator._query_map[fingerprint]

    forward = _run([_a(), _b()])
    reverse = _run([_b(), _a()])

    assert forward.actor == reverse.actor, "actor must be arrival-order independent"
    assert forward.session_id == reverse.session_id, (
        "session_id must be arrival-order independent"
    )

    aggregator.close()


# ---------------------------------------------------------------------------
# W2 — inline-reparse failure counted under the infra bucket, not parse failure
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME, tick=False)
def test_inline_reparse_failure_counts_as_infra_not_parse_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If ``_reparse_observed_inline`` itself raises (e.g. the pool degraded and
    the inline recovery hit an infra error), it must be recorded under the new
    infra-failure counter, NOT inflate ``num_observed_queries_failed``."""
    items = _infra_error_items()

    # Force every task down the inline branch by pretending the pool is broken,
    # then make the inline reparse raise.
    monkeypatch.setattr(SqlParsingAggregator, "_pool_is_broken", lambda self: True)

    def _boom_inline(
        self: SqlParsingAggregator, *args: object, **kwargs: object
    ) -> None:
        raise RuntimeError("inline reparse infra failure")

    monkeypatch.setattr(SqlParsingAggregator, "_reparse_observed_inline", _boom_inline)

    aggregator = _make_aggregator(use_parallel=True, workers=2)
    with aggregator.parallel_sql_parsing_scope():
        for item in items:
            aggregator.add(item)
    list(aggregator.gen_metadata())
    report = aggregator.report
    aggregator.close()

    assert report.num_observed_queries_failed == 0
    assert report.num_observed_queries_infra_failed == len(items)


# ---------------------------------------------------------------------------
# Full serialized-MCP equivalence: a complete-object comparison (not a
# field-subset). Every emitted MCP is serialized in full via to_obj() and the
# two runs must be byte-for-byte identical after a deterministic sort.
# ---------------------------------------------------------------------------


def _merge_path_items() -> List[_StreamItem]:
    """A workload that deliberately exercises the cross-session merge path.

    ``s1`` and ``s2`` emit the SAME query fingerprint (literals are generalized
    away when the fingerprint is computed) but with DIFFERENT literals and
    DIFFERENT actors. In the parallel path these two sessions are parsed on
    different workers and merged back in a non-deterministic order, so this is
    exactly the shape that would diverge if the merge (max-timestamp winner for
    text/actor/session) were not deterministic.
    """
    actor_a = CorpUserUrn("actor_a")
    actor_b = CorpUserUrn("actor_b")
    return [
        # Duplicate fingerprint across sessions, different literals + actors.
        ObservedQuery(
            query="insert into downstream (a, b) select a, b from upstream1 where a = 1",
            default_db="dev",
            default_schema="public",
            session_id="s1",
            user=actor_a,
            timestamp=_ts(10),
        ),
        ObservedQuery(
            query="insert into downstream (a, b) select a, b from upstream1 where a = 2",
            default_db="dev",
            default_schema="public",
            session_id="s2",
            user=actor_b,
            timestamp=_ts(20),
        ),
        # Independent lineage-producing queries in further sessions.
        ObservedQuery(
            query="create table foo as select a, b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s3",
            user=actor_a,
            timestamp=_ts(30),
        ),
        ObservedQuery(
            query="create table baz as select a, 2*b as b from bar",
            default_db="dev",
            default_schema="public",
            session_id="s4",
            user=actor_b,
            timestamp=_ts(40),
        ),
        PreparsedQuery(
            query_id=None,
            query_text="select a, c from upstream2",
            upstreams=[DatasetUrn("redshift", "dev.public.upstream2").urn()],
            downstream=DatasetUrn("redshift", "dev.public.derived").urn(),
            timestamp=_ts(50),
            session_id="s3",
        ),
    ]


def _serialize_mcps_canonical(mcps: list) -> List[str]:
    """Serialize EACH MCP in full via to_obj(), then sort deterministically.

    ``to_obj()`` renders the complete MetadataChangeProposalWrapper — entity
    urn, aspect name, and the entire aspect payload — so this is a genuine
    whole-object comparison, not a hand-picked field subset. Each object is
    dumped to a canonical JSON string (sort_keys=True) and the list is sorted by
    that string, giving an order-independent full-fidelity fingerprint of the run.
    """
    return sorted(json.dumps(mcp.to_obj(), sort_keys=True, default=str) for mcp in mcps)


@time_machine.travel(FROZEN_TIME, tick=False)
def _run_serial_raw(items: List[_StreamItem]) -> list:
    aggregator = _make_aggregator(use_parallel=False)
    for item in items:
        aggregator.add(item)
    mcps = list(aggregator.gen_metadata())
    aggregator.close()
    return mcps


@time_machine.travel(FROZEN_TIME, tick=False)
def _run_parallel_raw(items: List[_StreamItem], workers: int) -> tuple:
    aggregator = _make_aggregator(use_parallel=True, workers=workers)
    with aggregator.parallel_sql_parsing_scope():
        for item in items:
            aggregator.add(item)
    mcps = list(aggregator.gen_metadata())
    report = aggregator.report
    aggregator.close()
    return mcps, report


@pytest.mark.parametrize("workers", [2, 3, 4])
def test_full_serialized_mcp_equivalence_serial_vs_parallel(workers: int) -> None:
    """The COMPLETE serialized MCP output must be identical between serial and
    parallel, including the merge of a duplicate fingerprint across sessions with
    different literals and actors.

    This is stronger than the key/subset comparisons elsewhere in this file: we
    serialize every MCP in full (to_obj -> canonical JSON) and assert the two
    fully-serialized, deterministically-sorted lists are EQUAL. No aspect is
    ignored and no field is cherry-picked. Because timestamp/actor/text merges
    are deterministic (max-timestamp winner with a stable tie-break), no
    volatile-field normalization is required.
    """
    items = _merge_path_items()

    serial_mcps = _run_serial_raw(items)
    parallel_mcps, report = _run_parallel_raw(items, workers=workers)

    serial_serialized = _serialize_mcps_canonical(serial_mcps)
    parallel_serialized = _serialize_mcps_canonical(parallel_mcps)

    # Sanity: the workload actually produced output and exercised the pool.
    assert serial_serialized, "expected the workload to emit at least one MCP"
    assert report.num_queries_parsed_in_parallel > 0

    assert parallel_serialized == serial_serialized


# ---------------------------------------------------------------------------
# Config-activation behaviour: the config flag must genuinely drive parallelism
# end-to-end (activate the pool), not merely round-trip as a Pydantic field.
# ---------------------------------------------------------------------------


def test_config_flag_drives_parallelism_end_to_end() -> None:
    """``use_parallel_sql_parsing`` must actually activate the worker pool.

    With the flag ON (and a scope entered) the report must show parallelism
    enabled AND report a positive count of queries parsed in the pool. With the
    flag OFF, parallelism must be disabled, nothing is pool-parsed, and the
    serialized output must be identical to the parallel run (equivalence).
    """
    items = _observed_no_temp_items()

    # Flag OFF: no pool, nothing parsed in parallel.
    off = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
        use_parallel_sql_parsing=False,
    )
    off._schema_resolver.add_raw_schema_info(
        DatasetUrn("redshift", "dev.public.bar").urn(),
        {"a": "int", "b": "int", "c": "int"},
    )
    with time_machine.travel(FROZEN_TIME, tick=False):
        with off.parallel_sql_parsing_scope():
            for item in items:
                off.add(item)
        off_mcps = list(off.gen_metadata())
    off_report = off.report
    off.close()

    assert off_report.sql_parsing_parallel_enabled is False
    assert off_report.num_queries_parsed_in_parallel == 0

    # Flag ON: the pool must genuinely activate.
    on = _make_aggregator(use_parallel=True, workers=2)
    with time_machine.travel(FROZEN_TIME, tick=False):
        with on.parallel_sql_parsing_scope():
            for item in items:
                on.add(item)
        on_mcps = list(on.gen_metadata())
    on_report = on.report
    on.close()

    assert on_report.sql_parsing_parallel_enabled is True
    assert on_report.num_queries_parsed_in_parallel > 0

    # Same output regardless of the flag (the flag changes HOW, not WHAT).
    assert _serialize_mcps_canonical(on_mcps) == _serialize_mcps_canonical(off_mcps)
