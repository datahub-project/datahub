"""Equivalence tests for per-session-safe parallel SQL parsing in SqlParsingAggregator.

The correctness bar: parallel output MUST be byte-identical to serial output
(no lineage loss). Each test runs the same scripted stream of queries through a
serial aggregator (feature off) and a parallel aggregator (feature on, wrapped in
``parallel_sql_parsing_scope()``), then asserts the emitted MCPs are equal after
normalizing ordering.
"""

from datetime import datetime, timezone
from typing import List, Union

import pytest
import time_machine

from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    PreparsedQuery,
    QueryLogSetting,
    SqlParsingAggregator,
)
from datahub.utilities import cpu_detection

_StreamItem = Union[ObservedQuery, PreparsedQuery]

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
) -> SqlParsingAggregator:
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
        query_log=QueryLogSetting.DISABLED,
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


def test_fallback_to_serial(monkeypatch: pytest.MonkeyPatch) -> None:
    """With only 1 CPU detected, the feature falls back to serial and produces
    output identical to a plain serial run; the report records the fallback."""

    real_resolve = cpu_detection.resolve_worker_count

    def _one_cpu(requested, **kwargs):
        kwargs.pop("detected_cpus", None)
        return real_resolve(requested, detected_cpus=1, **kwargs)

    monkeypatch.setattr(
        "datahub.sql_parsing.sql_parsing_aggregator.resolve_worker_count",
        _one_cpu,
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
