import pathlib
import tempfile
import unittest.mock as mock

import pytest

from datahub.sql_parsing.parallel_sql_parser import (
    ParallelParserUnavailable,
    ParallelSqlParser,
    ParseOutcome,
    ParseTask,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    PreparsedQuery,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage
from datahub.sql_parsing.sqlglot_utils import try_format_query


def _make_writable_resolver(tmp_path: pathlib.Path) -> SchemaResolver:
    """Build a file-backed writable SchemaResolver with two known schemas."""
    cache_file = tmp_path / "schema_cache.db"
    resolver = SchemaResolver(
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
        graph=None,
        _cache_filename=cache_file,
    )
    resolver.add_raw_schema_info(
        urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.orders,PROD)",
        schema_info={"order_id": "int", "amount": "float", "customer_id": "int"},
    )
    resolver.add_raw_schema_info(
        urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.db.schema.customers,PROD)",
        schema_info={"customer_id": "int", "name": "varchar"},
    )
    return resolver


def _make_snapshot(tmp_path: pathlib.Path) -> pathlib.Path:
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)
    writable.close()
    return snap


def test_parity_end_to_end(tmp_path: pathlib.Path) -> None:
    """Parallel parsing must produce lineage identical to in-process serial parsing.

    Proves cross-process pickling + snapshot resolution preserve lineage.
    """
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)

    queries = {
        "q1": "SELECT order_id, amount FROM db.schema.orders",
        "q2": (
            "CREATE TABLE db.schema.summary AS "
            "SELECT customer_id, amount FROM db.schema.orders"
        ),
        "q3": (
            "SELECT o.order_id, c.name FROM db.schema.orders o "
            "JOIN db.schema.customers c ON o.customer_id = c.customer_id"
        ),
    }

    expected = {
        key: sqlglot_lineage(
            query,
            schema_resolver=writable,
            default_db="db",
            default_schema="schema",
        )
        for key, query in queries.items()
    }

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        for key, query in queries.items():
            outcome = parser.parse_one(
                ParseTask(query=query, default_db="db", default_schema="schema")
            )
            assert outcome.error is None, outcome.error
            assert outcome.result is not None
            exp = expected[key]
            assert outcome.result.in_tables == exp.in_tables
            assert outcome.result.out_tables == exp.out_tables
            assert outcome.result.column_lineage == exp.column_lineage

    writable.close()


def test_malformed_query_returns_result_not_error(tmp_path: pathlib.Path) -> None:
    """A genuinely unparseable query yields a ParseOutcome carrying a result whose
    debug_info records the error, rather than raising or an outcome-level error."""
    snap = _make_snapshot(tmp_path)

    task = ParseTask(
        query="SELECT SELECT FROM WHERE ((((",
        default_db="db",
        default_schema="schema",
    )

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        outcome = parser.parse_one(task)

    # Normal parse failures are captured inside SqlParsingResult.debug_info,
    # so this is a returned result, not an outcome-level error.
    assert outcome.error is None
    assert outcome.result is not None
    assert outcome.result.debug_info.error is not None


def test_close_is_idempotent(tmp_path: pathlib.Path) -> None:
    snap = _make_snapshot(tmp_path)

    parser = ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    )
    # Force pool use so close() has something to shut down.
    parser.parse_one(
        ParseTask(
            query="SELECT order_id FROM db.schema.orders",
            default_db="db",
            default_schema="schema",
        )
    )
    parser.close()
    parser.close()  # must be safe to call twice


def test_context_manager(tmp_path: pathlib.Path) -> None:
    snap = _make_snapshot(tmp_path)

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        outcome = parser.parse_one(
            ParseTask(
                query="SELECT order_id FROM db.schema.orders",
                default_db="db",
                default_schema="schema",
            )
        )
    assert outcome.error is None


def test_parse_one_blocking(tmp_path: pathlib.Path) -> None:
    """parse_one submits a single task and blocks for its outcome, producing a
    result identical to in-process serial parsing."""
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)

    query = "SELECT order_id, amount FROM db.schema.orders"
    expected = sqlglot_lineage(
        query,
        schema_resolver=writable,
        default_db="db",
        default_schema="schema",
    )

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        outcome = parser.parse_one(
            ParseTask(query=query, default_db="db", default_schema="schema")
        )

    assert outcome.error is None
    assert outcome.result is not None
    assert outcome.result.in_tables == expected.in_tables
    assert outcome.result.out_tables == expected.out_tables
    assert outcome.result.column_lineage == expected.column_lineage

    writable.close()


def test_worker_formats_query_when_enabled(tmp_path: pathlib.Path) -> None:
    """When format_queries is enabled, the worker must return a populated
    formatted_query that is byte-identical to try_format_query on the main thread."""
    snap = _make_snapshot(tmp_path)

    query = "select order_id, amount from db.schema.orders"

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
        format_queries=True,
    ) as parser:
        outcome = parser.parse_one(
            ParseTask(query=query, default_db="db", default_schema="schema")
        )

    assert outcome.ok
    assert outcome.formatted_query is not None
    assert outcome.formatted_query == try_format_query(query, "snowflake")
    # Formatting should actually change the query (pretty-print), proving it ran.
    assert outcome.formatted_query != query


def test_worker_leaves_formatted_query_none_when_disabled(
    tmp_path: pathlib.Path,
) -> None:
    """When format_queries is disabled (default), formatted_query stays None so the
    main thread formats as it does today."""
    snap = _make_snapshot(tmp_path)

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        outcome = parser.parse_one(
            ParseTask(
                query="select order_id from db.schema.orders",
                default_db="db",
                default_schema="schema",
            )
        )

    assert outcome.ok
    assert outcome.formatted_query is None


def test_preparsed_failure_counter(tmp_path: pathlib.Path) -> None:
    """PreparsedQuery failures must increment num_preparsed_queries_failed, not num_observed_queries_failed."""
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        use_parallel_sql_parsing=True,
        sql_parsing_workers=2,
    )

    def _boom(*args, **kwargs):
        raise RuntimeError("injected failure")

    with mock.patch.object(aggregator, "_add_preparsed_query_impl", side_effect=_boom):
        with aggregator.parallel_sql_parsing_scope():
            aggregator.add(
                PreparsedQuery(
                    query_id=None,
                    query_text="SELECT 1",
                    upstreams=[],
                )
            )

    assert aggregator.report.num_preparsed_queries_failed == 1
    assert aggregator.report.num_observed_queries_failed == 0
    assert len(aggregator.report.preparsed_query_parse_failures) == 1
    aggregator.close()


def test_snapshot_temp_dir_cleaned_up(tmp_path: pathlib.Path) -> None:
    """The snapshot temp directory must be deleted after the scope exits."""
    resolver = SchemaResolver(
        platform="snowflake",
        platform_instance=None,
        env="PROD",
        graph=None,
        _cache_filename=tmp_path / "schema.db",
    )

    aggregator = SqlParsingAggregator(
        platform="snowflake",
        schema_resolver=resolver,
        use_parallel_sql_parsing=True,
        sql_parsing_workers=2,
    )

    captured_snapshot_dir: list = []

    original_snapshot_to = resolver.snapshot_to

    def capturing_snapshot_to(path):
        captured_snapshot_dir.append(path.parent)
        original_snapshot_to(path)

    with mock.patch.object(resolver, "snapshot_to", side_effect=capturing_snapshot_to):
        with aggregator.parallel_sql_parsing_scope():
            pass

    assert len(captured_snapshot_dir) == 1
    snap_dir = captured_snapshot_dir[0]
    assert not snap_dir.exists(), f"Snapshot temp dir still exists: {snap_dir}"
    aggregator.close()


def test_broken_pool_sets_report_flag(tmp_path: pathlib.Path) -> None:
    """When the process pool breaks, report.sql_parsing_pool_broke must be set and run must complete."""
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        use_parallel_sql_parsing=True,
        sql_parsing_workers=2,
    )

    with aggregator.parallel_sql_parsing_scope():
        assert aggregator._parallel_parser is not None
        # Simulate broken pool by monkeypatching pool_broke
        aggregator._parallel_parser.pool_broke.set()

    assert aggregator.report.sql_parsing_pool_broke is True
    # A mid-run pool break means the rest of the run parsed serially.
    assert aggregator.report.sql_parsing_fell_back_to_serial is True
    aggregator.close()


def test_parse_outcome_rejects_both_result_and_error() -> None:
    """The result-XOR-error invariant is enforced structurally."""
    with pytest.raises(ValueError):
        ParseOutcome(result=object(), error="boom")  # type: ignore[arg-type]


def test_parse_outcome_failed_and_ok_properties() -> None:
    """`failed`/`ok` classify outcomes without repeating the null-check."""
    error_outcome = ParseOutcome(result=None, error="boom")
    assert error_outcome.failed
    assert not error_outcome.ok

    empty_outcome = ParseOutcome(result=None, error=None)
    assert empty_outcome.failed
    assert not empty_outcome.ok

    result_outcome = ParseOutcome(result=object(), error=None)  # type: ignore[arg-type]
    assert not result_outcome.failed
    assert result_outcome.ok


def test_post_close_use_raises_runtime_error(tmp_path: pathlib.Path) -> None:
    """Using the parser after close() is a caller bug and must surface as a plain
    RuntimeError, NOT ParallelParserUnavailable (which the aggregator's
    serial-fallback would otherwise silently swallow)."""
    snap = _make_snapshot(tmp_path)

    parser = ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    )
    parser.close()

    task = ParseTask(
        query="SELECT order_id FROM db.schema.orders",
        default_db="db",
        default_schema="schema",
    )
    with pytest.raises(RuntimeError) as exc_info:
        parser.parse_one(task)
    assert not isinstance(exc_info.value, ParallelParserUnavailable)


def test_executor_submit_failure_becomes_error_outcome(
    tmp_path: pathlib.Path,
) -> None:
    """An executor-layer failure (worker death / submit blowing up) must surface
    as a ParseOutcome(error=...), not a raised exception, and the parser must
    still close cleanly."""
    snap = _make_snapshot(tmp_path)

    task = ParseTask(
        query="SELECT order_id FROM db.schema.orders",
        default_db="db",
        default_schema="schema",
    )

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        executor = parser._ensure_executor()

        class _DeadFuture:
            def result(self, timeout=None):
                raise RuntimeError("simulated worker death at executor layer")

        # Force the submitted future to blow up on .result(), simulating a worker
        # that died before returning a ParseOutcome.
        original_submit = executor.submit

        def _boom_submit(fn, *args, **kwargs):  # type: ignore[no-untyped-def]
            return _DeadFuture()

        executor.submit = _boom_submit  # type: ignore[assignment]

        outcome = parser.parse_one(task)
        assert outcome.failed
        assert outcome.error is not None
        assert outcome.result is None

        # Restore real submit; the parser must still be usable afterwards.
        executor.submit = original_submit  # type: ignore[assignment]
        good = parser.parse_one(task)
        assert good.ok


def test_pool_broke_reported_via_close_outside_scope(tmp_path: pathlib.Path) -> None:
    """When the pool breaks and close() is called without the scope's finally
    running the check first, report.sql_parsing_pool_broke must still be True."""
    aggregator = SqlParsingAggregator(
        platform="snowflake",
        use_parallel_sql_parsing=True,
        sql_parsing_workers=2,
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        snapshot_path = pathlib.Path(tmp_dir) / "schema_snapshot.db"
        aggregator._schema_resolver.snapshot_to(snapshot_path)

        try:
            aggregator._parallel_parser = ParallelSqlParser(
                num_workers=2,
                snapshot_path=snapshot_path,
                platform="snowflake",
                platform_instance=None,
                env="PROD",
            )
        except ParallelParserUnavailable:
            pytest.skip("parallel parser unavailable in this environment")

        aggregator._parallel_active = True
        aggregator._parallel_parser.pool_broke.set()

        # Call close() directly - this goes through _teardown_parallel
        # WITHOUT the scope's finally having run first.
        aggregator.close()

    assert aggregator.report.sql_parsing_pool_broke is True


def test_add_preparsed_query_rejects_preformatted_when_external() -> None:
    """An external caller must not pass preformatted_query; doing so raises
    ValueError because the value would be silently dropped on the
    parallel-routing branch and is meaningless on the external path."""
    aggregator = SqlParsingAggregator(platform="snowflake")
    with pytest.raises(
        ValueError, match="preformatted_query is only valid on internal"
    ):
        aggregator.add_preparsed_query(
            PreparsedQuery(
                query_id=None,
                query_text="SELECT 1",
                upstreams=[],
            ),
            preformatted_query="SELECT 1",
        )
    aggregator.close()


def test_add_preparsed_query_accepts_preformatted_when_internal() -> None:
    """The internal observed-query apply path sets _is_internal=True and must
    be allowed to pass preformatted_query."""
    aggregator = SqlParsingAggregator(platform="snowflake")
    # Should not raise — _is_internal=True is the valid internal path.
    aggregator.add_preparsed_query(
        PreparsedQuery(
            query_id=None,
            query_text="SELECT 1",
            upstreams=[],
        ),
        _is_internal=True,
        preformatted_query="SELECT 1",
    )
    aggregator.close()


def test_parse_outcome_rejects_formatted_query_with_error() -> None:
    """It is a contract violation to set formatted_query alongside error;
    a formatting result is only meaningful on a successful outcome."""
    with pytest.raises(
        ValueError, match="formatted_query must be None when error is set"
    ):
        ParseOutcome(
            result=None,
            error="boom",
            formatted_query="SELECT 1",
        )


def test_parse_one_submit_time_broken_pool_sets_flag_no_raise(
    tmp_path: pathlib.Path,
) -> None:
    """If executor.submit() itself raises BrokenProcessPool, parse_one must
    convert it to an error ParseOutcome AND set pool_broke — never let it
    propagate (which would be counted as an ordinary parse failure upstream)."""
    from concurrent.futures.process import BrokenProcessPool

    snap = _make_snapshot(tmp_path)
    task = ParseTask(
        query="SELECT order_id FROM db.schema.orders",
        default_db="db",
        default_schema="schema",
    )
    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        with mock.patch.object(
            parser._ensure_executor(),
            "submit",
            side_effect=BrokenProcessPool("boom at submit"),
        ):
            outcome = parser.parse_one(task)
        assert outcome.failed
        assert outcome.error is not None
        assert outcome.result is None
        assert parser.pool_broke.is_set()
