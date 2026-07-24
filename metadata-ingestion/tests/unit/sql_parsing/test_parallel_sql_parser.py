import pathlib
import tempfile
import unittest.mock as mock
from unittest.mock import patch

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

    tasks = [
        ParseTask(
            key=key,
            query=query,
            default_db="db",
            default_schema="schema",
        )
        for key, query in queries.items()
    ]

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
        outcomes = list(parser.map_unordered(tasks))

    assert len(outcomes) == len(queries)
    for outcome in outcomes:
        assert outcome.error is None, outcome.error
        assert outcome.result is not None
        assert isinstance(outcome.key, str)
        exp = expected[outcome.key]
        assert outcome.result.in_tables == exp.in_tables
        assert outcome.result.out_tables == exp.out_tables
        assert outcome.result.column_lineage == exp.column_lineage

    writable.close()


def test_unordered_correlation(tmp_path: pathlib.Path) -> None:
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)
    writable.close()

    tasks = [
        ParseTask(
            key=i,
            query="SELECT order_id FROM db.schema.orders",
            default_db="db",
            default_schema="schema",
        )
        for i in range(25)
    ]
    submitted_keys = {task.key for task in tasks}

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        returned_keys = [outcome.key for outcome in parser.map_unordered(tasks)]

    assert len(returned_keys) == len(submitted_keys)
    assert set(returned_keys) == submitted_keys


def test_malformed_query_returns_result_not_error(tmp_path: pathlib.Path) -> None:
    """A genuinely unparseable query yields a ParseOutcome carrying a result whose
    debug_info records the error, rather than raising or an outcome-level error."""
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)
    writable.close()

    task = ParseTask(
        key="bad",
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
        outcomes = list(parser.map_unordered([task]))

    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert outcome.key == "bad"
    # Normal parse failures are captured inside SqlParsingResult.debug_info,
    # so this is a returned result, not an outcome-level error.
    assert outcome.error is None
    assert outcome.result is not None
    assert outcome.result.debug_info.error is not None


def test_close_is_idempotent(tmp_path: pathlib.Path) -> None:
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)
    writable.close()

    parser = ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    )
    # Force pool creation so close() has something to shut down.
    list(
        parser.map_unordered(
            [
                ParseTask(
                    key=1,
                    query="SELECT order_id FROM db.schema.orders",
                    default_db="db",
                    default_schema="schema",
                )
            ]
        )
    )
    parser.close()
    parser.close()  # must be safe to call twice


def test_context_manager(tmp_path: pathlib.Path) -> None:
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)
    writable.close()

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        outcomes = list(
            parser.map_unordered(
                [
                    ParseTask(
                        key=1,
                        query="SELECT order_id FROM db.schema.orders",
                        default_db="db",
                        default_schema="schema",
                    )
                ]
            )
        )
    assert len(outcomes) == 1
    assert outcomes[0].error is None


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
            ParseTask(
                key="only",
                query=query,
                default_db="db",
                default_schema="schema",
            )
        )

    assert outcome.key == "only"
    assert outcome.error is None
    assert outcome.result is not None
    assert outcome.result.in_tables == expected.in_tables
    assert outcome.result.out_tables == expected.out_tables
    assert outcome.result.column_lineage == expected.column_lineage

    writable.close()


def test_worker_formats_query_when_enabled(tmp_path: pathlib.Path) -> None:
    """When format_queries is enabled, the worker must return a populated
    formatted_query that is byte-identical to try_format_query on the main thread."""
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)
    writable.close()

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
            ParseTask(
                key="fmt",
                query=query,
                default_db="db",
                default_schema="schema",
            )
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
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)
    writable.close()

    with ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    ) as parser:
        outcome = parser.parse_one(
            ParseTask(
                key="nofmt",
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

    with patch.object(aggregator, "_add_preparsed_query_impl", side_effect=_boom):
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
    aggregator.close()


def test_parse_outcome_rejects_both_result_and_error() -> None:
    """The result-XOR-error invariant is enforced structurally (R1)."""
    with pytest.raises(ValueError):
        ParseOutcome(key="k", result=object(), error="boom")  # type: ignore[arg-type]


def test_parse_outcome_failed_and_ok_properties() -> None:
    """`failed`/`ok` classify outcomes without repeating the null-check (R1)."""
    error_outcome = ParseOutcome(key="k", result=None, error="boom")
    assert error_outcome.failed
    assert not error_outcome.ok

    empty_outcome = ParseOutcome(key="k", result=None, error=None)
    assert empty_outcome.failed
    assert not empty_outcome.ok

    result_outcome = ParseOutcome(key="k", result=object(), error=None)  # type: ignore[arg-type]
    assert not result_outcome.failed
    assert result_outcome.ok


def test_parse_task_rejects_non_picklable_key() -> None:
    """A non-picklable key raises TypeError at construction, not later as a
    broken pool (R4)."""
    with pytest.raises(TypeError):
        ParseTask(
            key=lambda: None,  # type: ignore[arg-type]
            query="SELECT 1",
            default_db=None,
            default_schema=None,
        )
    # The allowed simple types construct fine.
    for key in (None, "s", 7, ("a", 1)):
        ParseTask(key=key, query="SELECT 1", default_db=None, default_schema=None)


def test_post_close_use_raises_runtime_error(tmp_path: pathlib.Path) -> None:
    """Using the parser after close() is a caller bug and must surface as a plain
    RuntimeError, NOT ParallelParserUnavailable (which the aggregator's
    serial-fallback would otherwise silently swallow) (R3)."""
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)
    writable.close()

    parser = ParallelSqlParser(
        num_workers=2,
        snapshot_path=snap,
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
    )
    parser.close()

    task = ParseTask(
        key="k",
        query="SELECT order_id FROM db.schema.orders",
        default_db="db",
        default_schema="schema",
    )
    with pytest.raises(RuntimeError) as exc_info:
        parser.parse_one(task)
    assert not isinstance(exc_info.value, ParallelParserUnavailable)

    with pytest.raises(RuntimeError) as exc_info2:
        list(parser.map_unordered([task]))
    assert not isinstance(exc_info2.value, ParallelParserUnavailable)


def test_executor_submit_failure_becomes_error_outcome(
    tmp_path: pathlib.Path,
) -> None:
    """An executor-layer failure (worker death / submit blowing up) must surface
    as a ParseOutcome(error=...), not a raised exception, and the parser must
    still close cleanly (R6)."""
    writable = _make_writable_resolver(tmp_path)
    snap = tmp_path / "snapshot.db"
    writable.snapshot_to(snap)
    writable.close()

    task = ParseTask(
        key="k",
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
        # Simulate pool breaking - after C4 this is a threading.Event
        aggregator._parallel_parser.pool_broke.set()

        # Call close() directly - this goes through _teardown_parallel
        # WITHOUT the scope's finally having run first
        aggregator.close()

    assert aggregator.report.sql_parsing_pool_broke is True


# ---------------------------------------------------------------------------
# G1 — preformatted_query rejected on external add_preparsed_query calls
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# G2 — ParseOutcome rejects formatted_query when error is set
# ---------------------------------------------------------------------------


def test_parse_outcome_rejects_formatted_query_with_error() -> None:
    """It is a contract violation to set formatted_query alongside error;
    a formatting result is only meaningful on a successful outcome."""
    with pytest.raises(
        ValueError, match="formatted_query must be None when error is set"
    ):
        ParseOutcome(
            key="k",
            result=None,
            error="boom",
            formatted_query="SELECT 1",
        )


# ---------------------------------------------------------------------------
# G4 — ParseTask.key tuple elements must be str or int
# ---------------------------------------------------------------------------


def test_parse_task_rejects_tuple_key_with_non_str_int_elements() -> None:
    """A tuple key whose elements are not all str/int must raise TypeError
    at construction (not later as an opaque broken pool)."""
    with pytest.raises(TypeError, match="tuple elements must all be str or int"):
        ParseTask(
            key=("good", object()),  # type: ignore[arg-type]
            query="SELECT 1",
            default_db=None,
            default_schema=None,
        )


def test_parse_task_accepts_valid_tuple_keys() -> None:
    """Tuple keys made entirely of str/int elements are accepted."""
    for key in (("a",), (1,), ("a", 1), ("x", "y", 3)):
        ParseTask(key=key, query="SELECT 1", default_db=None, default_schema=None)


# ---------------------------------------------------------------------------
# G5 — ParseOutcome.key undergoes the same validation as ParseTask.key
# ---------------------------------------------------------------------------


def test_parse_outcome_rejects_invalid_key_types() -> None:
    """ParseOutcome.key must also be None/str/int/valid-tuple, matching the
    ParseTask.key restriction (G5 — same validator, DRY)."""
    with pytest.raises(TypeError):
        ParseOutcome(
            key=object(),  # type: ignore[arg-type]
            result=None,
            error="err",
        )


def test_parse_outcome_rejects_tuple_key_with_bad_elements() -> None:
    """A tuple ParseOutcome.key with non-str/int elements raises TypeError."""
    with pytest.raises(TypeError, match="tuple elements must all be str or int"):
        ParseOutcome(
            key=("ok", 3.14),  # type: ignore[arg-type]
            result=None,
            error="err",
        )


def test_parse_outcome_accepts_valid_key_types() -> None:
    """Valid key types (None, str, int, str/int tuple) all construct fine."""
    for key in (None, "s", 7, ("a", 1)):
        ParseOutcome(key=key, result=None, error="err")
