import pathlib

import pytest

from datahub.sql_parsing.parallel_sql_parser import (
    ParallelSqlParser,
    ParseOutcome,
    ParseTask,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage


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


@pytest.mark.parametrize("outcome_key", ["a", "b"])
def test_parse_outcome_shape(outcome_key: str) -> None:
    outcome = ParseOutcome(key=outcome_key, result=None, error="boom")
    assert outcome.key == outcome_key
    assert outcome.result is None
    assert outcome.error == "boom"
