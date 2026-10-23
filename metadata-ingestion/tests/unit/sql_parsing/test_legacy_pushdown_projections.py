from typing import Any, List

import pytest
import sqlglot
from sqlglot.errors import OptimizeError
from sqlglot.optimizer.journal import Journal, record

import datahub.sql_parsing.sqlglot_lineage as sqlglot_lineage_module
from datahub.sql_parsing._legacy_pushdown_projections import (
    pushdown_projections as legacy_pushdown_projections,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import (
    _OptimizerStats,
    _pushdown_projections,
    sqlglot_lineage,
)


@pytest.fixture
def legacy_calls(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    """Record the SQL each call to the vendored rule receives, then run it."""
    calls: List[str] = []

    def _recording_legacy(expression: sqlglot.exp.Expr, **kwargs: Any) -> Any:
        calls.append(expression.sql())
        return legacy_pushdown_projections(expression, **kwargs)

    monkeypatch.setattr(
        sqlglot_lineage_module, "legacy_pushdown_projections", _recording_legacy
    )
    return calls


def _schema_resolver() -> SchemaResolver:
    schema_resolver = SchemaResolver(platform="snowflake")
    schema_resolver.add_raw_schema_info(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.table_b,PROD)",
        {"id": "NUMBER", "col_b": "VARCHAR"},
    )
    return schema_resolver


def test_star_free_query_skips_fallback(legacy_calls: List[str]) -> None:
    result = sqlglot_lineage(
        "SELECT c.col_b FROM (SELECT * FROM my_db.my_schema.table_b) c",
        schema_resolver=_schema_resolver(),
    )

    # The star is over a table with schema info, so qualify expands it.
    assert legacy_calls == []
    assert not result.debug_info.used_legacy_pushdown_projections
    assert result.column_lineage


def test_unexpandable_star_uses_fallback(legacy_calls: List[str]) -> None:
    result = sqlglot_lineage(
        "SELECT c.col_a FROM (SELECT * FROM my_db.my_schema.table_a) c",
        schema_resolver=_schema_resolver(),
    )

    assert len(legacy_calls) == 1
    assert result.debug_info.used_legacy_pushdown_projections
    assert result.column_lineage
    assert [u.column for u in result.column_lineage[0].upstreams] == ["col_a"]


def test_other_optimize_error_propagates(
    legacy_calls: List[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def _failing_upstream(expression: sqlglot.exp.Expr, journal: Journal) -> Any:
        raise OptimizeError("Invalid set operation due to column mismatch")

    monkeypatch.setattr(
        sqlglot.optimizer.optimizer, "pushdown_projections", _failing_upstream
    )
    stats = _OptimizerStats()
    with pytest.raises(OptimizeError, match="column mismatch"):
        _pushdown_projections(
            sqlglot.parse_one("SELECT * FROM t"), datahub_optimizer_stats=stats
        )

    assert legacy_calls == []
    assert not stats.used_legacy_pushdown_projections


def test_fallback_sees_tree_before_upstream_mutation(
    legacy_calls: List[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def _mutate_then_raise(expression: sqlglot.exp.Expr, journal: Journal) -> Any:
        select = expression.find(sqlglot.exp.Select)
        assert select is not None
        record(journal, select, "expressions")
        select.set("expressions", [sqlglot.exp.column("pruned")])
        raise OptimizeError("projection_reachability requires star-free selections")

    monkeypatch.setattr(
        sqlglot.optimizer.optimizer, "pushdown_projections", _mutate_then_raise
    )
    _pushdown_projections(sqlglot.parse_one("SELECT * FROM (SELECT * FROM t) AS x"))

    assert legacy_calls == ["SELECT * FROM (SELECT * FROM t) AS x"]
