import os
import tempfile
from unittest.mock import patch

import pytest
import sqlalchemy as sa

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters import get_adapter
from datahub.ingestion.source.sqlalchemy_profiler.adapters.duckdb import (
    DuckDBAdapter,
    _convert_bound,
)
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)


def _engine() -> sa.engine.Engine:
    # On-disk temp file avoids the :memory: per-connection isolation trap.
    path = os.path.join(tempfile.mkdtemp(), "t.duckdb")
    return sa.create_engine(f"duckdb:///{path}")


def test_get_adapter_returns_duckdb_adapter() -> None:
    adapter = get_adapter("duckdb", ProfilingConfig(), SQLSourceReport(), _engine())
    assert isinstance(adapter, DuckDBAdapter)


def test_approx_unique_count_uses_native_function() -> None:
    adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), _engine())
    expr = adapter.get_approx_unique_count_expr("col_a")
    assert "approx_count_distinct" in str(expr).lower()


def test_median_and_quantiles_use_quantile_cont() -> None:
    adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), _engine())
    assert adapter.get_median_expr("col_a") is not None
    assert "quantile_cont" in str(adapter.get_median_expr("col_a")).lower()
    eng = _engine()
    with eng.connect() as conn:
        conn.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES (1),(2),(3),(4),(5)) AS v(col_a)"
        )
        table = sa.Table("t", sa.MetaData(), autoload_with=eng)
        qs = adapter.get_column_quantiles(table, "col_a", conn, [0.5])
        assert qs == [3.0]


def test_summarize_cache_serves_column_stats():
    eng = _engine()
    with eng.begin() as conn:
        conn.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,NULL),(4,'b'),(5,'a')) AS v(num, txt)"
        )
    with eng.connect() as conn:
        adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), eng)
        ctx = ProfilingContext(pretty_name="t", table="t")
        ctx = adapter.setup_profiling(ctx, conn)

        table = ctx.sql_table
        assert table is not None
        assert adapter.get_row_count(table, conn) == 5
        assert int(adapter.get_column_min(table, "num", conn)) == 1
        assert int(adapter.get_column_max(table, "num", conn)) == 5
        mean = adapter.get_column_mean(table, "num", conn)
        assert mean is not None
        assert abs(float(mean) - 3.0) < 1e-9
        assert adapter.get_column_non_null_count(table, "txt", conn) == 4
        assert adapter.get_column_unique_count(table, "num", conn) == 5


def test_summarize_cache_miss_falls_back():
    eng = _engine()
    with eng.begin() as conn:
        conn.execute("CREATE TABLE t AS SELECT 1 AS num")
    with eng.connect() as conn:
        adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), eng)
        ctx = adapter.setup_profiling(
            ProfilingContext(pretty_name="t", table="t"), conn
        )
        table = ctx.sql_table
        assert table is not None
        assert adapter.get_column_min(table, "num", conn) is not None


@pytest.mark.parametrize(
    "value,col_type,expected,expected_type",
    [
        (None, "BIGINT", None, type(None)),
        ("42", "BIGINT", 42, int),
        ("42", "INTEGER", 42, int),
        ("42", "UINTEGER", 42, int),
        ("3.5", "DOUBLE", 3.5, float),
        ("3.5", "DECIMAL(10,2)", 3.5, float),
        ("3.5", "FLOAT", 3.5, float),
        ("abc", "VARCHAR", "abc", str),
        # Malformed numeric falls back to the original string.
        ("notanumber", "BIGINT", "notanumber", str),
    ],
)
def test_convert_bound(value, col_type, expected, expected_type):
    result = _convert_bound(value, col_type)
    assert result == expected
    assert isinstance(result, expected_type)


def test_summarize_failure_fallback():
    """When _run_summarize raises, _summary is empty and metric calls use SQL fallback."""
    eng = _engine()
    with eng.begin() as conn:
        conn.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1),(2),(3)) AS v(num)")
    with eng.connect() as conn:
        adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), eng)
        with patch.object(
            DuckDBAdapter, "_run_summarize", side_effect=RuntimeError("forced fail")
        ):
            ctx = adapter.setup_profiling(
                ProfilingContext(pretty_name="t", table="t"), conn
            )
        # After failure, cache must be empty.
        assert adapter._summary == {}
        assert adapter._row_count is None

        # SQL fallback must still return correct values — no crash.
        table = ctx.sql_table
        assert table is not None
        assert int(adapter.get_column_min(table, "num", conn)) == 1
        assert float(adapter.get_column_mean(table, "num", conn)) == pytest.approx(2.0)  # type: ignore[arg-type]
        assert adapter.get_row_count(table, conn) == 3
        assert adapter.get_column_non_null_count(table, "num", conn) == 3


def test_summarize_cache_is_populated_and_served():
    """Proves setup_profiling populates _summary AND that the cache is served (not base SQL)."""
    from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
        PlatformAdapter,
    )

    eng = _engine()
    with eng.begin() as conn:
        conn.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,NULL),(4,'b'),(5,'a')) AS v(num, txt)"
        )
    with eng.connect() as conn:
        adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), eng)
        ctx = adapter.setup_profiling(
            ProfilingContext(pretty_name="t", table="t"), conn
        )

        # 1. Cache is populated with expected structure.
        assert adapter._summary, "_summary must be non-empty after setup_profiling"
        assert "num" in adapter._summary and "txt" in adapter._summary
        assert adapter._row_count == 5
        assert adapter._summary["num"].approx_unique == 5
        assert adapter._summary["txt"].non_null == 4

        # 2. Cache is served: if base SQL ran, the monkeypatched method would raise.
        table = ctx.sql_table
        assert table is not None
        with patch.object(
            PlatformAdapter, "get_column_min", side_effect=RuntimeError("base called")
        ):
            assert adapter.get_column_min(table, "num", conn) is not None
        with patch.object(
            PlatformAdapter,
            "get_column_non_null_count",
            side_effect=RuntimeError("base called"),
        ):
            assert adapter.get_column_non_null_count(table, "txt", conn) == 4
        with patch.object(
            PlatformAdapter, "get_row_count", side_effect=RuntimeError("base called")
        ):
            assert adapter.get_row_count(table, conn) == 5
