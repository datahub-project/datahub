import os
import tempfile

import sqlalchemy as sa

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters import get_adapter
from datahub.ingestion.source.sqlalchemy_profiler.adapters.duckdb import DuckDBAdapter
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
        assert adapter.get_row_count(table, conn) == 5
        assert int(adapter.get_column_min(table, "num", conn)) == 1
        assert int(adapter.get_column_max(table, "num", conn)) == 5
        assert abs(float(adapter.get_column_mean(table, "num", conn)) - 3.0) < 1e-9
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
        assert adapter.get_column_min(ctx.sql_table, "num", conn) is not None
