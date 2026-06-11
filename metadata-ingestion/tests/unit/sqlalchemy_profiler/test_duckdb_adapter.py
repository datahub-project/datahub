import os
import tempfile

import sqlalchemy as sa

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters import get_adapter
from datahub.ingestion.source.sqlalchemy_profiler.adapters.duckdb import DuckDBAdapter


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
    eng = _engine()
    with eng.connect() as conn:
        conn.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES (1),(2),(3),(4),(5)) AS v(col_a)"
        )
        table = sa.Table("t", sa.MetaData(), autoload_with=eng)
        qs = adapter.get_column_quantiles(table, "col_a", conn, [0.5])
        assert qs == [3.0]
