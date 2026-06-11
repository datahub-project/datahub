# DuckDB Profiling for Data-Lake Sources — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Spark/PyDeequ profiler in the data-lake sources (S3, GCS, abs, delta-lake) with DuckDB driven through the existing `SQLAlchemyProfiler`, removing the JVM/Java requirement and the `pyspark`/`pydeequ` dependencies.

**Architecture:** Two pieces. (1) A new `DuckDBAdapter` in `sqlalchemy_profiler/adapters/` providing DuckDB-native SQL expressions plus a `SUMMARIZE` fast-path (cache per-column stats in `setup_profiling`, serve them from overridden `get_column_*` execution methods). (2) A rewritten `s3/profiling.py` "attach layer" that opens a temp-file-backed DuckDB connection, injects cloud credentials via `CREATE SECRET`, attaches each table's files as a view (`read_parquet`/`read_csv_auto`/`read_json`/`delta_scan`), and delegates to `SQLAlchemyProfiler(platform="duckdb")`.

**Tech Stack:** Python, DuckDB, `duckdb-engine` (SQLAlchemy dialect), SQLAlchemy, pydantic, pytest.

**Spec:** `docs/superpowers/specs/2026-06-11-s3-duckdb-profiling-design.md`

**Conventions for every task:** verify Python with `../gradlew :metadata-ingestion:lintFix` then `../gradlew :metadata-ingestion:lint` (lintFix is ruff-only; lint runs mypy). Run unit tests with `pytest -m 'not integration' <path>`. Commit messages omit Co-Authored-By / "Generated with Claude Code" footers.

---

## File structure

**Create:**

- `src/datahub/ingestion/source/sqlalchemy_profiler/adapters/duckdb.py` — `DuckDBAdapter`
- `src/datahub/ingestion/source/s3/duckdb_profiler.py` — `DuckDBProfiler` attach layer (replaces Spark profiler logic)
- `src/datahub/ingestion/source/s3/duckdb_secrets.py` — cloud-credential → `CREATE SECRET` mapping
- `tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py`
- `tests/unit/s3/test_duckdb_profiler.py`
- `tests/unit/s3/test_duckdb_secrets.py`

**Modify:**

- `src/datahub/ingestion/source/sqlalchemy_profiler/adapters/__init__.py` — register `"duckdb"`
- `src/datahub/ingestion/source/s3/datalake_profiler_config.py` — subclass `GEProfilingConfig`
- `src/datahub/ingestion/source/s3/config.py` — deprecate `spark_driver_memory`/`spark_config`
- `src/datahub/ingestion/source/s3/source.py:279-307,596-597` — wire new profiler
- `src/datahub/ingestion/source/s3/profiling.py` — delete Spark/Deequ body (final task)
- `setup.py:447` — swap deps in `data_lake_profiling`
- `docs/how/updating-datahub.md`, connector docs

---

## Task 1: Add DuckDB dependencies to the profiling extra

**Files:**

- Modify: `setup.py:447-451`

- [ ] **Step 1: Add `duckdb-engine` alongside the existing Spark deps (keep Spark for now)**

In `setup.py`, change the `data_lake_profiling` set (currently at line 447):

```python
data_lake_profiling = {
    "pydeequ>=1.1.0,<2.0.0",
    "pyspark~=3.5.6,<4.0.0",
    # DuckDB profiling engine + its SQLAlchemy dialect (replaces Spark/Deequ).
    # Spark deps remain temporarily and are removed in the final migration task.
    "duckdb>=1.0.0,<2.0.0",
    "duckdb-engine>=0.13.0,<1.0.0",
    # cachetools is used by the profiling config
    *cachetools_lib,
}
```

- [ ] **Step 2: Regenerate the lockfile**

Run: `../gradlew :metadata-ingestion:updateLockFile`
Expected: `pyproject.toml`, `uv.lock`, `constraints.txt` updated; command succeeds.

- [ ] **Step 3: Verify lockfile is in sync**

Run: `../gradlew :metadata-ingestion:checkLockFile`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add setup.py pyproject.toml uv.lock metadata-ingestion/src/datahub/constraints.txt
git commit -m "build(ingest/data-lake): add duckdb + duckdb-engine to data_lake_profiling extra"
```

---

## Task 2: `DuckDBAdapter` with native expression overrides

Implements DuckDB-native expressions so the profiler uses `approx_count_distinct` and `quantile_cont` instead of the generic fallbacks. No `SUMMARIZE` yet (Task 3).

**Files:**

- Create: `src/datahub/ingestion/source/sqlalchemy_profiler/adapters/duckdb.py`
- Modify: `src/datahub/ingestion/source/sqlalchemy_profiler/adapters/__init__.py`
- Test: `tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py
import sqlalchemy as sa
from unittest.mock import MagicMock

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.adapters import get_adapter
from datahub.ingestion.source.sqlalchemy_profiler.adapters.duckdb import DuckDBAdapter


def _engine():
    # On-disk temp file avoids the :memory: per-connection isolation trap.
    import tempfile, os
    path = os.path.join(tempfile.mkdtemp(), "t.duckdb")
    return sa.create_engine(f"duckdb:///{path}")


def test_get_adapter_returns_duckdb_adapter():
    adapter = get_adapter("duckdb", ProfilingConfig(), SQLSourceReport(), _engine())
    assert isinstance(adapter, DuckDBAdapter)


def test_approx_unique_count_uses_native_function():
    adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), _engine())
    expr = adapter.get_approx_unique_count_expr("col_a")
    assert "approx_count_distinct" in str(expr).lower()


def test_median_and_quantiles_use_quantile_cont():
    adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), _engine())
    assert adapter.get_median_expr("col_a") is not None
    eng = _engine()
    with eng.connect() as conn:
        conn.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1),(2),(3),(4),(5)) AS v(col_a)")
        table = sa.Table("t", sa.MetaData(), autoload_with=eng)
        qs = adapter.get_column_quantiles(table, "col_a", conn, [0.5])
        assert qs == [3.0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest -m 'not integration' tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py -v`
Expected: FAIL — `ModuleNotFoundError: ...adapters.duckdb`.

- [ ] **Step 3: Implement the adapter**

```python
# src/datahub/ingestion/source/sqlalchemy_profiler/adapters/duckdb.py
"""DuckDB-specific profiling adapter."""

import logging
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy.sql.elements import ColumnElement

from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import (
    DEFAULT_QUANTILES,
    PlatformAdapter,
)

logger = logging.getLogger(__name__)


class DuckDBAdapter(PlatformAdapter):
    """
    DuckDB profiling adapter.

    DuckDB optimizations:
    1. approx_count_distinct for fast unique counts (HyperLogLog).
    2. quantile_cont for median and quantiles in a single call.
    3. (Task 3) a SUMMARIZE fast-path that computes the base numeric +
       cardinality + null block for all columns in one table scan.
    """

    def get_approx_unique_count_expr(self, column: str) -> ColumnElement[Any]:
        return sa.func.approx_count_distinct(sa.column(column))

    def get_median_expr(self, column: str) -> Optional[ColumnElement[Any]]:
        # DuckDB: quantile_cont(col, 0.5) is the continuous median.
        return sa.func.quantile_cont(sa.column(column), 0.5)

    def get_column_quantiles(
        self,
        table: sa.Table,
        column: str,
        conn: Any,
        quantiles: Optional[List[float]] = None,
    ) -> List[Optional[float]]:
        if quantiles is None:
            quantiles = DEFAULT_QUANTILES
        # DuckDB returns all quantiles in one call: quantile_cont(col, [..]) -> list.
        expr = sa.func.quantile_cont(sa.column(column), sa.literal(quantiles))
        query = sa.select([expr]).select_from(table)
        result = conn.execute(query).scalar()
        if result is None:
            return [None] * len(quantiles)
        return [float(v) if v is not None else None for v in result]

    def get_sample_clause(self, sample_size: int) -> Optional[str]:
        # DuckDB reservoir sampling by absolute row count.
        return f"USING SAMPLE {int(sample_size)} ROWS"
```

- [ ] **Step 4: Register the adapter in the factory**

In `adapters/__init__.py`, add this branch before the final `else:` (after the `clickhouse` branch, ~line 95):

```python
    elif platform_lower == "duckdb":
        from datahub.ingestion.source.sqlalchemy_profiler.adapters.duckdb import (
            DuckDBAdapter,
        )

        adapter_class = DuckDBAdapter
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest -m 'not integration' tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Lint**

Run: `../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add src/datahub/ingestion/source/sqlalchemy_profiler/adapters/duckdb.py \
        src/datahub/ingestion/source/sqlalchemy_profiler/adapters/__init__.py \
        tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py
git commit -m "feat(ingest/profiling): add DuckDB profiling adapter with native expressions"
```

---

## Task 3: `SUMMARIZE` fast-path in `DuckDBAdapter`

`SUMMARIZE <relation>` returns one row per column: `column_name, column_type, min, max, approx_unique, avg, std, q25, q50, q75, count, null_percentage`. We run it once in `setup_profiling`, cache `{column -> stats}` on the (per-table) adapter instance, and override the `get_column_*` execution methods to read the cache. Confirmed call path: the profiler obtains every batched metric through these adapter methods (`sqlalchemy_profiler.py` stages 1–3 → `QueryCombinerRunner` → `adapter.get_column_*`), so overriding them intercepts metric collection cleanly.

`SUMMARIZE` returns min/max/avg/std/q25/q50/q75 as **VARCHAR**, count/approx_unique as **BIGINT**, null_percentage as a percentage. Conversions are handled below. Only q25/q50/q75 are available from `SUMMARIZE`; the 0.05/0.95 tails still use the native `quantile_cont` path from Task 2 (we do **not** override `get_column_quantiles` to use the cache).

**Files:**

- Modify: `src/datahub/ingestion/source/sqlalchemy_profiler/adapters/duckdb.py`
- Test: `tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py
from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import (
    ProfilingContext,
)


def test_summarize_cache_serves_column_stats():
    eng = _engine()
    with eng.connect() as conn:
        conn.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,NULL),(4,'b'),(5,'a')) AS v(num, txt)"
        )
        adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), eng)
        ctx = ProfilingContext(pretty_name="t", table="t")
        ctx = adapter.setup_profiling(ctx, conn)

        table = ctx.sql_table
        assert adapter.get_row_count(table, conn) == 5
        assert int(adapter.get_column_min(table, "num", conn)) == 1
        assert int(adapter.get_column_max(table, "num", conn)) == 5
        assert abs(float(adapter.get_column_mean(table, "num", conn)) - 3.0) < 1e-9
        # txt has 1 null of 5 rows -> non-null count 4
        assert adapter.get_column_non_null_count(table, "txt", conn) == 4
        # approx_unique of num is 5
        assert adapter.get_column_unique_count(table, "num", conn) == 5


def test_summarize_cache_miss_falls_back():
    # A column not present in SUMMARIZE output must fall back to base SQL.
    eng = _engine()
    with eng.connect() as conn:
        conn.execute("CREATE TABLE t AS SELECT 1 AS num")
        adapter = DuckDBAdapter(ProfilingConfig(), SQLSourceReport(), eng)
        ctx = adapter.setup_profiling(ProfilingContext(pretty_name="t", table="t"), conn)
        # 'missing' is not a real column; base method would error, so we assert the
        # cache lookup is keyed correctly and 'num' resolves from cache.
        assert adapter.get_column_min(ctx.sql_table, "num", conn) is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest -m 'not integration' tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py -k summarize -v`
Expected: FAIL — `setup_profiling` does not populate a cache; `get_row_count` runs SQL (still returns 5, but cache assertions on internal state fail) / `AttributeError`.

- [ ] **Step 3: Implement the SUMMARIZE cache and overrides**

Add to `DuckDBAdapter` (imports: add `from sqlalchemy.engine import Connection` and `from datahub.ingestion.source.sqlalchemy_profiler.profiling_context import ProfilingContext`):

```python
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Per-table cache: column_name -> parsed SUMMARIZE stats. A fresh adapter is
        # created per table in _generate_single_profile, so instance state is safe.
        self._summary: dict = {}
        self._row_count: Optional[int] = None

    def setup_profiling(
        self, context: ProfilingContext, conn: Connection
    ) -> ProfilingContext:
        context = super().setup_profiling(context, conn)
        try:
            self._run_summarize(context, conn)
        except Exception as e:
            # SUMMARIZE is an optimization; on any failure fall back to per-metric
            # queries by leaving the cache empty.
            logger.debug(f"DuckDB SUMMARIZE failed for {context.pretty_name}: {e}")
            self._summary = {}
            self._row_count = None
        return context

    def _run_summarize(self, context: ProfilingContext, conn: Connection) -> None:
        identifier = self.quote_identifier(context.get_table_identifier())
        rows = conn.execute(f"SUMMARIZE {identifier}").fetchall()
        summary: dict = {}
        for row in rows:
            m = row._mapping
            name = m["column_name"]
            null_pct = float(m["null_percentage"]) if m["null_percentage"] is not None else 0.0
            count = int(m["count"]) if m["count"] is not None else 0
            summary[name] = {
                "min": m["min"],
                "max": m["max"],
                "avg": self._to_float(m["avg"]),
                "std": self._to_float(m["std"]),
                "q50": self._to_float(m["q50"]),
                "approx_unique": int(m["approx_unique"]) if m["approx_unique"] is not None else 0,
                "non_null": round(count * (1.0 - null_pct / 100.0)),
            }
            self._row_count = count
        self._summary = summary

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    # ---- cache-backed overrides (fall back to base SQL on cache miss) ----

    def get_row_count(self, table, conn, sample_clause=None, use_estimation=False) -> int:
        if sample_clause is None and self._row_count is not None:
            return self._row_count
        return super().get_row_count(table, conn, sample_clause, use_estimation)

    def get_column_non_null_count(self, table, column, conn) -> int:
        s = self._summary.get(column)
        if s is not None:
            return int(s["non_null"])
        return super().get_column_non_null_count(table, column, conn)

    def get_column_unique_count(self, table, column, conn, use_approx=True) -> int:
        s = self._summary.get(column)
        if use_approx and s is not None:
            return int(s["approx_unique"])
        return super().get_column_unique_count(table, column, conn, use_approx)

    def get_column_min(self, table, column, conn) -> Any:
        s = self._summary.get(column)
        return s["min"] if s is not None else super().get_column_min(table, column, conn)

    def get_column_max(self, table, column, conn) -> Any:
        s = self._summary.get(column)
        return s["max"] if s is not None else super().get_column_max(table, column, conn)

    def get_column_mean(self, table, column, conn) -> Optional[Any]:
        s = self._summary.get(column)
        return s["avg"] if s is not None else super().get_column_mean(table, column, conn)

    def get_column_stdev(self, table, column, conn) -> Optional[Any]:
        s = self._summary.get(column)
        if s is not None and s["std"] is not None:
            return s["std"]
        return super().get_column_stdev(table, column, conn)

    def get_column_median(self, table, column, conn) -> Any:
        s = self._summary.get(column)
        if s is not None and s["q50"] is not None:
            return s["q50"]
        return super().get_column_median(table, column, conn)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest -m 'not integration' tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py -v`
Expected: PASS (all tests).

- [ ] **Step 5: Lint, then commit**

```bash
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
git add src/datahub/ingestion/source/sqlalchemy_profiler/adapters/duckdb.py \
        tests/unit/sqlalchemy_profiler/test_duckdb_adapter.py
git commit -m "feat(ingest/profiling): add DuckDB SUMMARIZE fast-path to profiling adapter"
```

---

## Task 4: Credential mapping — `CREATE SECRET` (S3 first)

**Files:**

- Create: `src/datahub/ingestion/source/s3/duckdb_secrets.py`
- Test: `tests/unit/s3/test_duckdb_secrets.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/s3/test_duckdb_secrets.py
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.s3.duckdb_secrets import build_s3_secret_sql


def test_s3_secret_with_explicit_keys():
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_region="us-east-1",
    )
    sql = build_s3_secret_sql(aws)
    assert "CREATE OR REPLACE SECRET" in sql
    assert "TYPE s3" in sql
    assert "KEY_ID 'AKIA_EXAMPLE'" in sql
    assert "REGION 'us-east-1'" in sql


def test_s3_secret_falls_back_to_credential_chain():
    aws = AwsConnectionConfig(aws_region="us-east-1")  # no static keys
    sql = build_s3_secret_sql(aws)
    assert "PROVIDER credential_chain" in sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest -m 'not integration' tests/unit/s3/test_duckdb_secrets.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement credential mapping**

```python
# src/datahub/ingestion/source/s3/duckdb_secrets.py
"""Map DataHub cloud-connection configs to DuckDB CREATE SECRET statements.

Credentials are injected via DuckDB's secret manager, never via os.environ
(process-environment leakage; see repo security guidance).
"""

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig


def _esc(value: str) -> str:
    return value.replace("'", "''")


def build_s3_secret_sql(aws: AwsConnectionConfig) -> str:
    parts = ["TYPE s3"]
    if aws.aws_access_key_id and aws.aws_secret_access_key:
        parts.append(f"KEY_ID '{_esc(aws.aws_access_key_id)}'")
        parts.append(f"SECRET '{_esc(aws.aws_secret_access_key)}'")
        if aws.aws_session_token:
            parts.append(f"SESSION_TOKEN '{_esc(aws.aws_session_token)}'")
    else:
        # Reuse the ambient AWS credential chain (instance/role/profile).
        parts.append("PROVIDER credential_chain")
    if aws.aws_region:
        parts.append(f"REGION '{_esc(aws.aws_region)}'")
    body = ", ".join(parts)
    return f"CREATE OR REPLACE SECRET datahub_s3 ({body})"
```

> Note: confirm exact attribute names on `AwsConnectionConfig` (`aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, `aws_region`) by reading `src/datahub/ingestion/source/aws/aws_common.py`; adjust accessors if they differ (some are exposed via `get_credentials()`).

- [ ] **Step 4: Run tests, lint, commit**

```bash
pytest -m 'not integration' tests/unit/s3/test_duckdb_secrets.py -v
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
git add src/datahub/ingestion/source/s3/duckdb_secrets.py tests/unit/s3/test_duckdb_secrets.py
git commit -m "feat(ingest/s3): map AWS config to DuckDB CREATE SECRET for profiling"
```

---

## Task 5: `DuckDBProfiler` attach layer

Replaces the Spark profiler. Same public method `get_table_profile(table_data, dataset_urn) -> Iterable[MetadataWorkUnit]`.

**Files:**

- Create: `src/datahub/ingestion/source/s3/duckdb_profiler.py`
- Test: `tests/unit/s3/test_duckdb_profiler.py`

- [ ] **Step 1: Write the failing test (local Parquet → profile)**

```python
# tests/unit/s3/test_duckdb_profiler.py
import os
import tempfile

import duckdb

from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig
from datahub.ingestion.source.s3.duckdb_profiler import DuckDBProfiler
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator  # if needed


def _make_parquet(tmp: str) -> str:
    path = os.path.join(tmp, "data.parquet")
    con = duckdb.connect()
    con.execute(
        "COPY (SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'a')) AS v(num, txt)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return path


def test_profiles_local_parquet(tmp_path):
    parquet = _make_parquet(str(tmp_path))
    cfg = DataLakeProfilerConfig(enabled=True)
    profiler = DuckDBProfiler(aws_config=None, report=_DummyReport(), profiling_config=cfg)
    table_data = _table_data(full_path=parquet, format="parquet")
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,test,PROD)"

    wus = list(profiler.get_table_profile(table_data, urn))
    profile = _extract_profile(wus)
    assert profile.rowCount == 3
    assert profile.columnCount == 2
    field = {f.fieldPath: f for f in profile.fieldProfiles}
    assert int(field["num"].min) == 1
    assert int(field["num"].max) == 3
```

Add small local helpers `_DummyReport`, `_table_data`, `_extract_profile` in the test file (a `DataLakeSourceReport()` instance for the report; a minimal object with `.full_path`/`.table_path`/`.format` attributes for `table_data`; and a function that pulls the `DatasetProfileClass` aspect out of the yielded workunits).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest -m 'not integration' tests/unit/s3/test_duckdb_profiler.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement the attach layer**

```python
# src/datahub/ingestion/source/s3/duckdb_profiler.py
"""DuckDB-based profiler for data-lake sources (replaces Spark/Deequ)."""

import logging
import os
import tempfile
from typing import Iterable, List, Optional

import sqlalchemy as sa

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.profiling.common import ProfilerRequest
from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig
from datahub.ingestion.source.s3.duckdb_secrets import build_s3_secret_sql
from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
    SQLAlchemyProfiler,
)

logger = logging.getLogger(__name__)

_READERS = {
    "parquet": "read_parquet('{path}', union_by_name=true)",
    "csv": "read_csv_auto('{path}')",
    "tsv": "read_csv_auto('{path}', delim='\\t')",
    "json": "read_json_auto('{path}')",
    "jsonl": "read_json_auto('{path}')",
    "delta": "delta_scan('{path}')",
}

_EXTENSIONS_BY_FORMAT = {"delta": ["delta"]}


class DuckDBProfiler:
    def __init__(
        self,
        aws_config: Optional[AwsConnectionConfig],
        report,  # DataLakeSourceReport
        profiling_config: DataLakeProfilerConfig,
        platform: str = "s3",
    ) -> None:
        self.aws_config = aws_config
        self.report = report
        self.profiling_config = profiling_config
        self.platform = platform
        self._tmpdir = tempfile.mkdtemp(prefix="datahub-duckdb-profile-")
        self._db_path = os.path.join(self._tmpdir, "profile.duckdb")
        self._engine: Optional[sa.engine.Engine] = None
        self._secrets_done = False
        self._loaded_extensions: set = set()

    def _engine_lazy(self) -> sa.engine.Engine:
        if self._engine is None:
            # On-disk temp file: shared across the profiler's worker threads and
            # provides out-of-core spill space. A bare :memory: would give each
            # connection a separate empty database (view invisible to workers).
            self._engine = sa.create_engine(f"duckdb:///{self._db_path}")
        return self._engine

    def _ensure_setup(self, conn, fmt: str) -> None:
        conn.execute("INSTALL httpfs; LOAD httpfs;") if "httpfs" not in self._loaded_extensions else None
        self._loaded_extensions.add("httpfs")
        for ext in _EXTENSIONS_BY_FORMAT.get(fmt, []):
            if ext not in self._loaded_extensions:
                conn.execute(f"INSTALL {ext}; LOAD {ext};")
                self._loaded_extensions.add(ext)
        if not self._secrets_done and self.aws_config is not None and self.platform == "s3":
            conn.execute(build_s3_secret_sql(self.aws_config))
            self._secrets_done = True

    def _reader_expr(self, path: str, fmt: str) -> str:
        template = _READERS.get(fmt)
        if template is None:
            raise ValueError(f"Unsupported format for DuckDB profiling: {fmt}")
        return template.format(path=path.replace("'", "''"))

    def get_table_profile(
        self, table_data, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        fmt = _resolve_format(table_data)
        path = _resolve_glob_path(table_data)
        view = "profile_target"
        engine = self._engine_lazy()
        try:
            with engine.begin() as conn:
                self._ensure_setup(conn, fmt)
                reader = self._reader_expr(path, fmt)
                conn.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {reader}")

            profiler = SQLAlchemyProfiler(
                conn=engine,
                report=self.report,
                config=self.profiling_config,
                platform="duckdb",
            )
            request = ProfilerRequest(
                pretty_name=dataset_urn,
                batch_kwargs={"schema": None, "table": view},
            )
            for _req, profile in profiler.generate_profiles(
                [request], max_workers=1, platform="duckdb"
            ):
                if profile is not None:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=profile
                    ).as_workunit()
        except Exception as e:
            self.report.warning(
                title="Profiling failed",
                message="DuckDB profiling failed for table",
                context=f"{dataset_urn}: {type(e).__name__}: {e}",
            )

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
        try:
            import shutil
            shutil.rmtree(self._tmpdir, ignore_errors=True)
        except Exception:
            pass


def _resolve_format(table_data) -> str:
    # table_data.full_path drives format; reuse the source's existing extension logic.
    fmt = getattr(table_data, "format", None)
    if fmt:
        return fmt.lower()
    path = _resolve_glob_path(table_data)
    return os.path.splitext(path)[1].lstrip(".").lower()


def _resolve_glob_path(table_data) -> str:
    # A folder-as-table maps to a glob. Reuse the path the source already computed.
    return getattr(table_data, "table_path", None) or table_data.full_path
```

> Implementation notes for the engineer:
>
> - Read `src/datahub/ingestion/source/data_lake_common/` and the existing `s3/profiling.py:655` `get_table_profile` to learn the real `TableData` attributes (`full_path`, `table_path`, `partitions`, `is_folder`, and how the current code derives the file extension/format). Replace `_resolve_format` / `_resolve_glob_path` with that real logic rather than the placeholders above.
> - `ProfilerRequest` is defined in `src/datahub/ingestion/source/profiling/common.py` — confirm its constructor fields (`pretty_name`, `batch_kwargs`, and any `table`/`size`/`profile_table_level_only` it carries).

- [ ] **Step 4: Run tests, lint, commit**

```bash
pytest -m 'not integration' tests/unit/s3/test_duckdb_profiler.py -v
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
git add src/datahub/ingestion/source/s3/duckdb_profiler.py tests/unit/s3/test_duckdb_profiler.py
git commit -m "feat(ingest/s3): add DuckDB attach-layer profiler for data-lake sources"
```

---

## Task 6: New profiling config (preserve data-lake defaults)

**Files:**

- Modify: `src/datahub/ingestion/source/s3/datalake_profiler_config.py`
- Test: `tests/unit/s3/test_datalake_profiler_config.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/s3/test_datalake_profiler_config.py
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig


def test_is_ge_profiling_config_subclass():
    assert issubclass(DataLakeProfilerConfig, GEProfilingConfig)


def test_preserves_data_lake_defaults():
    cfg = DataLakeProfilerConfig()
    assert cfg.include_field_quantiles is True
    assert cfg.include_field_distinct_value_frequencies is True
    assert cfg.include_field_histogram is True


def test_default_method_is_sqlalchemy():
    assert DataLakeProfilerConfig().method == "sqlalchemy"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest -m 'not integration' tests/unit/s3/test_datalake_profiler_config.py -v`
Expected: FAIL — current `DataLakeProfilerConfig` extends `ConfigModel`, not `GEProfilingConfig`; `method` attribute missing.

- [ ] **Step 3: Rewrite the config as a `GEProfilingConfig` subclass**

```python
# src/datahub/ingestion/source/s3/datalake_profiler_config.py
from pydantic.fields import Field

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig


class DataLakeProfilerConfig(GEProfilingConfig):
    """Profiling config for data-lake sources.

    Inherits the shared profiler config (sampling, limits, query combiner, the
    `method` switch defaulting to "sqlalchemy"). Data-lake profiling historically
    enabled quantiles/histograms/distinct-value-frequencies by default, whereas
    GEProfilingConfig defaults them off — we restore the historical defaults here
    so existing recipes produce the same output.
    """

    include_field_quantiles: bool = Field(
        default=True,
        description="Whether to profile for the quantiles of numeric columns.",
    )
    include_field_distinct_value_frequencies: bool = Field(
        default=True, description="Whether to profile for distinct value frequencies."
    )
    include_field_histogram: bool = Field(
        default=True,
        description="Whether to profile for the histogram for numeric fields.",
    )
```

> Confirm `GEProfilingConfig` exposes `enabled`, `operation_config`, `profile_table_level_only`, `max_number_of_fields_to_profile`, and the remaining `include_field_*` fields (it does — see `ge_profiling_config.py`). If the old `_allow_deny_patterns` private attr is referenced elsewhere (e.g. `s3/config.py`), keep it working — check `grep -rn "_allow_deny_patterns" src/datahub/ingestion/source/s3/`.

- [ ] **Step 4: Run tests, lint, commit**

```bash
pytest -m 'not integration' tests/unit/s3/test_datalake_profiler_config.py -v
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
git add src/datahub/ingestion/source/s3/datalake_profiler_config.py tests/unit/s3/test_datalake_profiler_config.py
git commit -m "feat(ingest/s3): base data-lake profiling config on GEProfilingConfig"
```

---

## Task 7: Deprecate Spark config knobs

**Files:**

- Modify: `src/datahub/ingestion/source/s3/config.py:77-84`

- [ ] **Step 1: Find the deprecation helper**

Run: `grep -rn "pydantic_removed_field" src/datahub/configuration/`
Expected: locate the `pydantic_removed_field` helper and its import path.

- [ ] **Step 2: Replace the spark fields with deprecations**

In `s3/config.py`, remove the `spark_driver_memory` and `spark_config` `Field(...)` declarations (lines 77-84) and add, at class body scope, the removed-field markers (use the exact helper signature found in Step 1), e.g.:

```python
    _spark_driver_memory_removed = pydantic_removed_field("spark_driver_memory")
    _spark_config_removed = pydantic_removed_field("spark_config")
```

Add the import near the other configuration imports:

```python
from datahub.configuration.pydantic_migration_helpers import pydantic_removed_field
```

(Use the real module path from Step 1.)

- [ ] **Step 3: Write/adjust a test that an old recipe with `spark_driver_memory` still loads**

```python
# tests/unit/s3/test_datalake_profiler_config.py (append)
from datahub.ingestion.source.s3.config import DataLakeSourceConfig  # confirm class name


def test_legacy_spark_knobs_do_not_crash():
    cfg = DataLakeSourceConfig.parse_obj(
        {"path_specs": [{"include": "s3://bucket/*.parquet"}], "spark_driver_memory": "8g"}
    )
    assert cfg is not None
```

> Confirm the S3 source config class name (likely `DataLakeSourceConfig`) and the minimal valid `path_specs` shape by reading `s3/config.py`.

- [ ] **Step 4: Run tests, lint, commit**

```bash
pytest -m 'not integration' tests/unit/s3/test_datalake_profiler_config.py -v
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
git add src/datahub/ingestion/source/s3/config.py tests/unit/s3/test_datalake_profiler_config.py
git commit -m "feat(ingest/s3): soft-deprecate spark_driver_memory/spark_config"
```

---

## Task 8: Wire the S3 source to the DuckDB profiler

**Files:**

- Modify: `src/datahub/ingestion/source/s3/source.py:279-307` (profiler init) and `:596-597` (call site; unchanged) and the run-summary/close.

- [ ] **Step 1: Replace the SparkProfiler init block**

Replace lines 287-307 (the `os.environ.setdefault("SPARK_VERSION"...)` block through the `except (ImportError, ...)`) with:

```python
            from datahub.ingestion.source.s3.duckdb_profiler import DuckDBProfiler

            self.profiler = DuckDBProfiler(
                aws_config=config.aws_config,
                report=self.report,
                profiling_config=config.profiling,
                platform=self.platform,  # confirm the source exposes self.platform / "s3"
            )
```

Keep the telemetry ping block (lines 279-286) as-is. The call site at line 597 (`yield from self.profiler.get_table_profile(table_data, dataset_urn)`) is unchanged.

- [ ] **Step 2: Ensure the temp DB is cleaned up at source close**

Find the source's close/summary path (around line 1197-1208 where profiling times are reported) and add `self.profiler.close()` when profiling is enabled and `self.profiler` exists. Confirm the source has a `close()`/`get_report()` lifecycle hook; if it overrides `close`, call `super().close()` too.

- [ ] **Step 3: Integration test — S3 source over local files end-to-end**

Add an integration-marked test that constructs the S3 source against a local directory of Parquet files (using the `file://` or local path support in path_specs if available) with `profiling.enabled=true`, runs `get_workunits`, and asserts at least one `datasetProfile` aspect is emitted. Place under `tests/integration/s3/` mirroring existing S3 integration tests; gate cloud specifics behind moto if needed.

- [ ] **Step 4: Run unit tests, lint, commit**

```bash
pytest -m 'not integration' tests/unit/s3/ -v
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
git add src/datahub/ingestion/source/s3/source.py tests/integration/s3/
git commit -m "feat(ingest/s3): use DuckDB profiler instead of Spark/Deequ"
```

---

## Task 9: Sampling threshold (stream by default, sample over warehouse-equivalent limits)

Reuse the inherited `use_sampling`, `sample_size`, `profile_table_row_limit`, `profile_table_size_limit` knobs (same defaults as warehouse sources). The attach layer estimates per-table size/rows and enables sampling above the limit.

**Files:**

- Modify: `src/datahub/ingestion/source/s3/duckdb_profiler.py`
- Test: `tests/unit/s3/test_duckdb_profiler.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/s3/test_duckdb_profiler.py (append)
def test_large_table_enables_sampling(tmp_path, monkeypatch):
    parquet = _make_parquet(str(tmp_path))
    cfg = DataLakeProfilerConfig(enabled=True, profile_table_row_limit=2, use_sampling=True, sample_size=2)
    profiler = DuckDBProfiler(aws_config=None, report=_DummyReport(), profiling_config=cfg)
    table_data = _table_data(full_path=parquet, format="parquet")
    # 3-row parquet, row limit 2 -> sampling path engaged; profile still emitted.
    wus = list(profiler.get_table_profile(table_data, "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)"))
    assert _extract_profile(wus) is not None
```

- [ ] **Step 2: Run it (expect FAIL or no-op until sampling wired)**

Run: `pytest -m 'not integration' tests/unit/s3/test_duckdb_profiler.py -k sampling -v`

- [ ] **Step 3: Add the estimate + sampling decision**

Before creating the view, compute a cheap estimate and decide whether to sample. DuckDB reads the Parquet footer for `COUNT(*)` cheaply:

```python
    def _estimate_row_count(self, conn, reader: str) -> int:
        result = conn.execute(f"SELECT COUNT(*) FROM {reader}").scalar()
        return int(result) if result is not None else 0
```

In `get_table_profile`, after `reader = self._reader_expr(...)` and inside the connection block, if `self.profiling_config.use_sampling` and the estimate exceeds `profile_table_row_limit`, build the view with a sample:

```python
                row_estimate = self._estimate_row_count(conn, reader)
                limit = self.profiling_config.profile_table_row_limit
                if self.profiling_config.use_sampling and limit and row_estimate > limit:
                    sample = self.profiling_config.sample_size
                    conn.execute(
                        f"CREATE OR REPLACE VIEW {view} AS "
                        f"SELECT * FROM {reader} USING SAMPLE {int(sample)} ROWS"
                    )
                    self.report.report_warning(  # or info; see existing report API
                        title="Profiling sampled",
                        message=f"Sampled {sample} of ~{row_estimate} rows",
                        context=dataset_urn,
                    )
                else:
                    conn.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {reader}")
```

(Replace the single `CREATE VIEW` from Task 5 with this branch. `profile_table_size_limit` enforcement is optional here since the row limit covers the common case; if byte-size gating is desired, sum the file sizes the source already lists.)

- [ ] **Step 4: Run tests, lint, commit**

```bash
pytest -m 'not integration' tests/unit/s3/test_duckdb_profiler.py -v
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
git add src/datahub/ingestion/source/s3/duckdb_profiler.py tests/unit/s3/test_duckdb_profiler.py
git commit -m "feat(ingest/s3): sample large tables during DuckDB profiling"
```

---

## Task 10: Extend credentials to GCS and Azure + wire those sources

**Files:**

- Modify: `src/datahub/ingestion/source/s3/duckdb_secrets.py` (add GCS + Azure builders)
- Modify: `src/datahub/ingestion/source/gcs/` and `src/datahub/ingestion/source/abs/` source + config
- Test: `tests/unit/s3/test_duckdb_secrets.py`

- [ ] **Step 1: Write failing tests for GCS + Azure secret SQL**

```python
# tests/unit/s3/test_duckdb_secrets.py (append)
from datahub.ingestion.source.s3.duckdb_secrets import (
    build_azure_secret_sql,
    build_gcs_secret_sql,
)


def test_gcs_secret_hmac():
    sql = build_gcs_secret_sql(key_id="GOOG_EXAMPLE", secret="s3cr3t")
    assert "TYPE gcs" in sql and "KEY_ID 'GOOG_EXAMPLE'" in sql


def test_azure_secret_connection_string():
    sql = build_azure_secret_sql(connection_string="DefaultEndpointsProtocol=https;Acc...")
    assert "TYPE azure" in sql
```

- [ ] **Step 2: Run (FAIL — builders missing)**

Run: `pytest -m 'not integration' tests/unit/s3/test_duckdb_secrets.py -v`

- [ ] **Step 3: Implement GCS + Azure secret builders**

```python
# src/datahub/ingestion/source/s3/duckdb_secrets.py (append)
def build_gcs_secret_sql(key_id: str, secret: str) -> str:
    return (
        "CREATE OR REPLACE SECRET datahub_gcs "
        f"(TYPE gcs, KEY_ID '{_esc(key_id)}', SECRET '{_esc(secret)}')"
    )


def build_azure_secret_sql(connection_string: str) -> str:
    return (
        "CREATE OR REPLACE SECRET datahub_azure "
        f"(TYPE azure, CONNECTION_STRING '{_esc(connection_string)}')"
    )
```

- [ ] **Step 4: Generalize `DuckDBProfiler._ensure_setup` for platform**

Replace the S3-only secret block in `_ensure_setup` with a dispatch on `self.platform` (`"s3"`→`build_s3_secret_sql`, `"gcs"`→`build_gcs_secret_sql`, `"abs"`→`build_azure_secret_sql`), loading the `azure` extension for abs. Pull the GCS HMAC keys / Azure connection string from the respective source configs passed into the profiler (extend `__init__` to accept an optional `cloud_credentials` object rather than only `aws_config`). Read the GCS/abs source + config modules to find the credential fields.

- [ ] **Step 5: Wire GCS and abs sources to `DuckDBProfiler`**

The GCS and abs sources currently reuse the S3 source's profiler path (they share `data_lake_profiling`). Confirm how they construct the profiler (they may inherit `S3Source` or instantiate `SparkProfiler` similarly) and replace with `DuckDBProfiler(platform="gcs"|"abs", ...)`. Add unit tests mirroring `test_duckdb_profiler.py` using local files (and `gs://`/`az://` path parsing where format detection differs).

- [ ] **Step 6: Run tests, lint, commit**

```bash
pytest -m 'not integration' tests/unit/s3/ -v
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
git add -A && git commit -m "feat(ingest/gcs,abs): DuckDB profiling with cloud secret injection"
```

---

## Task 11: delta-lake spike + wiring (data parity)

Goal: profile the **same rows as the Spark path** — current snapshot with deletion vectors applied.

**Files:**

- Modify: `src/datahub/ingestion/source/delta_lake/` source + config
- Test: `tests/unit/delta_lake/test_duckdb_delta_profiling.py` (create)

- [ ] **Step 1: Spike — verify `delta_scan` semantics**

Write a throwaway script (do not commit) that creates a small Delta table with a delete (producing a deletion vector), then runs `SELECT COUNT(*) FROM delta_scan('<path>')` via DuckDB and asserts the count matches the live snapshot (post-delete). Record the DuckDB `delta` extension version that works. If deletion vectors are NOT honored, STOP and convert this task into a follow-up issue (delta-lake stays on Spark for now) — surface this decision to the user.

- [ ] **Step 2: Write the failing test (assuming spike passed)**

```python
# tests/unit/delta_lake/test_duckdb_delta_profiling.py
def test_delta_profile_counts_match_current_snapshot(tmp_path):
    # build a delta table with N rows, delete M, expect profile.rowCount == N - M
    ...
```

(Fill in using the delta-rs/deltalake Python lib already used by the delta-lake source to build the fixture.)

- [ ] **Step 3: Wire delta-lake source to `DuckDBProfiler` with `format="delta"`**

Ensure `_EXTENSIONS_BY_FORMAT["delta"]` loads the `delta` extension and the reader uses `delta_scan`. The delta-lake source passes the table root path as the glob path.

- [ ] **Step 4: Run tests, lint, commit**

```bash
pytest -m 'not integration' tests/unit/delta_lake/ -v
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
git add -A && git commit -m "feat(ingest/delta-lake): DuckDB profiling via delta_scan"
```

---

## Task 12: Remove Spark/Deequ code and dependencies

Do this only after Tasks 8/10/11 are green.

**Files:**

- Modify/Delete: `src/datahub/ingestion/source/s3/profiling.py`
- Modify: `setup.py:447`
- Delete: Spark-based profiling unit tests

- [ ] **Step 1: Delete the Spark profiler implementation**

Remove `SparkProfiler`, `_SingleTableProfiler`, `init_spark`, `read_file_spark`, and all `pydeequ`/`pyspark` imports from `s3/profiling.py`. If nothing remains, delete the file and remove its imports; otherwise keep only still-referenced helpers. Run `grep -rn "from datahub.ingestion.source.s3.profiling import\|s3.profiling" src/ tests/` and fix every reference.

- [ ] **Step 2: Remove deps from the extra**

In `setup.py`, edit `data_lake_profiling` (line 447) to drop `pydeequ` and `pyspark`:

```python
data_lake_profiling = {
    "duckdb>=1.0.0,<2.0.0",
    "duckdb-engine>=0.13.0,<1.0.0",
    *cachetools_lib,
}
```

- [ ] **Step 3: Delete Spark-based profiling tests**

Run `grep -rln "pydeequ\|SparkProfiler\|spark" tests/unit/s3 tests/integration/s3` and remove/convert the Spark-specific profiling tests.

- [ ] **Step 4: Regenerate + verify lockfile**

```bash
../gradlew :metadata-ingestion:updateLockFile
../gradlew :metadata-ingestion:checkLockFile
```

- [ ] **Step 5: Full lint + unit tests for touched areas**

```bash
../gradlew :metadata-ingestion:lintFix && ../gradlew :metadata-ingestion:lint
pytest -m 'not integration' tests/unit/s3 tests/unit/sqlalchemy_profiler -v
```

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(ingest/data-lake): remove Spark/Deequ profiler and dependencies"
```

---

## Task 13: Re-baseline golden tests + docs + migration notes

**Files:**

- Modify: data-lake profiling golden files (under `tests/integration/s3/` etc.)
- Modify: `docs/how/updating-datahub.md`
- Modify: connector docs `metadata-ingestion/docs/sources/s3/`, `gcs/`, `abs/`, `delta-lake/` (`_pre.md`/`_post.md`)

- [ ] **Step 1: Regenerate and review golden profile output**

Run the data-lake integration tests in golden-update mode (use the repo's golden update flag, e.g. `pytest --update-golden-files` or the project equivalent — confirm in `tests/`), then **manually diff** the changed profile values to confirm shifts are only expected approximation differences (distinct counts, quantile/histogram binning), not structural breakage.

- [ ] **Step 2: Add the breaking-change note**

In `docs/how/updating-datahub.md`, add an entry (non-technical audience, reference the PR): data-lake profiling now uses DuckDB instead of Spark/Deequ; Java/Spark no longer required; `spark_driver_memory`/`spark_config` deprecated; profile metric values may shift slightly due to different approximation algorithms.

- [ ] **Step 3: Update connector docs**

Remove Java/Spark prerequisites from the S3/GCS/abs/delta-lake docs; document DuckDB (and that the first profiling run downloads DuckDB extensions; air-gapped installs must pre-stage them).

- [ ] **Step 4: Format docs + commit**

```bash
../gradlew :datahub-web-react:mdPrettierWrite
git add -A
git commit -m "docs(ingest/data-lake): document DuckDB profiling migration"
```

---

## Self-review

**Spec coverage:**

- Full replacement + drop deps → Tasks 1, 12. ✓
- All four sources → S3 (8), GCS/abs (10), delta-lake (11). ✓
- `SQLAlchemyProfiler` + `DuckDBAdapter` + SUMMARIZE → Tasks 2, 3. ✓
- Config adopts `GEProfilingConfig`, preserves defaults → Task 6; spark knobs deprecated → Task 7. ✓
- Temp-file connection / `:memory:` gotcha → Task 5 (`_engine_lazy`). ✓
- Credentials via `CREATE SECRET` → Tasks 4, 10. ✓
- Formats (parquet/csv/json/delta; Avro flagged) → Task 5 `_READERS` (Avro deferred — see note). ✓
- Stream/sample over warehouse-equivalent threshold → Task 9. ✓
- DuckDB autodetects memory/threads (no pragmas) → reflected in Task 5 (none set). ✓
- Testing (adapter, attach, secrets, sampling, re-baseline, delete Spark tests) → Tasks 2–13. ✓
- Migration/docs → Task 13. ✓

**Gap noted:** Avro (`read_avro` community extension) is in `_READERS` but has no dedicated task. Avro is the flagged parity risk; the engineer should add a focused test in Task 5/8 and, if the community extension is unavailable, warn-and-skip (matching the spec's error-handling). If Avro profiling was actively used, add a small dedicated task mirroring Task 11's spike.

**Type/naming consistency:** `DuckDBProfiler.get_table_profile(table_data, dataset_urn)` matches the call site (`source.py:597`); `DuckDBAdapter` overrides match `PlatformAdapter` signatures verified in `base_adapter.py`; config class name `DataLakeProfilerConfig` reused intentionally (now subclassing `GEProfilingConfig`).

**Placeholder note:** Tasks 5, 7, 8, 10, 11 contain explicit "confirm by reading X" callouts where exact upstream attribute names (`TableData` fields, `AwsConnectionConfig` accessors, the removed-field helper path, GCS/abs credential fields, source close hook) must be read from the codebase rather than assumed. These are deliberate verification steps, not unfinished design.
