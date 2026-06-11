# Design: Replace Spark/Deequ profiling in data-lake sources with DuckDB

**Date:** 2026-06-11
**Status:** Design approved, pending spec review
**Scope:** `metadata-ingestion` — S3, GCS, Azure Blob (`abs`), and `delta-lake` sources

## Problem & motivation

The data-lake sources (S3, GCS, abs, delta-lake) profile data with a self-contained
Spark + PyDeequ implementation in `src/datahub/ingestion/source/s3/profiling.py`
(~740 lines). This carries a heavy cost:

- **Dependencies:** `pyspark~=3.5.6` (~300 MB) + `pydeequ`, declared in the
  `data_lake_profiling` extra (`setup.py:447`).
- **A mandatory JVM/Java install** at runtime.
- **Slow startup:** local Spark session boot plus first-run Maven download of
  `hadoop-aws`, `spark-avro`, and the Deequ JAR.

Notably, the Spark profiler already runs in **local mode** (no `master` set →
`local[*]`, in-process JVM, `spark.driver.memory=4g` default). There is no cluster.
So the trade is _local Spark_ vs _local DuckDB_, not distributed vs single-node.

DuckDB is an in-process, vectorized, multi-threaded (morsel-driven), out-of-core
query engine with a SQLAlchemy dialect (`duckdb-engine`) and native readers for
Parquet/CSV/JSON/Delta over cloud object stores (`httpfs`). It can be driven by
DataHub's **existing SQLAlchemy profiler**, letting us delete the bespoke Spark
code rather than rewrite it.

## Goals

- Remove `pyspark` and `pydeequ` from the `data_lake_profiling` extra and eliminate
  the JVM/Java requirement.
- Profile data-lake datasets via the existing `SQLAlchemyProfiler` over a DuckDB
  connection, reusing its metric logic, query batching, sampling, and config surface.
- Preserve existing profile output and recipe compatibility as far as practical.
- Add a `DuckDBAdapter` (including a `SUMMARIZE` fast-path) reusable by any
  DuckDB-backed source, including the in-flight DuckDB ingestion source.

## Non-goals

- No distributed/cluster profiling (Spark local mode wasn't distributed either).
- No opt-back-to-Spark fallback — this is a full replacement.
- No changes to the DataHub profile aspect model (`DatasetProfileClass` /
  `DatasetFieldProfileClass` are reused unchanged).
- No unrelated refactoring of the SQLAlchemy profiler beyond adding the adapter.

## Key decisions

| Decision              | Choice                                                                                                              |
| --------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Spark/Deequ end state | **Full replacement** — delete it, drop the deps                                                                     |
| Source scope          | **All data-lake sources**: S3, GCS, abs, delta-lake                                                                 |
| Profiler              | Newer **`SQLAlchemyProfiler`** (`method: "sqlalchemy"`, now default); the GE profiler is deprecated and is NOT used |
| DuckDB optimization   | Full **`DuckDBAdapter`**: native expression overrides **+ `SUMMARIZE` fast-path**                                   |
| Config                | Adopt `GEProfilingConfig`, **preserve old data-lake defaults** via a subclass                                       |
| Large data            | **Stream by default, sample over threshold** — reuse the warehouse sources' existing sampling knobs/defaults        |
| Resource limits       | **Let DuckDB autodetect** memory/threads — no explicit pragmas                                                      |
| Expected performance  | **Neutral-to-faster** (no JVM/Maven startup; fast Parquet pushdown; vectorized aggregation)                         |

## Architecture

Two clean, independently testable pieces.

### Piece A — `DuckDBAdapter` (in the SQLAlchemy profiler)

Location: `src/datahub/ingestion/source/sqlalchemy_profiler/adapters/duckdb.py`,
registered in `adapters/__init__.py::get_adapter()` under platform `"duckdb"`.

The `SQLAlchemyProfiler` uses a `PlatformAdapter` strategy pattern: a registry maps a
platform name to an adapter that overrides SQL-expression builders and higher-level
getters; unknown platforms fall back to `GenericAdapter` (standard SQL). The
`DuckDBAdapter`:

- Overrides native expressions: `get_approx_unique_count_expr` (DuckDB
  `approx_count_distinct`), `get_median_expr` / `get_quantiles_expr`
  (`quantile_cont`), `get_sample_clause` (DuckDB `USING SAMPLE` / `TABLESAMPLE`).
- Adds a **`SUMMARIZE` fast-path**: `SUMMARIZE <table>` returns
  `count / approx_unique / min / max / avg / std / q25 / q50 / q75 / null_percentage`
  for **all columns in one statement**, mapping ~1:1 onto the profiler's Stage-2
  (cardinality/null) + Stage-3 (numeric stats) batches. The adapter runs `SUMMARIZE`
  once per table and populates those metrics from the result set, collapsing ~3
  combiner-batched scans into a single scan. Non-batchable extras (sample values,
  distinct-value-frequencies, histogram, the 0.05/0.95 quantile tails) run as today.

Because `GenericAdapter` already profiles any dialect correctly, the `DuckDBAdapter`
is a **performance layer, not a correctness prerequisite** — correctness and
optimization are separable and neither forks the shared profiler hot path.

### Piece B — Data-lake attach layer (rewrite of `s3/profiling.py`)

Single purpose: _given a table's file set + format + cloud credentials, produce
`DatasetProfileClass` workunits._ Replaces `SparkProfiler` / `_SingleTableProfiler` /
`init_spark` / `read_file_spark`. The source keeps calling
`self.profiler.get_table_profile(table_data, dataset_urn)` — unchanged interface, so
`s3/source.py` barely changes.

**Deleted:** the entire Spark/Deequ body of `s3/profiling.py`.
**Reused unchanged:** `sqlalchemy_profiler/` (`SQLAlchemyProfiler`, adapters, query
combiner), and the `ProfilerRequest`/`ProfilerConfig` abstraction.

## Data flow (per source run)

1. **Connection (once):** open a DuckDB connection backed by a **temp on-disk file**
   (not bare `:memory:` — see gotcha below). `INSTALL`/`LOAD` extensions on first use
   (`httpfs`; `azure` for abs; `delta` for delta-lake). Memory/threads are left to
   DuckDB's autodetection; the on-disk temp file's directory is DuckDB's default spill
   location, so out-of-core execution works without explicit pragmas.
2. **Credentials:** inject via `CREATE SECRET` (never env vars — repo security rule).
3. **Per table:** `CREATE OR REPLACE VIEW <t> AS SELECT * FROM <reader>(…)`.
4. **Delegate:** `SQLAlchemyProfiler(conn=duckdb_conn, platform="duckdb", config=…,
env=…)` profiles the view; yield the `DatasetProfileClass` MCP for the dataset URN.
5. **Cleanup (guaranteed via context manager/`finally`):** drop the view; at source
   end, close the connection and delete the temp DB file.

### The `:memory:` gotcha (critical)

A bare `duckdb:///:memory:` is unsafe here: the profiler is multi-threaded and each new
SQLAlchemy connection to `:memory:` is a **separate empty database**, so attached views
are invisible to worker threads ("table not found"). Fix: a **temp on-disk DuckDB file**
(all connections share it; also provides out-of-core spill space). A `StaticPool`
single-shared-connection is an alternative; the temp file is the recommended default.

### Credential injection

- **S3:** `CREATE SECRET (TYPE s3, KEY_ID …, SECRET …, [SESSION_TOKEN …], REGION …)`
  or `PROVIDER credential_chain` (reuse instance/role chain), mapped from `aws_config`.
- **Azure (abs):** `azure` extension + `CREATE SECRET (TYPE azure, …)`.
- **GCS:** DuckDB `gcs` secret (HMAC) via `httpfs`.

### Format → reader mapping

| Format       | DuckDB reader                                                 | Notes                                                                                                                                            |
| ------------ | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Parquet      | `read_parquet(glob, union_by_name=true, hive_partitioning=…)` | Glob = multi-file table; column/stat pushdown                                                                                                    |
| CSV / TSV    | `read_csv_auto(…, delim, header)`                             | Carry over source CSV settings                                                                                                                   |
| JSON / JSONL | `read_json_auto(…)`                                           |                                                                                                                                                  |
| Delta        | `delta_scan('s3://…')`                                        | delta-lake source; must read the **current snapshot honoring deletion vectors** so the profiled rows match the Spark path (see validation below) |
| **Avro**     | `read_avro(…)` (community extension)                          | **Parity risk** — community extension, less mature than Spark's reader                                                                           |

A folder-as-table maps to a glob; the source already knows the file list and feeds it
directly rather than re-listing.

## Configuration

Switch the data-lake `profiling:` block from `DataLakeProfilerConfig` to a subclass of
the shared `GEProfilingConfig` that preserves historical defaults:

```python
class DataLakeProfilerConfig(GEProfilingConfig):
    # Data-lake profiling historically defaulted these ON; GEProfilingConfig defaults OFF.
    include_field_quantiles: bool = True
    include_field_distinct_value_frequencies: bool = True
    include_field_histogram: bool = True
```

- **Field continuity:** every existing data-lake knob (`enabled`,
  `profile_table_level_only`, `max_number_of_fields_to_profile`, all `include_field_*`,
  `operation_config`) exists in `GEProfilingConfig` with matching names → existing
  recipes keep working; users additionally gain `method`, sampling, and row/size limits.
- **`profile_patterns`** (table selection) and column allow/deny patterns stay in the
  source config and are wired into the profiler as today.
- **Removed knobs:** `spark_driver_memory`, `spark_config` → soft-deprecate via
  `pydantic_removed_field` (clean message, not a crash).
- **No new sizing knobs.** Sampling reuses the **same fields and defaults the
  warehouse sources use**: `use_sampling`, `sample_size` (10,000),
  `profile_table_row_limit` (5M), `profile_table_size_limit` (5GB). Memory/threads are
  DuckDB-autodetected — no DataHub-side knobs added.

## Large data, concurrency & errors

**Stream-by-default, sample-over-threshold:**

- Compute a cheap per-table estimate before profiling — row count from the Parquet
  footer (no full scan) and total bytes from the file listing.
- Below threshold → profile the full view. Above the warehouse-equivalent threshold
  (`profile_table_row_limit` / `profile_table_size_limit`) → enable sampling via
  `DuckDBAdapter.get_sample_clause` (the profiler's existing sampling path, same hook
  BigQuery uses, same defaults the db sources use).
- **Resource bounds:** left to DuckDB's autodetection. Out-of-core spill works
  automatically because the connection is backed by an on-disk temp file (whose
  directory is DuckDB's default spill location). No explicit `memory_limit`/`threads`.

**Concurrency:** the temp-file-backed DuckDB lets all profiler worker threads see the
same DB and the attached view; DuckDB serves concurrent reads safely.

**Error handling — layered, fail-soft per table:**

- Extension/credential/access failures → caught at the table boundary, `report.warning`
  with the dataset URN, continue to the next table.
- Avro: if the community `avro` extension is not loadable, warn and skip that table.
- Per-column failures → absorbed by the profiler's `catch_exceptions` config.
- Unsupported/nested types (STRUCT/LIST/MAP/JSON) → skipped via the profiler's
  `_should_ignore_column` + `DuckDBAdapter` type mapping.
- Cleanup guaranteed via context manager/`finally`.

## Testing

DuckDB being in-process is a test-ergonomics win (no JVM, no Docker for core paths).

- **`DuckDBAdapter` unit tests:** native expression generation and **`SUMMARIZE`
  result parsing → field profiles**, against a real in-memory DuckDB over tiny local
  Parquet/CSV fixtures. Hermetic, no mocks.
- **Attach-layer tests:** local files (`read_parquet`/`read_csv_auto`/`read_json`) →
  view → profile → assert profile values. Covers format dispatch, multi-file globs,
  empty tables (reuse the `test_ge_empty_table_profile.py` pattern for `rowCount=0`),
  nested-type skipping.
- **Credential-mapping unit tests:** assert correct `CREATE SECRET` SQL from
  `aws_config`/Azure/GCS — no network.
- **Sampling-threshold tests:** below → full scan; above → sample clause applied.
- **Cloud integration:** one lightweight moto/localstack-style S3 test to prove
  `httpfs` + secret wiring end-to-end.
- **Re-baseline + delete:** existing data-lake profiling golden files will shift
  (different engine + approximation algorithms) → re-baseline as an explicit reviewed
  change. Delete the Spark/Deequ profiling tests.
- Verify with `:metadata-ingestion:lintFix` then `:metadata-ingestion:lint` (mypy).

## Dependencies & packaging

`setup.py` is the source of truth.

- `data_lake_profiling` extra: **remove** `pyspark` and `pydeequ`; **add** `duckdb` and
  `duckdb-engine` (version ranges with a comment, not pins).
- DuckDB extensions (`httpfs`, `delta`, `azure`, community `avro`) are runtime
  `INSTALL`/`LOAD`, not pip deps — document the first-run network fetch; air-gapped
  deployments may need pre-staged extensions.
- Run `:metadata-ingestion:updateLockFile` after the `setup.py` edit (CI `checkLockFile`
  enforces sync).
- **Headline win:** JVM/Java requirement removed; the extra shrinks dramatically.

## Migration / breaking changes

Document in `docs/how/updating-datahub.md` (non-technical audience, PR-referenced):

- **Behavioral:** profile metric _values_ may shift slightly — `approx_count_distinct`,
  quantile/histogram algorithms differ from Deequ's. Expected, not a regression.
- **Removed config:** `spark_driver_memory` / `spark_config` soft-deprecated.
- **No Spark fallback:** full replacement, no opt-back switch.
- **Docs:** update S3/GCS/abs/delta-lake connector docs (`_pre.md`/`_post.md`) to drop
  Java/Spark prerequisites and document new DuckDB knobs.

## Risks

- **Avro** via community extension is the weakest format vs Spark's mature reader.
- **Profile value drift** from different approximation algorithms — re-baseline needed.
- **Air-gapped installs** need pre-staged DuckDB extensions (no `INSTALL` egress).
- **`SUMMARIZE` only yields q25/q50/q75** — the 0.05/0.95 tails still need a separate
  `quantile_cont` query (already non-batchable, so no regression).
- **Delta data parity:** `delta_scan` must profile the same rows the Spark path did
  (current snapshot, deletion vectors applied). Validated by an early spike — if the
  `delta` extension can't honor this, delta-lake is split into a follow-up rather than
  shipping divergent profiles. Goal: **profile the same data as before.**

## Success criteria

- `pyspark`/`pydeequ` gone from `data_lake_profiling`; no JVM needed to profile.
- S3/GCS/abs/delta-lake emit `DatasetProfileClass` with parity (modulo documented
  approximation drift) via `SQLAlchemyProfiler` + `DuckDBAdapter`.
- `SUMMARIZE` fast-path active for DuckDB; profiling latency neutral-to-better.
- Lint/mypy clean; lockfile in sync; golden tests re-baselined; Spark tests removed.

## Resolved during review

1. **Sample threshold:** reuse the warehouse sources' existing knobs/defaults
   (`use_sampling`, `sample_size=10k`, `profile_table_row_limit=5M`,
   `profile_table_size_limit=5GB`) — no data-lake-specific threshold.
2. **Memory/threads:** let DuckDB autodetect — no DataHub-side pragmas.
3. **delta-lake:** stays in scope with the goal of **profiling the same data as
   before**. An early implementation spike validates that DuckDB `delta_scan` reads the
   current snapshot with deletion vectors applied; if it can't, delta-lake becomes a
   follow-up rather than shipping divergent profiles.
