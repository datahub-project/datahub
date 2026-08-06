# MySQL Data Profiling Performance — Design Spec

Date: 2026-07-28
Status: Draft, pending human review
Scope: `metadata-ingestion` profiling against MySQL (Aurora MySQL 3.x / MySQL 8.0-compatible).

## 1. Problem statement

A customer runs DataHub ingestion with profiling enabled against MySQL (Aurora MySQL 3.x) tables of ~500M–1B rows. Their end goal is Volume and Column Metric assertions on a small set of core business tables; they do not need profiling across all MySQL databases. Profiling currently produces SQL that:

- runs inside a single transaction lasting **5–20+ hours**, causing InnoDB undo log growth that degrades their other production services;
- performs **`columns × metrics` full table scans** where one scan would do;
- more recently, **OOMs** during ingestion.

The first two are from the customer's written analysis; the OOM is the symptom that actually blocked them after they applied the advice they were given (see §3.2 framing correction). The long-transaction mechanism is real and PR 1 makes it deterministic, but it is not what blocked the customer and we have no evidence they hit it.

The customer submitted a competent written analysis with three issues and proposed fixes. Their analysis is largely correct; one place it is wrong in a way that matters is called out in §3.7.

This spec covers only the engineering implementation. Customer-facing comms are out of scope.

## 2. Approach

"MySQL-safe first, generalize later." Land low-blast-radius fixes first; put the risky shared-code change behind a flag defaulted off; flip the default only after validation. PR 2 is scoped to MySQL-only config overrides so it stays non-breaking. PR 4 is inherently cross-platform: it changes shared combiner code (`utilities/sqlalchemy_query_combiner.py`), so every SQL source benefits once the flag flips. Only PRs 1–2 are MySQL-scoped.

## 3. Verified root causes

All paths relative to `metadata-ingestion/src/datahub/` unless stated. All references spot-checked on `master` on 2026-07-28; no drift found.

### 3.1 SQLAlchemy profiler is the default; GE is deprecated

`ingestion/source/ge_profiling_config.py:24-36` — `ProfilingMethodConfig.method` defaults to `"sqlalchemy"`. `ge` is deprecated and gated behind `pip install 'acryl-datahub[profiling-ge]'`. Support previously advised the customer to add `method: "sqlalchemy"`; that is now a no-op and never addressed issues 2/3 because those live in shared code (3.3).

### 3.2 Issue 1 — long transaction. REAL mechanism, but NOT the customer's reported symptom.

`ingestion/source/sqlalchemy_profiler/sqlalchemy_profiler.py:1603` opens `with self.base_engine.connect() as conn:` and holds it across all three flush stages for one table. No isolation level or autocommit is set, so the driver's implicit transaction spans every query for that table. PR 1's empirical check (MySQL 8, general log) confirmed the mechanism: pymysql issues `SET AUTOCOMMIT = 0` on connect by default, so all profiling SELECTs for a table run inside one open transaction that only ends on pool return. On a large table held for hours this grows the InnoDB undo log.

**Important framing correction:** the customer did **not** report a persisting transaction as their symptom. Going back to the ticket, what they reported was an OOM ("We encountered the Out of Memory error which did not happened before, while the rows of testing table remained ~500M rows") after applying the advice they'd been given. The long-transaction mechanism above is a real latent issue that PR 1 makes deterministic, but it is not what blocked the customer and we have no evidence they ever hit it. PR 1's honest claim is therefore **"make autocommit deterministic and on by default,"** not "fix the broken transaction the customer couldn't." Deterministic beats accidental (an Aurora parameter group with `autocommit=0` would break the accidental path), so it's still worth shipping — but the customer-facing story must not become "we fixed the thing your workaround couldn't."

Safe to fix for MySQL because `MySQLAdapter` (`ingestion/source/sqlalchemy_profiler/adapters/mysql.py`, 117 lines) does not override `setup_profiling`; its class docstring states it uses the default. The default at `base_adapter.py:65-105` only builds a SQLAlchemy `Table` object — no temp tables/views. Five adapters override `setup_profiling` — Athena, ClickHouse, BigQuery, Trino, and Snowflake — and four of those (BigQuery, Athena, Trino, Snowflake) also override `cleanup`, meaning they create resources requiring teardown. ClickHouse overrides `setup_profiling` but not `cleanup`, so it may only customize table construction; it needs its own review rather than being assumed safe or unsafe. MySQL and Postgres override neither, which is what makes them safe to switch. (`base_adapter.py:76-77`'s docstring names only "BigQuery, Athena, and Trino" — it is also incomplete.) SQLAlchemy is pinned `>=1.4.39,<2` (`metadata-ingestion/setup.py:212`), so `connect().execution_options(isolation_level="AUTOCOMMIT")` is available.

Open hypothesis that PR 1's empirical check **disproved in its strong form**: the customer tried `options: {connect_args: {autocommit: true}}` and reported it "not working so well." We inferred this meant the transaction persisted. The empirical check shows that with pymysql + a server default of `autocommit=ON` (Aurora's default parameter group), `connect_args={"autocommit": True}` _does_ avoid the long transaction — by skipping pymysql's default `SET AUTOCOMMIT = 0` and relying on the server default. So autocommit probably _did_ work for them transactionally, and "not working so well" referred to the OOM that followed, not a transaction. The dialect-level `isolation_level="AUTOCOMMIT"` path is still the correct mechanism (it deterministically issues `SET AUTOCOMMIT = 1` regardless of server default or driver quirks), but the justification is "deterministic over accidental," not "fixes what connect_args couldn't."

### 3.3 Issues 2 and 3 — N full scans per statement. REAL. Root cause located.

`utilities/sqlalchemy_query_combiner.py:330-346` builds one CTE per queued query and `append_from(cte)`s them all into one `SELECT`, producing a cross-join of N one-row CTEs — exactly the SQL shape in the customer's report. Each CTE is an independent aggregate over the same table, so MySQL scans the table once **per CTE**.

The SQLAlchemy profiler activates this combiner at `sqlalchemy_profiler.py:1037-1047` via `QueryCombinerRunner`, which wraps `SQLAlchemyQueryCombiner`. Three flush points per table: `:1148` (row count), `:1253` (cardinality), `:1463` (numeric stats) — which is why the customer saw a few distinct statements.

Every batched metric is the same shape — `sa.select([<agg>]).select_from(table)` with no WHERE / GROUP BY / ORDER BY / LIMIT — at `base_adapter.py` lines 322 (row count), 347 (non-null count), 363 (min), 378 (max), 402 (mean), 424 (stddev), 470 (unique count), 489 (median). Trivially mergeable into one flat `SELECT`.

Result extraction at `sqlalchemy_query_combiner.py:367-386` maps results by `col.name` (line 375-378) and asserts `index == len(row)` at line 386. This is the code flattening must change, and the main regression risk. SQLAlchemy 1.4 requires `.subquery()` before reading `.columns` (lines 370-372). Serial fallback at `:388-425`; `flush()` at `:427-455` loops `_execute_queue` with serial fallback on exception.

`MAX_QUERIES_TO_COMBINE_AT_ONCE = 40` (line 31) caps queries per combined statement.

Not verified: the customer's "4 statements × 4 = 101 scans" framing. The per-statement scan multiplication is confirmed; the ×4 repetition is not explained by the code (one `_execute_queue` pass per `flush()`). Possible explanations include multiple tables/partitions in the run or >40 queries splitting into multiple batches. Ask the customer for their logs in the PR 5 writeup rather than asserting a mechanism.

#### 3.3.1 Why this hasn't surfaced before

Cost is `columns × metrics` scans, invisible until one scan is expensive. Most customers profiling large tables are on Snowflake/BigQuery, where (a) `profile_table_row_limit` defaults to 5M **and is enforced**, so oversized tables get skipped, and (b) columnar scans of a few aggregate columns are cheap. MySQL is the first case where all three go wrong at once: no working guardrail (3.4), no sampling (3.6), and row-store full scans.

### 3.4 GAP — MySQL has no row-count or size guardrail

More impactful than the customer's three issues, and they didn't find it. Enforcement lives at `ingestion/source/sql/sql_generic_profiler.py:310-328` (`profile_table_size_limit` at 310-318, `profile_table_row_limit` at 320-328), but that path is only reached when `profile_candidates` is populated, which requires `generate_profile_candidates`. The base implementation at `sql_common.py:1389-1395` raises `NotImplementedError`; implementors are only `oracle.py` and `teradata.py` (grep-confirmed). MySQL flows through `sql_common.loop_profiler_requests` (`:1418-1465+`), where the `NotImplementedError` is swallowed at `:1446-1447` leaving `profile_candidates = None`, then `is_dataset_eligible_for_profiling` (`:1398-1416`) checks **patterns only**. Config annotations confirm the gap: `profile_table_size_limit` (`ge_profiling_config.py:153-164`) and `profile_table_row_limit` (`:166-173`) do not list `mysql`.

There is currently no way to tell MySQL "don't profile tables over N rows." A 1B-row table gets fully profiled. MySQL _can_ answer this cheaply — the adapter already queries `information_schema.tables.table_rows` (`adapters/mysql.py:75-117`), and `data_length` is available in the same table. `MySQLSource` (`sql/mysql.py:271`) extends `TwoTierSQLAlchemySource`, so `generate_profile_candidates` can be overridden there, mirroring Oracle/Teradata. Report counters already exist: `profiling_skipped_row_limit`, `profiling_skipped_size_limit`.

### 3.5 GAP — `max_workers` default is aggressive, and the pool is widened to match

`ge_profiling_config.py:108-111` — `max_workers` defaults to `5 * (os.cpu_count() or 4)` — 40+ on an 8-core box. Each worker profiles a different table concurrently, each holding its own connection and issuing full-scan statements at the same MySQL instance (`sqlalchemy_profiler.py:1030` clamps to `len(requests)`; ThreadPoolExecutor at `:1037-1041`).

Critically, the connection pool is deliberately widened to match: `sql_common.py:373-380` sets `options["max_overflow"] = profiling.max_workers` when profiling is enabled. So the default pool (size=5, max_overflow=10 → ~15 connections) becomes size=5 + max_overflow=40 → ~45 concurrent connections. `max_workers` is not just a thread count — it is a connection-count multiplier with the natural brake removed on purpose. On an 8-core box: ~40 concurrent sessions, each full-scanning a 500M-row table, each potentially holding multi-GB `COUNT(DISTINCT)` structures (3.7). Server-side OOM on Aurora is very plausible. **Leading suspect for the OOM.**

### 3.6 GAP — sampling does not exist for MySQL

`base_adapter.py:213-223` — `get_sample_clause` returns `None`; MySQL does not override it. `ge_profiling_config.py:207-217` — `use_sampling`/`sample_size` are annotated `SupportedSources(["bigquery", "snowflake"])`. Not a regression — the deprecated GE profiler also only sampled BigQuery/Athena. But it means skipping is currently the only lever for a huge MySQL table, which strengthens the case for 3.4. **Out of scope for this work — noted as a gap, not fixed.**

### 3.7 The customer's proposed fix has a memory trade-off — IMPORTANT

Their "merge everything into one SELECT" puts 17 `COUNT(DISTINCT colN)` in a single statement. MySQL materializes CTEs one at a time, freeing each distinct-value structure after use. Flattening makes all 17 distinct-trees **coexist**, raising peak server memory. `MySQLAdapter.get_approx_unique_count_expr` (`adapters/mysql.py:32-45`) returns **exact** `COUNT(DISTINCT col)` — MySQL has no approximate distinct function, so there is no cheap escape. Naive flattening trades a scan problem for a memory problem and is a plausible contributor to the OOM. **The grouping logic in PR 4 must cap memory-heavy aggregates (`COUNT(DISTINCT)`) per statement separately from cheap ones.** Do not ship the customer's SQL as-is.

### 3.8 Cardinality is computed from real counts — rules out one OOM theory

`sqlalchemy_profiler.py:1359-1366` calls `convert_to_cardinality` (`profiling/common.py:20-56`) using `unique_count / non_null_count`, where `non_null_count` is a real `COUNT(col)`, not the row-count estimate. So `profile_table_row_count_estimate_only: true` does **not** cause cardinality misclassification and does not trigger unbounded distinct-value-frequency queries. Do not chase that theory. (There _is_ an unbounded query — `get_column_distinct_value_frequencies` at `base_adapter.py:722-768` does a `GROUP BY` with no LIMIT — but it is gated on low cardinality at `sqlalchemy_profiler.py:557-604`, allowed sets at `:868-870`/`:903-906`, and that gate is trustworthy per the above.)

### 3.9 Test surface

- **No unit test for `utilities/sqlalchemy_query_combiner.py` itself.** Only the facade is tested, at `tests/unit/sqlalchemy_profiler/test_query_combiner_runner.py` (find-confirmed).
- `tests/unit/sqlalchemy_profiler/` — `test_adapters.py`, `test_profiling_context.py`, `test_query_combiner_runner.py`, `test_sqlalchemy_profiler.py`, `test_type_mapping.py`.
- `tests/unit/test_mysql_profiling.py`.
- `tests/integration/mysql/` — `docker-compose.yml`, `test_mysql.py`, golden files including `mysql_table_level_only.json`, `mysql_table_row_count_estimate_only.json`, `mysql_mces_with_db_golden.json`.
- `tests/integration/sqlalchemy_profiler/` — **postgres only**. No MySQL integration coverage of the profiler.

## 4. Open decisions (settled with the human)

### 4.1 `profile_table_row_limit` / `profile_table_size_limit` defaults for MySQL

**Decision: ship `None` (opt-in) for MySQL.** Via a new `MySQLProfilingConfig(GEProfilingConfig)` overriding both fields to `None`, following the established per-source override pattern (`AthenaProfilingConfig` at `sql/athena.py:101-106` overrides `partition_profiling_enabled`; Dremio's `ProfileConfig` at `dremio/dremio_config.py:114-121` forces `include_field_median_value=False`; Kafka and Unity Catalog do the same).

Rationale:

- Per-source profiling-default overrides are an established pattern, not an inconsistency. The codebase convention is "override the shared default where the platform's behavior differs" — exactly this situation.
- The 5M shared default is miscalibrated for MySQL by ~100×. It is tuned to Snowflake/BigQuery billing, not "InnoDB will melt." A 10–50M row MySQL table profiles fine; the customer's pain starts around 500M–1B. Cutting at 5M would deny profiles to a large population of healthy MySQL tables to solve a problem that appears ~100× higher up.
- Silently dropping profiles silently breaks assertions. Profiles feed Volume and Column Metric assertions. A user whose 20M-row table stops being profiled gets no error — they get a monitor that quietly stops evaluating. That is strictly worse than slow profiling, and it is precisely the use case driving this ticket.
- It works against the customer we are building this for. Under a 5M default they must set it to `null` to keep profiling their 500M-row tables — at which point they have zero protection and we have shipped them a migration step with no benefit.
- The acute harm is fixed by PR 1, not PR 2. The production damage is the 5–20h transaction and undo log growth. Autocommit fixes that regardless of guardrails. PR 2 is about controlling cost and duration, not preventing incidents — so it does not need to carry safety-by-default at the price of breaking existing profiles.

**Consequence: PR 2 is non-breaking.** No `docs/how/updating-datahub.md` entry, no migration story, and it unblocks to land right after PR 1.

**The `None` limits deliberately propagate to MySQL-protocol descendants** (Doris, TiDB, MariaDB — all inherit `MySQLConfig`/`MySQLSource`). This is not an oversight; it is the same decision extended for a specific reason: PR 2 newly implements `generate_profile_candidates` (the enforcement mechanism) for the MySQL family — before PR 2 the base method raised `NotImplementedError`, so the limits were configured-but-never-enforced for every MySQL-protocol source. A non-None default on a descendant is therefore an _activation_, not a _restoration_ — it would silently drop profiles for tables over 5M rows using `information_schema.tables.table_rows` semantics that Doris (MPP) and TiDB (distributed HTAP) do not share with InnoDB. That is precisely the failure mode rejected for MySQL above. `max_workers` and `report_expensive_tables` are reverted by `DorisProfilingConfig`/`TiDBProfilingConfig` (single-primary-row-store rationale and MySQL-specific remediation advice don't apply to them); MariaDB inherits all four (it IS a single-primary row store, MySQL fork). The per-field decision is encoded in `test_mysql_profiling_overrides_do_not_drift` so a fifth MySQL override forces a deliberate subclass decision. If we ever want the guardrail active by default on any of these, it needs reliable stats plus a `docs/how/updating-datahub.md` entry — a separate decision, not a side effect of this PR.

**Opt-in only works if operators can discover they need it.** Today the skip is invisible at default log level — `logger.debug` plus a report counter (`sql_generic_profiler.py:320-328`). PR 2 must also make the cost visible. Warn on **observed cost, not predicted size**, and aggregate per run:

- After the run, emit **one** `self.report.warning(...)` naming the few most expensive tables by actual profiling time, with the config to set (`profile_table_row_limit` / `profile_table_size_limit`). The timing machinery already exists: per-table `time_taken = timer.elapsed_seconds()` at `sqlalchemy_profiler.py:1806-1809` and `self.times_taken` at `:1074`/`:1810`. `self.times_taken` is a list of floats (not name+time pairs), so PR 2 adds a small `(pretty_name, time_taken)` tracker to name the top-N — a few lines.
- This is self-limiting (one warning per run regardless of table count), calibrated to reality rather than a guessed row count, and actionable — it names the tables actually costing money.
- Keep the existing `profiling_skipped_row_limit` / `profiling_skipped_size_limit` counters so skips are attributable once someone does configure a limit.

A row-count threshold for the warning was rejected: §4.1 rejects 5M as a limit because it is miscalibrated for MySQL by ~100×, so 5M is also the wrong place to warn — it would fire on healthy 10–50M row tables that profile fine, and `report.warning` surfaces prominently in the ingestion report and DataHub Cloud's run UI, so spamming it trains operators to ignore warnings (which destroys the justification for choosing opt-in).

**Future work (not in this plan):** the profiler already pings telemetry with discretized `total_row_count` and `platform` (`sqlalchemy_profiler.py:1087-1096`). If that data shows the real-world MySQL distribution, a defensible default becomes a follow-up with evidence behind it rather than a guess now. Treat "revisit the default in a future major version" as genuinely contingent on actually pulling that data — otherwise the warning is doing the real work.

### 4.2 `max_workers` for MySQL

**Decision: ship a MySQL-specific `max_workers` override inside the same `MySQLProfilingConfig` (folded into PR 2), proposed value ~5, pending measurement.** Do **not** touch the shared default in `GEProfilingBaseConfig`.

Rationale:

- Lowering the shared default hits every profiling source. For BigQuery and Snowflake, 40 concurrent queries is unremarkable — they scale horizontally and high concurrency is desirable. Lowering globally is a real performance regression for those users, needs its own benchmarking to pick a number, and has nothing to do with this ticket. Raise separately.
- A MySQL-specific default costs almost nothing because PR 2 already introduces `MySQLProfilingConfig`. Adding the override is a few lines in a class we are creating anyway, with the same Athena/Dremio precedent and zero blast radius outside MySQL.
- The rationale is platform-specific: MySQL is a row store on a single primary, so profiling throughput is bound by the same buffer pool and IO path no matter how many sessions you open. Concurrency past a handful mostly adds contention and multiplies peak memory rather than adding throughput. That argument does not apply to warehouses, which is exactly why the shared default should not move.
- Classification: performance default, not correctness. Profiles still get emitted, just less concurrently. Much safer class than PR 2's original framing — nothing disappears. Note in the PR description; does not warrant `updating-datahub.md`.

**On the number:** ~5 is a starting point, not a validated figure. It should be benchmarked in PR 5's validation alongside the flattening work, where a MySQL integration harness already exists. State it in the spec as "proposed, pending measurement."

**Kept separate from this decision:**

- The `max_overflow = max_workers` coupling (3.5) deserves scrutiny — auto-widening the pool to match thread count is reasonable for warehouses and questionable for a single-primary row store. That is a note for the separate global `max_workers` discussion, not something to change here.
- This is a mitigation, not a root cause. Capping MySQL concurrency likely reduces OOM pressure, but it does not tell us where the memory went. **Still get the customer's OOM traceback** before anyone treats the OOM as closed.

### 4.3 Reviewer constraints addressed

Three constraints were raised in internal review before this work started. All three are resolved; recorded here so they are not re-opened.

1. Target the SQLAlchemy profiler, not the deprecated GE profiler. Resolved — §3.1 confirms `method` now defaults to `"sqlalchemy"`; §6 excludes GE changes. Note that Issues 2/3 live in shared code (§3.3), so switching profilers was never a fix for them.
2. Verify no transaction impact where temp tables are created (e.g. sampling). Resolved — §3.2: MySQL and Postgres override neither `setup_profiling` nor `cleanup`, so they create no temp resources; the five adapters that do are excluded from PR 1 by the per-adapter opt-in. §3.6: sampling does not exist for MySQL at all, so there is no sampling path to create temp resources on.
3. The CTE-construction concern for batched queries. Resolved — confirmed valid; root cause at §3.3 (`sqlalchemy_query_combiner.py:330-346`), with §3.3.1 explaining why it stayed latent.

## 5. Phased design

### PR 1 — Autocommit for profiling connections (fixes Issue 1)

Per-adapter opt-in hook rather than a global behavior change:

```python
# base_adapter.py
def profiling_isolation_level(self) -> Optional[str]:
    return None          # preserve current behavior

# adapters/mysql.py (and postgres.py)
def profiling_isolation_level(self) -> Optional[str]:
    return "AUTOCOMMIT"
```

Resolved ONCE at profiler construction (`sqlalchemy_profiler.py` `__init__`), not per table: `__init__` calls `adapter.profiling_isolation_level()`, applies a config escape hatch (below), and — if the resolved level is non-`None` — eagerly validates it by applying `execution_options(isolation_level=...)` on a throwaway connection (raises `ArgumentError` on an unknown name, verified on SQLAlchemy 1.4). This hoist is the fix for the failure mode the reviewer flagged: the level was previously resolved inside the per-table `try` whose handler catches `sa.exc.SQLAlchemyError`, and `ArgumentError` subclasses `SQLAlchemyError` — so a bad level was swallowed into one warning per table and zero profiles for the run, silently. Resolving+validating at construction fails loudly once instead. The per-table path only re-applies the already-validated `self._profiling_isolation_level` to each checked-out connection (the `execution_options` call must apply per connection; only resolution+validation were hoisted). The hook contract is `Optional[str]` (kept as `str`, not `Literal["AUTOCOMMIT"]`, so a future adapter can return e.g. `READ COMMITTED` without a type change); a misbehaving adapter returning a non-string raises inside `execution_options` at construction — loud and correct.

Config escape hatch (item 3): `GEProfilingConfig.profiling_isolation_level` (base, default `None`, no MySQL override so PR 2's drift guard is unaffected) overrides the adapter in both directions — force a level (e.g. `READ COMMITTED`) on every source, or force transactional behavior via the `TRANSACTIONAL` sentinel (e.g. for MySQL behind a proxy that rejects the `AUTOCOMMIT` session setting). An invalid level fails loudly at construction (above), not per table.

Per-adapter opt-in because five adapters override `setup_profiling` (Athena, ClickHouse, BigQuery, Trino, Snowflake), and four of those also override `cleanup` (BigQuery, Athena, Trino, Snowflake) — i.e. they create resources requiring teardown. Those resources are session-scoped rather than transaction-scoped, so autocommit is probably safe for them, but there is no reason to take that risk in this PR. Extending autocommit beyond MySQL/Postgres gates on individually reviewing those five, not two. (Postgres is included here: `PostgresAdapter` overrides only expression/estimation methods, with no `setup_profiling` or `cleanup` override, so it creates no temp resources.)

**Accepted correctness trade-off.** Today the single transaction gives every profiling query for a table a consistent snapshot. Under autocommit, `min`, `max`, `COUNT(*)`, `COUNT(col)`, `uniqueCount`, quantiles, histograms, and sample values each come from different snapshots, so on a concurrently-written table a profile can be internally inconsistent — e.g. `uniqueCount` > `rowCount` (`uniqueCount` is emitted raw at `sqlalchemy_profiler.py:1341`, NOT clamped), or a histogram bucketed on a stale `min`/`max` containing out-of-range values. This is an explicitly accepted trade-off: the customer stated they tolerate it ("DataHub performs analytical operations and can tolerate a small amount of inconsistency"). The clamps that exist (`null_count = max(0, row_count - non_null_count)` at `:1326`, `nullProportion`/`uniqueProportion` via `min(1, ...)` at `:1333`/`:1347`) prevent nonsensical RATIOS, not inconsistent COUNTS — they only make sense because cross-snapshot skew is already possible, so autocommit widens a window the code is already written to tolerate. They do NOT make `uniqueCount` ≤ `rowCount` or keep histograms within `min`/`max`. State this plainly in the PR description; do not overstate the clamps as "already defends against the skew."

PR 1 must verify the 3.2 hypothesis empirically: with MySQL query logging on, characterize what `isolation_level="AUTOCOMMIT"`, `connect_args={"autocommit": True}`, and the pymysql default each actually do at the `SET AUTOCOMMIT` level. **Verified** — captured output and analysis are in `.superpowers/specs/pr1-empirical-check.md` (reproducer run from the dev session; not committed to the repo). Summary: pymysql's default issues `SET AUTOCOMMIT = 0` on connect, so all profiling SELECTs run inside one open transaction (a real latent long-transaction issue, reproduced); `execution_options(isolation_level="AUTOCOMMIT")` deterministically issues `SET AUTOCOMMIT = 1` and each SELECT is its own transaction (the deterministic fix PR 1 ships); `connect_args={"autocommit": True}` issued no `SET AUTOCOMMIT` at all and relied on the server default — it avoids the long transaction only when the server default is `autocommit=ON` (Aurora's default), which is consistent with autocommit having worked for the customer transactionally and "not working so well" referring to the subsequent OOM, not a transaction. PR 1's claim is therefore "make autocommit deterministic and on by default," not "fix what connect_args couldn't."

### PR 2 — MySQL guardrails + concurrency default (closes 3.4, mitigates 3.5)

Three changes, all inside MySQL-specific files:

1. **New `MySQLProfilingConfig(GEProfilingConfig)`** (in `ingestion/source/sql/mysql.py`, alongside `MySQLConfig`) overriding:
   - `profile_table_row_limit = None`
   - `profile_table_size_limit = None`
   - `max_workers = 5` (proposed, pending measurement — see §4.2)
   - with a comment explaining the per-source-override precedent and the MySQL-specific rationale (row store on a single primary; 5M shared default miscalibrated by ~100×).
   - `MySQLConfig.profiling` is repointed to `MySQLProfilingConfig`.
   - **Implementation detail to verify:** both base fields are `Annotated[Optional[int], SupportedSources([...])]`. Redeclaring in a subclass without re-wrapping in `Annotated[...]` will likely drop the `SupportedSources` metadata, which affects docs generation and any validation keyed off it. PR 2 must confirm whether the subclass override alone is sufficient (in which case the base annotation change below is redundant and should be dropped) or whether the base annotation must be updated instead. Do both only if both are needed; otherwise pick the one that actually carries the metadata.
2. **Override `generate_profile_candidates` in `MySQLSource`**, mirroring Oracle/Teradata: one `information_schema.tables` query per schema pulling `table_rows` and `data_length`, feeding the existing enforcement at `sql_generic_profiler.py:310-328`. Add `mysql` to the `SupportedSources` annotations on both config fields in `ge_profiling_config.py`.
3. **Make the cost visible (warn on observed cost, aggregated).** After the run, emit **one** `self.report.warning(...)` naming the few most expensive tables by actual profiling time, with the config to set (`profile_table_row_limit` / `profile_table_size_limit`). Use the per-table timing at `sqlalchemy_profiler.py:1806-1809` / `self.times_taken` at `:1074`/`:1810`; add a small `(pretty_name, time_taken)` tracker to name the top-N. Keep the existing `profiling_skipped_row_limit` / `profiling_skipped_size_limit` counters. See §4.1 for why a row-count threshold was rejected.

Document clearly: InnoDB `table_rows` is an **estimate** and can be off substantially. Acceptable for a guardrail; must not be presented as an accurate row count. Surface skips via `self.report.warning(...)` per repo convention.

**Non-breaking.** No `docs/how/updating-datahub.md` entry. Unblocks to land right after PR 1.

### PR 3 — Test scaffolding for the combiner (prerequisite, not optional)

Build unit coverage for `utilities/sqlalchemy_query_combiner.py` against **current** behavior before changing it: queue partitioning, result mapping by `col.name`, the `index == len(row)` invariant, the exception path, and the serial fallback path. PR 4 rewrites result extraction and there is currently nothing to catch a regression (3.9).

Follow the repo testing philosophy in `AGENTS.md` — behavior over implementation, no `@Nonnull`-style trivia, no reflection into privates, no exact-error-message assertions. Tests go in `metadata-ingestion/tests/unit/utilities/test_sqlalchemy_query_combiner.py` (mirroring source structure under `src/datahub/utilities/`; `tests/unit/utilities/` already exists).

### PR 4 — Flatten same-shape aggregates, behind a flag (fixes Issues 2 and 3)

In `_execute_queue`, before the CTE construction at `sqlalchemy_query_combiner.py:330`, partition `pending_queue` into groups keyed by a signature over: the FROM clause, the WHERE clause, and the _absence_ of GROUP BY / ORDER BY / LIMIT / DISTINCT. Each group with >1 member becomes a single flat `SELECT <agg>, <agg>, ... FROM t`. Anything unmatched falls through to today's CTE path unchanged. Because every profiling metric shares the same shape with no WHERE (3.3), MySQL profiling collapses to a small number of statements.

Two things this must get right:

1. **Result mapping.** Today's extraction maps by `col.name` (`:375-378`). Flattening requires unique generated labels and strict index-based mapping back to each `_QueryFuture`. Watch for label collisions (multiple metrics produce `count_1`-style names) and preserve the `index == len(row)` invariant. **This is where the regression risk lives** — PR 3 is the guard.
2. **The memory cap from 3.7.** Group memory-heavy aggregates (`COUNT(DISTINCT)`) separately and cap how many land in one statement, so 17 distinct-trees do not coexist. Goal: a few scans instead of ~100, without trading a scan problem for a memory problem.

Gate on a new config flag `query_combiner_flatten_enabled` (suggested name), **default `False`**, added to `GEProfilingConfig` with a description. Omit any `SupportedSources` annotation — follow the precedent of `query_combiner_enabled` (`ge_profiling_config.py:185-188`), which is a cross-platform flag with no annotation. (An empty `SupportedSources([])` would plausibly mean "supported nowhere" and could warn or reject for every source.)

**Open implementation question — how flattening interacts with `MAX_QUERIES_TO_COMBINE_AT_ONCE = 40`.** Today the cap governs queued queries, which equals CTEs, which equals scans. After flattening, 34 queued queries may collapse into one statement, so the cap no longer governs the thing that matters (scans / peak memory). PR 4 must decide whether the knob becomes "aggregates per flat statement" and how it composes with the `COUNT(DISTINCT)` cap from 3.7 — otherwise two caps interact in an undefined way. Resolve before implementing; state the chosen semantics in the PR description.

### PR 5 — Validate (flip the default in a separate follow-up PR)

Validate against the Postgres integration profiler suite plus a new MySQL profiling integration test (closes the gap in 3.9). **The decisive test: run the same table with flattening off and on, and assert identical profile output** — flattening changes SQL text, not results. PR 5 is also where the MySQL `max_workers` value (4.2) is benchmarked using the harness built here. The default flip for `query_combiner_flatten_enabled` lands in a **separate follow-up PR** so it can be reverted independently.

## 6. Non-goals

- **No new sampling implementation for MySQL.** None exists today (3.6); building one is a larger effort. Noted as a gap, not fixed here.
- **No profiler architecture refactor.** Targeted changes only.
- **No changes to the deprecated GE profiler** (3.1). Target the SQLAlchemy profiler.
- **No global `max_workers` default change.** Out of scope; raised separately (§4.2).
- **No `max_overflow` coupling mechanism change.** The coupling mechanism at `sql_common.py:377-380` is unchanged. Its _value_ does change for MySQL as an intended consequence of PR 2's `max_workers` override: the pool shrinks from ~45 connections (size=5 + max_overflow=40) to ~10 (size=5 + max_overflow=5). That is desirable and is part of why the `max_workers` override mitigates the OOM — but it is a second-order effect worth calling out so a reviewer does not trip over it. Whether the coupling itself should exist is a question for the separate global `max_workers` discussion.
- **No OOM root-cause claim.** The OOM is not root-causable from the code alone. The customer's traceback is required to distinguish client-side (ingestion pod) from MySQL-side failure. Candidates in priority order: (1) `max_workers` concurrency (3.5), (2) concurrent `COUNT(DISTINCT)` trees on a 500M-row table (3.7), (3) Aurora temp/sort memory limits. Explicitly ruled out: cardinality misclassification from row-count estimation (3.8). PR 4's memory-aware grouping may address candidate 2 incidentally. **Do not promise a fix for the OOM in this plan.**

### Follow-up: the broad `except Exception` at `sqlalchemy_profiler.py:1840-1851`

Not PR 1's job, but it hampered PR 1's debugging and will hamper PR 4's. The outer `except Exception` at `sqlalchemy_profiler.py:1843-1854` catches anything that escapes the inner `setup_profiling` try and reports it as a generic `"Unexpected error during profiling"` warning with a `None` profile return. During PR 1, a connection-setup `TypeError` (from a mock returning a non-string isolation level) was swallowed here and surfaced as a profiling failure rather than a setup failure — which is exactly the kind of bug-eating that will make PR 4's combiner-flattening failures hard to diagnose (a wrong-shaped result or a label-extraction error would land here as a generic "unexpected error"). Worth a separate follow-up to log the exception type and a more specific context (e.g. "profiling infrastructure error" vs "profiling metric error") so infrastructure bugs don't masquerade as per-table profiling failures. Not in scope for this plan.

### Follow-up: PR 1's autocommit fix doesn't reach MariaDB (CERTIFIED source)

PR 1's `profiling_isolation_level` hook is dispatched through `get_adapter`, which matches on the _exact_ platform string. `mariadb`, `tidb`, and `doris` all resolve to `GenericAdapter`, whose `profiling_isolation_level()` returns `None` — so none of them get PR 1's autocommit fix. For TiDB and Doris that's the safer default anyway (no autocommit until their drivers are individually reviewed, same as Athena/ClickHouse/BigQuery/Trino/Snowflake). But **MariaDB is a CERTIFIED source with the identical InnoDB undo-log / history-list semantics that produce the long-transaction problem in the first place** — and it inherits PR 2's guardrails (via `MySQLConfig`) but not PR 1's autocommit. That's the worst symptom, unaddressed, for a certified source. Closing it means adding `mariadb` to the `mysql` branch of `get_adapter` (PR 1 scope, not PR 2). Tracked here so it isn't lost; do not expand PR 2 to cover it.

### Follow-up: Redshift autocommit (deferred from PR 1, named)

Redshift subclasses `PlatformAdapter` directly (not via a MySQL/Postgres adapter) and overrides neither `setup_profiling` nor `cleanup`, so by PR 1's own criterion it is as safe to put under AUTOCOMMIT as MySQL/Postgres — yet it is NOT opted in here, to keep PR 1's blast radius minimal and to require an individual review. The reason this deserves a named follow-up (not just "deferred"): long transactions blocking VACUUM is a real, well-known Redshift problem, so autocommit is likely the right call and worth scheduling rather than letting it sit in the generic "deferred" bucket. MSSQL, Databricks, and `GenericAdapter` (the fallback for all unlisted platforms) remain deferred without a named follow-up — opting in `GenericAdapter` is the base-default inversion rejected in `base_adapter.profiling_isolation_level`.

### The 6-month caveat

PRs 1, 2, and 4 are all tractable well inside 6 months; 1 and 2 are small. The honest caveat: even fully fixed, profiling a 500M-row MySQL table means full scans. The scan _count_ drops roughly 20–40×, but a single scan of a billion InnoDB rows is still expensive. For the customer's actual goal — Volume and Column Metric assertions — the durable answer is bounded/incremental queries, not cheaper full profiling. Worth stating explicitly as future work beyond this plan.

## 7. Testing strategy

- **PR 1:** Unit test that the adapter hook returns `"AUTOCOMMIT"` for MySQL/postgres and `None` for the default; unit test that `sqlalchemy_profiler` applies `execution_options(isolation_level=...)` when the hook returns non-`None` and does not otherwise. Plus the empirical MySQL query-logging verification described in §5/§3.2 — captured as a manual test note in the PR description (a local MySQL, not CI).
- **PR 2:** Unit test `MySQLProfilingConfig` defaults (row/size `None`, `max_workers=5`). Unit test `MySQLSource.generate_profile_candidates` returns the right identifiers for a mocked `information_schema` result and that `is_dataset_eligible_for_profiling` skips/enables correctly. Unit test that the post-run `report.warning` names the top-N tables by observed time (feed the `(pretty_name, time_taken)` tracker canned values and assert the warning names the top few with the config to set) and that the skip counters still increment when a limit is configured. Integration: extend `tests/integration/mysql/` with a golden-file case for a table that is skipped by a configured row limit.
- **PR 3:** New `tests/unit/utilities/test_sqlalchemy_query_combiner.py` covering current behavior (§5 PR 3).
- **PR 4:** Extend the PR 3 unit tests to assert flat-group partitioning, unique labels, index-based mapping, the `index == len(row)` invariant, and the `COUNT(DISTINCT)` cap. Add a unit test that unmatched shapes fall through to the CTE path unchanged.
- **PR 5:** Postgres integration profiler suite stays green. New MySQL profiling integration test: assert identical profile output with `query_combiner_flatten_enabled` off vs on. Benchmark `max_workers` values here.

All Python verification via `./gradlew :metadata-ingestion:lintFix` and `./gradlew :metadata-ingestion:lint` (never `py_compile` or direct `ruff`/`mypy`). Tests via `./gradlew :metadata-ingestion:testQuick` or `testSingle -PtestFile=...`. Never create or pip-install into venvs manually.

## 8. Rollout / flag plan

- PR 1: lands enabled for MySQL (and postgres) — no flag, per-adapter opt-in is the switch.
- PR 2: lands enabled for MySQL — non-breaking, no flag.
- PR 4: lands behind `query_combiner_flatten_enabled`, default `False`. Off for everyone.
- PR 5: validation PR adds MySQL integration coverage and benchmarks `max_workers`. A _separate_ follow-up PR flips `query_combiner_flatten_enabled` default to `True` so it can be reverted independently.

## 9. Breaking-change implication

PR 2 is non-breaking by design (§4.1). PR 4 is off by default. **PR 1, however, is a query-semantics change** — MySQL/Postgres profiling now runs under AUTOCOMMIT instead of one long-lived transaction, so profile _values_ can differ on concurrently-written tables (`uniqueCount` > `rowCount`, histograms bucketed on stale `min`/`max`, etc. — see §5). That warrants a `docs/how/updating-datahub.md` entry (added), documenting the change and the `profiling_isolation_level: TRANSACTIONAL` escape hatch. The earlier "no entry required" judgement covered PR 2's guardrails, not PR 1's query-semantics change; corrected here.

## 10. Confidentiality

This is a public repository. No customer names, ticket IDs, real host/schema/table/column names, usernames, or account IDs in code, tests, comments, commit messages, or PRs. Use generic placeholders (`my_db.my_schema.events`, `col_a`) that preserve the structural pattern being tested.

## 11. Sequenced implementation plan

Per-PR: files touched, change, tests added, verification, rollback.

### PR 1 — Autocommit for profiling connections

- **Files:** `metadata-ingestion/src/datahub/ingestion/source/sqlalchemy_profiler/base_adapter.py` (add `profiling_isolation_level` hook + full 11-adapter accounting docstring); `metadata-ingestion/src/datahub/ingestion/source/sqlalchemy_profiler/adapters/mysql.py` (override, corrected causal story); `metadata-ingestion/src/datahub/ingestion/source/sqlalchemy_profiler/adapters/postgres.py` (override, corrected causal story with idle-in-transaction/VACUUM); `metadata-ingestion/src/datahub/ingestion/source/sqlalchemy_profiler/sqlalchemy_profiler.py` (resolve+validate ONCE in `__init__`; re-apply per-table); `metadata-ingestion/src/datahub/ingestion/source/ge_profiling_config.py` (add `profiling_isolation_level` escape hatch on the BASE config, default `None`, `TRANSACTIONAL` sentinel — no MySQL override, so PR 2's drift guard is unaffected); `docs/how/updating-datahub.md` (entry — PR 1 is a query-semantics change, see §9).
- **Change:** Add the hook; resolve+validate the level once at construction (Blocking 1 — a bad level previously fell into the per-table `SQLAlchemyError` handler and silently zeroed the run); add the config escape hatch (item 3) overriding the adapter in both directions; re-apply the validated level per-table. Opt-in is per-adapter by exact platform match in `get_adapter` — do NOT invert the base default (`GenericAdapter` is the fallback for every unlisted platform). Adapter comments deduplicated (2–3 platform-specific lines each; shared reasoning in the base docstring). Clamp claim corrected: clamps prevent nonsensical ratios, not inconsistent counts (`uniqueCount` is emitted raw).
- **Tests:** Hook unit tests; profiler tests that the configured connection flows downstream (item 10), a real-dialect round-trip via sqlite `get_isolation_level` (item 11), invalid-level fails loudly at construction (item 12), shared `mock_adapter` fixture (item 13); `_make_profiler` inline flag (item 9, leaving PR 2's separate helper intact). Pre-existing tests that mock `get_adapter` set `profiling_isolation_level.return_value = None`.
- **Verification:** `./gradlew :metadata-ingestion:lintFix`, `./gradlew :metadata-ingestion:testQuick`, plus manual MySQL query-logging check (PR description).
- **Rollback:** Revert the PR; the hook returning `None` preserves prior behavior. (The `profiling_isolation_level: TRANSACTIONAL` escape hatch also lets operators restore per-table transactional behavior without reverting.)

### PR 2 — MySQL guardrails + concurrency default

- **Files:** `metadata-ingestion/src/datahub/ingestion/source/sql/mysql.py` (new `MySQLProfilingConfig`, repoint `MySQLConfig.profiling`, override `MySQLSource.generate_profile_candidates`, add the post-run top-N-by-time `report.warning` and the `(pretty_name, time_taken)` tracker); `metadata-ingestion/src/datahub/ingestion/source/ge_profiling_config.py` (add `mysql` to `SupportedSources` on both fields — see the annotation-drop caveat in §5 PR 2 item 1).
- **Change:** Per §5 PR 2.
- **Tests:** Unit tests per §7 PR 2; one new MySQL integration golden case.
- **Verification:** `./gradlew :metadata-ingestion:lintFix`, `./gradlew :metadata-ingestion:testQuick`, MySQL integration suite.
- **Rollback:** Revert the PR; MySQL reverts to no guardrail (today's behavior).

### PR 3 — Combiner test scaffolding

- **Files:** `metadata-ingestion/tests/unit/utilities/test_sqlalchemy_query_combiner.py` (new; mirrors `src/datahub/utilities/sqlalchemy_query_combiner.py`).
- **Change:** Behavior-preserving unit tests for current combiner.
- **Tests:** The file itself.
- **Verification:** `./gradlew :metadata-ingestion:lintFix`, `./gradlew :metadata-ingestion:testSingle -PtestFile=tests/unit/utilities/test_sqlalchemy_query_combiner.py`.
- **Rollback:** Delete the test file.

### PR 4 — Flatten same-shape aggregates (flagged off)

- **Files:** `metadata-ingestion/src/datahub/utilities/sqlalchemy_query_combiner.py` (partition + flat path + memory cap); `metadata-ingestion/src/datahub/ingestion/source/ge_profiling_config.py` (new `query_combiner_flatten_enabled` flag, default `False`); `metadata-ingestion/src/datahub/ingestion/source/sqlalchemy_profiler/query_combiner_runner.py` (wire flag through).
- **Change:** Per §5 PR 4.
- **Tests:** Extend PR 3 tests; add partition/label/cap/fallback tests.
- **Verification:** `./gradlew :metadata-ingestion:lintFix`, `./gradlew :metadata-ingestion:testQuick`.
- **Rollback:** Set flag to `False` (default) — no behavior change.

**Priority note — the OOM traceback gates PR 4.** The OOM is the symptom that actually blocked the customer after they applied the advice (§3.2 framing correction). PRs 2 and 4 are speculative mitigations against the leading OOM candidates (§3.5, §3.7), but we still have no traceback and cannot distinguish client-side (ingestion pod) from MySQL-side failure. **Getting the customer's OOM traceback is higher priority than shipping PR 4.** Do not treat PR 4 as closing the OOM; treat it as a best-effort mitigation that the traceback may invalidate. If the traceback arrives and points somewhere unexpected, PR 4's design (memory-aware grouping) may need to change before it ships.

### PR 5 — Validate (flip default in a separate follow-up PR)

- **Files:** `metadata-ingestion/tests/integration/sqlalchemy_profiler/` (new MySQL case); `metadata-ingestion/tests/integration/mysql/` (golden files as needed). A _separate_ follow-up PR flips the flag default in `ge_profiling_config.py`.
- **Change:** Per §5 PR 5.
- **Tests:** Integration parity test; `max_workers` benchmark notes.
- **Verification:** Postgres + MySQL integration suites; `./gradlew :metadata-ingestion:lintFix`.
- **Rollback:** Revert the default-flip follow-up PR independently.
