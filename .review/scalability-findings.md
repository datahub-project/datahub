# Scalability Review — snowflake_openflow

**Connector:** `metadata-ingestion/src/datahub/ingestion/source/snowflake/` (scope: `snowflake_openflow.py` +
`snowflake_openflow_{config,models,query,report}.py` + `tests/unit/snowflake_openflow/` +
`tests/integration/snowflake_openflow/` only)

**Verdict:** No Critical, High, or Medium findings. 2 Low findings, both carried forward from the
previous round and already accepted/documented architectural trade-offs or platform limitations.

## Resolved since last round

### scal-005 — nested retry amplification (was Medium) — RESOLVED

Retry now lives in exactly one layer per call site:

- `_query_rows_with_retry` (line 949) wraps the bare `_query_rows` for the SHOW inventory fetch
  (line 1477) and each paged-history page (line 1373).
- `_read_connector_url` wraps its own DESCRIBE call in `_retrying()` directly (line 1054).
- `_read_connector_config` wraps `_download_connector_config` in `_retrying()` (line 1118), which
  internally calls the bare `_query_rows` (line 1159) — no further retry inside it.

A grep across the file for `_retrying()|_query_rows_with_retry|_query_rows(` confirms no call site
wraps an already-retrying method a second time. `test_a_persistent_failure_is_not_retried_twice_over`
asserts the total attempt count equals `_RETRY_MAX_ATTEMPTS`, not its square, and passes. This also
restores `_download_connector_config`'s per-attempt `TemporaryDirectory` guard, since retrying is no
longer re-entering the download from a layer below the directory scope.

### scal-001 — unbounded SHOW / no truncation detection (was Medium) — RESOLVED

`_warn_if_show_truncated` (line 904) compares `len(rows)` to `_SHOW_ROW_CAP` (10,000) after every
SHOW call inside `_fetch_inventory` (line 1478), incrementing `report.num_show_results_at_row_cap`
and warning by object type on equality. Both boundary tests
(`test_an_inventory_at_the_row_cap_is_reported_as_possibly_truncated`,
`test_an_inventory_below_the_row_cap_is_not_flagged`) pass. This is *detection*, not a pagination
fix — see scal-003 below for the residual gap that detection alone cannot close.

## Open findings (carried forward)

### scal-003 (Low) — SHOW endpoints have no cursor pagination path

- **File:** `snowflake_openflow.py:145`
- **Rule:** `unbounded-list-endpoint-no-cursor-pagination`
- **Standard:** `standards/performance.md#api-pagination`, `standards/api.md`

`SHOW OPENFLOW DEPLOYMENTS/RUNTIMES/CONNECTORS` has no cursor, `LIMIT`, or `OFFSET` parameter on
the Snowflake side, unlike the `ACCOUNT_USAGE` history queries (which correctly cursor on
`CREATED_ON`, see below). There is no code-side fix available for this: the scal-001 fix converts a
silent truncation into a detected, counted, warned one, which is the correct mitigation given the
platform constraint, but it cannot page past 10,000 rows because the command itself offers no way
to.

**Projected impact:** At the documented tenant-size assumption (tens of deployments/runtimes, low
hundreds of connectors), this sits three orders of magnitude below the cap, and any approach to it
is now detected rather than silent. Left open only because the underlying gap is a platform
limitation, not something this connector's code can close.

### scal-004 (Low) — per-connector network I/O is N+1, accepted with a documented cap

- **File:** `snowflake_openflow.py:1668` (`get_workunits_internal`'s connector loop, calling
  `_read_connector_url` and `_lineage_for_connector` once per connector)
- **Rule:** `n-plus-one-per-connector-network-io`
- **Standard:** `standards/sql.md#n1` (Row 10, API form), `standards/performance.md#the-n1-query-problem`

This matches the rubric's explicit "accept N+1 as an architectural decision documented in
`_PLANNING.md` with an explicit tenant-size cap" resolution path. `_PLANNING.md`'s
`performance.concurrency` section:

- States the per-connector stage GET (for lineage config) is unconditional and `O(connectors)`.
- Documents *why* it can't be batched: `SELECT $1` truncates `config.json` at the first comma under
  Snowflake's default CSV file format; an inline `FILE_FORMAT` argument is rejected as non-constant;
  a named file format is DDL the read-only metadata role cannot create.
- Declines a bounded worker pool for v1 (ALPHA) with a stated rationale.
- Names the exact crossover point (~10x the documented tenant ceiling, ~30 minutes) at which the
  declined `max_workers` decision should be revisited.

The per-runtime DESCRIBE half (for the external canvas URL) already self-limits at
`_MAX_RUNTIMES_FOR_URL_LOOKUP` (500 distinct runtimes) via `_decide_url_lookup`, and is cached per
runtime so it contributes `O(runtimes)` rather than `O(connectors)`.

**What changed since last round:** this cap and its rationale are now also surfaced to operators in
`metadata-ingestion/docs/sources/openflow/snowflake-openflow_post.md` ("scales with replicated
tables, not connectors" and "above 500 distinct runtimes the lookup is skipped automatically").
That doc update was the fix item open last round; the underlying N+1 shape itself remains, by
design, as documented.

**Projected impact:** At the documented ceiling (low hundreds of connectors), the per-connector
phase is roughly 1-5 minutes, dominated by the stage GET. It crosses the 30-minute signpost in
`standards/performance.md` at roughly 10x that ceiling. A bounded worker pool over
`_read_connector_config` and `_read_connector_url` (both already isolated per-connector methods for
exactly this reason) would cut wall time roughly linearly with `max_workers` if/when that threshold
is approached.

## Rubric walkthrough (fresh, full 10-row pass)

| # | Check | Result | Notes |
|---|---|---|---|
| 1 | Cursor pagination on unbounded list endpoints | Partial (Low: scal-003) | `ACCOUNT_USAGE` history queries cursor correctly on `CREATED_ON` with inclusive `>=`, stall guard, and NULL-cursor guard (`_paged_history`). `SHOW OPENFLOW ...` has no cursor primitive at all — platform limitation, not a defect; mitigated by scal-001's truncation detection. |
| 2 | Workunits yielded incrementally | Pass | `get_workunits_internal` is a generator yielding per-object. The bounded accumulators inside `_fetch_inventory`/`_paged_history` are explicitly documented and justified in `_PLANNING.md`'s `performance.memory_profile`, not a surprise. |
| 3 | SQL query log LIMIT + cursor | N/A | `USAGE_STATS` is declined; `OPENFLOW_USAGE_HISTORY` (credits only) is never queried. |
| 4 | `SqlParsingAggregator` for lineage | N/A | `lineage_strategy.approach = custom-upstream`; no SQL is parsed. Edges are built directly from connector configuration key-value pairs. Explicitly justified in `_PLANNING.md`. |
| 5 | Memory profile matches planning | Pass | The three bounded accumulators described in `_PLANNING.md` (`performance.memory_profile`) are exactly what's in the code; no undocumented full-source accumulator found. |
| 6 | Bounded concurrency | N/A | No `ThreadPoolExecutor`/`asyncio` usage anywhere in scope; sequential by design, documented with a stated future trigger (scal-004). |
| 7 | Connection/session pooling | Pass | Single `SnowflakeConnection` created once in `__init__`, reused for every query/GET, closed once in `close()`. No per-call connection/engine creation. |
| 8 | Retry with exponential backoff | Pass (fixed this round) | See scal-005 resolution. Narrow, enumerated set of transient exceptions; single retry layer per call site. SDK-client retry/endpoint-override sub-checks N/A (no shared vendor client/session factory in this connector). |
| 9 | Per-entity-type report counts | Pass | `SnowflakeOpenflowReport` tracks counts per object type plus granular skip/drop/failure counters. |
| 10 | No N+1 fetch loops | Partial (Low: scal-004) | No SQL-inspector N+1. API-form N+1 (per-connector config GET, per-runtime URL DESCRIBE) is an accepted, capped, documented architectural decision. |

**Source-type classification:** `sql_warehouse` (direct `SnowflakeConnection` + hand-written
SQL/SHOW/DESCRIBE/GET, not a SQLAlchemy inspector-based extractor, but closest fit of the three
buckets). Rows 2, 5, and 9 were evaluated as first-class regardless of classification.

## Test/verification basis

- `venv/bin/python -m pytest tests/unit/snowflake_openflow/ -q` → 226 passed (matches the reported
  232 when integration tests are included).
- Retry-layering verified by direct grep of every `_retrying()` / `_query_rows_with_retry` /
  `_query_rows(` call site — exactly one retry layer per network call.
- Truncation-detection boundary verified via the two dedicated tests at the `_SHOW_ROW_CAP`
  boundary.
