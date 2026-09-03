### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features.

#### Assertion modeling

Every Monte Carlo monitor and custom SQL rule is modeled as a `CUSTOM` assertion, and its native
`comparisons` data is mapped onto DataHub's structured `CustomAssertionInfo` fields so Monte Carlo
assertions render through the same shared description component as dbt and Great Expectations.

For each monitor/rule, the first `comparisons` entry drives the structured fields:

- **`scope`** — `DATASET_COLUMN` when the comparison carries a column reference (`field`/`fields`),
  else `DATASET_ROWS` (table-level and row-predicate checks). This mirrors dbt's column-vs-row
  heuristic.
- **`operator`** — the MC comparison operator mapped to `AssertionStdOperator` (`EQ`→`EQUAL_TO`,
  `GT`/`GTE`/`LT`/`LTE`/`NEQ`, `INSIDE_RANGE`→`BETWEEN`, `IS_NULL`→`NULL`, `IS_NOT_NULL`→`NOT_NULL`).
  Operators with no clean DataHub equivalent (`AUTO*`, `NOOP`, `OUTSIDE_RANGE`) fall back to
  `_NATIVE_`.
- **`aggregation`** — the MC metric mapped to `AssertionStdAggregation` (`row_count`→`ROW_COUNT`,
  `distinct_count`→`UNIQUE_COUNT`, `null_count`→`NULL_COUNT`, `null_rate`→`NULL_PROPORTION`,
  `min`/`max`/`mean`/`median`/`stddev`/`sum`). Unmapped metrics fall back to `_NATIVE_`.
- **`fields`** — schema-field URNs built from the comparison's `field`/`fields`.
- **`parameters`** — thresholds mapped to `AssertionStdParameters` (`BETWEEN`→`minValue`/`maxValue`;
  scalar comparisons → `value`).

The Monte Carlo native type is preserved in `nativeType`, and native fields (severity, data-quality
dimension, resource id, comparison type, metric) are carried in `nativeParameters`. `customProperties`
keeps only the DataHub-internal `mc_monitor_uuid` correlation key. For custom SQL rules, the raw
SQL expression is captured in `customAssertion.logic`.

Monte Carlo rules can carry several independent comparisons, but DataHub's assertion model is
single-comparison. The connector maps `comparisons[0]` onto the structured fields above and folds
any remaining comparisons into `customAssertion.logic` as JSON, so a compound rule is still fully
represented. This preserves the one-monitor → one-assertion-URN scheme, so alert and run-event
wiring is unchanged.

Monitors for which no `comparisons` are returned (or the comparisons are malformed) fall back to
`scope = DATASET_ROWS` with `_NATIVE_` operator/aggregation, so they still render through the
shared path and carry their native fields on `nativeType`/`nativeParameters`.

#### Alert and incident ingestion

Monte Carlo alerts and incidents are ingested as `AssertionRunEvent` failures on their
corresponding assertion. Each event carries a timestamp, the Monte Carlo alert ID, and the
alert's native severity/priority/sub-type on `nativeResults`.

When `emit_incidents_on_failure` is enabled (default), the connector also creates a DataHub
`Incident` entity (`urn:li:incident:…`) for each alert/incident. The incident links back to the
assertion via `IncidentSource(type=ASSERTION_FAILURE, sourceUrn=<assertion>)`, so the failure
appears on the **Incidents** tab of the monitored dataset in addition to the Assertions tab. The
incident URN is derived deterministically from a hash of `(assertion_urn, alert_uuid)`, so
re-ingesting the same alert updates the existing incident rather than creating a duplicate. Set
`emit_incidents_on_failure: false` to suppress incident entities and keep only the
`AssertionRunEvent` failures.

#### Run history and measured metric values

By default the connector emits only the alert-driven `FAILURE` run events above. To also ingest
Monte Carlo's monitor **run history** (`getJobExecutions`) together with the **measured metric
values** (`getMetricsV4`) — i.e. the actual numbers Monte Carlo computed for each monitor run —
set `run_events_lookback_days` to a positive integer N.

When enabled, for each ingested monitor the connector:

- Fetches the most recent runs (capped by `run_events_first`, default `5`) within the last N days.
- Emits every `SUCCESS` run as an `AssertionRunEvent` with `status = COMPLETE` and
  `result.type = SUCCESS`. The **latest** SUCCESS run carries the measured metric value on
  `AssertionResult` — standard metrics land on the typed slots (`rowCount`, `missingCount`,
  `unexpectedCount`, `actualAggValue`) and the rest fall back to `nativeResults`. Older SUCCESS
  runs carry only per-run execution metadata (`totalResultCount`, `evaluatedRecordCount`,
  `exceptions`).
- Leaves `FAILURE` runs to the alert-driven path above (no duplicate events).

`run_events_lookback_days` bounds the **query window**, not the run count — `run_events_first`
caps the count. Enabling this adds roughly one `getJobExecutions` call plus one `getMetricsV4` call
per metric per ingested monitor; set `rate_limit_daily` to bound the extra API spend. Leave
`run_events_lookback_days` unset (`None`) to keep the historical FAILURE-only behaviour.

Run events are emitted with `is_primary_source = False`, so stale entity removal never touches
them — the assertion entity itself is still subject to soft-deletion (via the monitor-definition
path) if the monitor disappears from Monte Carlo, but its run history is preserved.

### Limitations

- **Run history is opt-in:** Without `run_events_lookback_days`, the connector emits only
  `FAILURE` run events (from alerts/incidents); periodic `SUCCESS` events and measured metric
  values are not synthesized. Set `run_events_lookback_days` to ingest them.
- **Best-effort metric correlation:** `getMetricsV4` does not populate `jobExecutionUuid` for
  table-level metrics, so a per-run join is not possible. The measured value is attached to the
  latest SUCCESS run as a best-effort temporal correlation ("most recent measurement" on "most
  recent successful run"), not a proven same-run match.
- **MCON resolution:** Each monitored asset requires one `getTable` call to resolve its MCON to a
  warehouse table (results are cached per MCON). Assets whose warehouse is not in
  `connection_to_platform_map` are skipped with a warning unless `auto_map_connection_types` is
  enabled, in which case the platform is inferred from the warehouse connection type (with
  `default_platform` as the fallback for unrecognized types).
- **Assertion typing:** All monitors and rules are modeled as `CUSTOM` assertions. Their native
  `comparisons` data is mapped onto DataHub's structured `CustomAssertionInfo` fields
  (`scope`/`operator`/`aggregation`/`fields`/`parameters`) and the native type/parameters are
  carried on `nativeType`/`nativeParameters`, but the assertions are not coerced into DataHub's
  typed freshness/volume/SQL/field assertion schemas.
- **Monte Carlo Cloud only:** Requires a Monte Carlo Cloud account and API key pair. Self-hosted
  deployments are not supported.

### Troubleshooting

#### Monitored assets are skipped with a warning

If you see warnings like `Could not resolve MCON to a DataHub dataset URN`, the warehouse for that
asset is not in `connection_to_platform_map`. Add a mapping entry for the warehouse resource UUID
shown in the warning, or enable `auto_map_connection_types` to infer the platform from the
warehouse connection type (falling back to `default_platform` for unrecognized types).

#### Assertion URNs do not match your warehouse source

Assertion URNs are keyed from the Monte Carlo monitor ID, but they target the dataset URN resolved
via `connection_to_platform_map` (or auto-mapped from the warehouse connection type, falling back to
`default_platform`). If the `platform`, `platform_instance`, or `env` values differ from those used by
your warehouse source connector, the assertions will not appear on the correct dataset. Align the
values in `connection_to_platform_map` with the config of your warehouse source.

A few specifics that commonly cause silent mis-attachment:

- **`platform_instance` is Monte Carlo's, not the warehouse's.** The top-level `platform_instance`
  field is Monte Carlo's own instance (it stamps the `dataPlatformInstance` aspect on the assertion
  entity). It is **not** applied to warehouse dataset URNs. For warehouses listed in
  `connection_to_platform_map`, set the instance per entry. For auto-mapped warehouses (those not in
  the map), use `target_platform_instance` instead — leaving it unset means no platform instance on
  those URNs, which is safer than guessing.
- **`env` for auto-mapped warehouses.** `target_env` controls the env on auto-mapped warehouse URNs
  independently of Monte Carlo's own `env`; when unset it falls back to the top-level `env` (the values
  usually coincide). Set it explicitly if your warehouse source uses a different env.
- **Identifier casing.** Snowflake and Redshift dataset URNs are lowercased by default to match those
  warehouses' sources. If your warehouse source preserves case (e.g. Snowflake with
  `convert_urns_to_lowercase: false`), set `convert_urns_to_lowercase: false` on the matching
  `connection_to_platform_map` entry so the assertion targets the same-cased dataset. The top-level
  `convert_urns_to_lowercase: true` still forces lowercase everywhere.
- **Malformed table ids.** A Monte Carlo `full_table_id` that does not resolve to
  `database.schema.table` (three dot-separated segments) is skipped with a warning rather than
  producing a URN for a dataset that does not exist.

#### No assertions appear after ingestion

Verify that:

1. The API key has read access to monitors, custom rules, and alerts in the Monte Carlo UI.
2. At least one monitor is active and has fired an alert (the connector ingests only monitors that
   have associated assets and alerts).
3. The `connection_to_platform_map` covers the warehouse connections used by your monitored assets.

#### Stateful ingestion and soft-deletion

Stateful ingestion (the `stateful_ingestion` config block) is **opt-in** — the starter recipes
ship it commented out. When enabled, assertions that no longer exist in Monte Carlo are
soft-deleted from DataHub at the end of a run.

Two safety guards prevent soft-deletion from a bad run:

- **Zero-assertion guard.** If the run attempted to build assertions but emitted none
  (every monitor failed to resolve), the run records a failure and stale removal is skipped.
- **Partial-failure guard.** If any monitor or custom rule failed to build due to a transient
  error (network blip, API error, unexpected exception during `getTable`), the run records a
  failure and stale removal is skipped — even if most monitors built successfully. This
  covers the band the zero-assertion guard misses (e.g. 40 of 100 monitors hit a transient
  `getTable` error). Permanent failures (the table is genuinely gone, or the platform is
  unmapped) do **not** trip this guard — those are legitimate deletions.

Transient `getTable` failures are also **not cached**, so a later monitor sharing the same
MCON retries instead of inheriting a stale `None`. Leave `stateful_ingestion` disabled until
you have confirmed a run is healthy end-to-end; the guards make it safe to enable afterwards.
