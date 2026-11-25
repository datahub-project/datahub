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
represented. Monitors with an explicit entity list and custom SQL rules keep one assertion URN per
monitor. TABLE monitors resolved through asset selection fan out to one assertion per covered table
(see [Limitations](#limitations)).

Monitors for which no `comparisons` are returned (or the comparisons are malformed) fall back to
`scope = DATASET_ROWS` with `_NATIVE_` operator/aggregation, so they still render through the
shared path and carry their native fields on `nativeType`/`nativeParameters`.

#### Alert and incident ingestion

Monte Carlo alerts and incidents are ingested as `AssertionRunEvent` failures on their
corresponding assertion. Each event carries a timestamp, the Monte Carlo alert ID, and the
alert's native severity/priority/sub-type on `nativeResults`.

When `emit_incidents_on_failure` is enabled (disabled by default), the connector also creates a DataHub
`Incident` entity (`urn:li:incident:…`) for each alert/incident. The incident links back to the
assertion via `IncidentSource(type=ASSERTION_FAILURE, sourceUrn=<assertion>)`, so the failure
appears on the **Incidents** tab of the monitored dataset in addition to the Assertions tab. The
incident URN is derived deterministically from a hash of `(assertion_urn, alert_uuid)`, so
re-ingesting the same alert updates the existing incident rather than creating a duplicate. Set
`emit_incidents_on_failure: true` to enable incident entities. It is off by default because Monte
Carlo alerts resolve over time but the connector has no signal to emit an
`IncidentState.RESOLVED` transition (see [Limitations](#limitations)), so enabling it can
accumulate stale `ACTIVE` incidents.

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

#### What you'll see when Monte Carlo changes its schema

Before each run the connector introspects Monte Carlo's live GraphQL schema and compares it to
the fields it expects. Each checked type gets a **PROCEED**, **DEGRADED**, or **ABORT** verdict.
Verdicts apply **per ingestion phase**, not to the whole run:

- **Monitors** (including warehouse table resolution for dataset URNs)
- **Custom rules**
- **Alerts** (and schema-change metadata used to enrich alert run events, when alerts are enabled)
- **TABLE monitor scope** (`evaluateAssetSelection`), checked the first time a pattern-scoped TABLE
  monitor is resolved in the run
- **Run history** (`getJobExecutions` / `getMetricsV4`), only when `run_events_lookback_days` is set

| Verdict      | What happens                                                                                                                                                                                                                                                                                                           |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **PROCEED**  | The live schema exposes every field the connector requests for that type. Ingestion uses the dynamic query shape.                                                                                                                                                                                                      |
| **DEGRADED** | One or more **non-critical** fields are missing. The connector drops them, continues the phase, and emits a **warning** titled `Monte Carlo schema drift (<type>)`. The message lists each missing field and, when known, what you lose (for example an empty native slot on the assertion).                           |
| **ABORT**    | A **critical** field is missing, the GraphQL type itself is gone, or `strict_schema_drift` is enabled and (any requested field is missing or introspection failed). The connector **skips that phase** and records a **failure** so stateful stale removal does not treat the run as complete. Other phases still run. |

**Failure titles you may see on ABORT:**

- `Monte Carlo schema drift: critical fields missing (...)` — a field required to build valid records
  for that phase is absent (listed in the title).
- `Monte Carlo schema drift: strict mode: drift detected` — `strict_schema_drift: true` and at least
  one non-critical field is missing for that type.
- `Monte Carlo schema drift: type <Name> not found in the Monte Carlo schema` — introspection returned
  no such type (Monte Carlo removed or renamed it).
- `Monte Carlo schema drift: could not verify schema` — introspection failed (network, auth, or API
  error); the connector does not fall back silently when strict mode is on.

For alerts, a phase ABORT means **no alerts are ingested for that run** (one failure explains schema
drift rather than one warning per alert row).

**`strict_schema_drift`:** Leave it `false` (default) to ingest with warnings when Monte Carlo drops
optional fields. Set it `true` if any missing field — critical or not — should stop the affected phase
instead of degrading.

**Stale-removal lock:** When stateful ingestion is enabled, assertions that disappear from a run can
be soft-deleted at the end. The connector skips that cleanup when the run recorded **failures** that
mean the ingest was incomplete, including:

- A schema-drift **ABORT** on monitors, custom rules, or alerts (phase skipped).
- **Partial build failures** — transient errors while resolving tables or building assertions (`Partial
build failures; skipping stale entity removal`).

That lock protects assertions that still exist in Monte Carlo but were missing from a bad or partial
run. Permanent skips (for example a table that is genuinely gone) do not trip the lock.

**What you should do:** If you see schema-drift warnings or failures after Monte Carlo upgrades their
API, upgrade to a DataHub release that includes an updated Monte Carlo connector, then re-run ingestion.
If you are already on the latest connector, report the drift message (type name and missing fields) so
the connector can be updated for the new schema.

### Limitations

- **Run history is opt-in:** Without `run_events_lookback_days`, the connector emits only
  `FAILURE` run events (from alerts/incidents); periodic `SUCCESS` events and measured metric
  values are not synthesized. Set `run_events_lookback_days` to ingest them.
- **Best-effort metric correlation:** `getMetricsV4` does not populate `jobExecutionUuid` for
  table-level metrics, so a per-run join is not possible. The measured value is attached to the
  latest SUCCESS run as a best-effort temporal correlation ("most recent measurement" on "most
  recent successful run"), not a proven same-run match.
- **TABLE monitor scope:** Pattern-scoped TABLE monitors (tags, activity, table-name filters) are
  resolved via Monte Carlo's `evaluateAssetSelection` API. Each covered table becomes its own
  assertion. Coverage is capped by `table_monitor_max_assets` (default 100) after sorting the
  resolved tables, so the same tables are kept each run; remaining tables are skipped with a
  warning.
- **Catalog schema history is not ingested as assertions:** Monte Carlo's Schema changes panel
  (`getSchemaChanges`) is per-table catalog history, not a monitor. Unmonitored tables with
  schema-change history do not get an assertion in DataHub. Schema-change _alerts_ raised by TABLE
  monitors are ingested as assertion failures on the covered tables. For those alerts the connector
  joins `getSchemaChanges` by table MCON and timestamp (closest event within 2 hours, from the
  first 50 catalog events per table in the alert window) so `fieldsAdded` / `fieldsRemoved` /
  `fieldTypeChanges` land on the run event's `nativeResults`. Alerts with no matching event are
  ingested without the field-level diff and a warning names the cause.
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
- **Monitor SQL body:** On the current Monte Carlo gateway the `Monitor` type has no SQL body field
  the connector reads, so `customAssertion.logic` stays empty for monitors. Row filters appear on
  `nativeParameters` as `whereCondition`. Custom SQL rules still populate `logic` from `customSql`.
- **Monte Carlo Cloud only:** Requires a Monte Carlo Cloud account and API key pair. Self-hosted
  deployments are not supported.
- **Incidents stay ACTIVE:** When `emit_incidents_on_failure` is enabled, each Monte Carlo
  alert creates a DataHub `Incident` in the `ACTIVE` state. The connector fetches alerts only
  within `alerts_lookback_days`, and resolved alerts typically no longer appear in Monte Carlo's
  alert feed, so there is no signal to emit an `IncidentState.RESOLVED` transition. As a result,
  DataHub incidents can accumulate in the `ACTIVE` state after the underlying Monte Carlo alert has
  resolved. This is why `emit_incidents_on_failure` is disabled by default; leave it off if your
  workflow does not tolerate stale active incidents, and only enable it if you manage incident
  resolution in DataHub separately.

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
- **Identifier casing.** Dataset URN casing is controlled at recipe level, not
  hardcoded per platform. The per-warehouse `convert_urns_to_lowercase` override on
  the matching `connection_to_platform_map` entry takes precedence when set — set
  it to `false` for a case-preserving Snowflake/Redshift deployment whose warehouse
  source runs with `convert_urns_to_lowercase: false`, so the assertion targets the
  same-cased dataset. Otherwise the top-level `convert_urns_to_lowercase` flag applies:
  `true` forces lowercase everywhere; leaving it unset preserves case. Set the
  top-level flag to `true` if your warehouse source lowercases (e.g. Snowflake with
  `convert_urns_to_lowercase: true`).
- **Malformed table ids.** A Monte Carlo `full_table_id` that does not resolve to
  `database.schema.table` (three dot-separated segments) is skipped with a warning rather than
  producing a URN for a dataset that does not exist.

#### No assertions appear after ingestion

Verify that:

1. The API key has read access to monitors, custom rules, and alerts in the Monte Carlo UI.
2. At least one monitor is active and its scope resolves to warehouse table(s) (the connector
   ingests assertions only for monitors whose assets resolve to dataset URNs; alerts are optional
   and controlled separately by `include_alerts`).
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
