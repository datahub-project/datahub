### Capabilities

#### Supported model kinds

All SQLMesh model kinds are supported: `FULL`, `INCREMENTAL_BY_TIME_RANGE`,
`INCREMENTAL_BY_UNIQUE_KEY`, `INCREMENTAL_BY_PARTITION`, `SCD_TYPE_2`, `VIEW`, `SEED`,
`EXTERNAL`, and `EMBEDDED`. Each maps to a DataHub dataset subtype (`Model`, `Seed`,
`Source`, or `Embedded`).

#### Data quality assertions

Three families of DataHub Assertions are emitted per model, all attached to
the SQLMesh logical URN (sibling stitching surfaces them on the warehouse
sibling in the UI automatically):

1. **Audit-derived `DATASET` assertions** — each `not_null`,
   `unique_values`, `unique_combination_of_columns`, `number_of_rows`,
   `forall`, `accepted_range`, and `accepted_values` audit in the model's
   SQL becomes a typed assertion. Unrecognised audit names fall back to a
   `_NATIVE_` dataset-rows assertion so they're still discoverable.

2. **`FRESHNESS` assertions** — two per non-external, non-embedded model
   (`pipeline_freshness` and `upstream_freshness`). Both use a
   `FIXED_INTERVAL` schedule derived from the model's `interval_unit`:
   3× the cron cadence with a 1-hour floor (mapping in
   `_INTERVAL_UNIT_TO_SLA`). The two-assertion split is diagnostic: when
   freshness drifts, `customProperties.sqlmesh.freshness_kind` tells you
   whether the pipeline stopped rebuilding or upstream data is stale.

3. **`VOLUME` assertions** — one per model, asserting `row_count >= 1`.
   Catches the catastrophic empty-table-after-rebuild failure mode
   universally. Tighter expected-value thresholds aren't predictable from
   the model definition; anomaly detection (Cloud feature) wraps the
   static threshold with an ML baseline derived from accumulated run-event
   history.

External and embedded models skip the freshness and volume families
(they have no rebuild schedule and no own materialised output).

Today only the assertion **definitions** are emitted; evaluation is
performed by DataHub's monitor framework (auto-run on Cloud) or by
re-ingestion-time run events (planned). The `audit_results_path` config
still provides a manual pass/fail path for audit assertions.

#### Smart anomaly detection

When `emit_smart_assertion_anomaly_detection: true` (default), every
emitted assertion carries `customProperties["sqlmesh.anomaly_detection"]
= "requested"`. DataHub stores this like any other custom property —
it shows in the assertion's properties panel and is queryable through
the GraphQL API. Cloud's monitor framework additionally reads the
marker to wrap the assertion's static threshold in an ML detector, so
a drop below the historical baseline fires even when the static rule
passes. Set the flag to `false` only if you don't want the marker
visible on assertions in the UI.

#### Stateful ingestion

When `stateful_ingestion.enabled: true`, the connector tracks emitted URNs across runs
and soft-deletes entities that have been removed from the SQLMesh project.

### Limitations

- **Python models**: Python-based SQLMesh models (ibis, pandas) are supported for
  schema extraction when `columns_to_types` is defined, but column-level lineage is
  not available.
- **Audit run results**: Pass/fail status on the Validation tab requires an external
  JSON results file (`audit_results_path`). The connector does not execute audits
  itself at ingestion time.
- **Assertion run events**: Today only assertion definitions are emitted.
  Per-ingest run events with actual signal values (row counts via
  `ctx.engine_adapter`, fingerprint `updated_ts` from the state store)
  are planned so DataHub instances without Cloud monitors get a usable
  Validation tab.
- **Metadata Tests**: `emit_metadata_tests` is reserved on the config but
  the Test JSON DSL is undocumented in DataHub, so the flag is currently
  a no-op. Will wire up once the DSL is exposed.
- **incremental_mode: changed**: Changed-only incremental processing is defined in
  the config but not yet implemented; it falls back to full mode with a warning.
- **Sibling merging**: Sibling stitching requires the warehouse connector to be running
  and producing URNs that match this connector's output. Verify URN alignment using
  `preview_urns: true` before full ingestion.

### Troubleshooting

#### URNs do not stitch with warehouse entities

Run with `preview_urns: true` to print a sample of generated sqlmesh and warehouse URN
pairs before emitting. Compare these against the URNs your warehouse connector produces.
Common causes:

- `target_platform_instance` mismatch — must be identical in both recipes
- Missing `default_catalog` — needed when model names are two-part but warehouse URNs
  are three-part
- Case mismatch — enable `convert_urns_to_lowercase: true` for Snowflake and other
  case-folding warehouses

#### Context fails to load

If ingestion logs `Could not initialize SQLMesh context`, check:

- The `project_path` points to a valid SQLMesh project directory (contains `config.yaml`)
- The specified `gateway` name matches a gateway defined in the project config
- All Python dependencies for Python models are installed in the ingestion environment
  (for example `ibis-framework` for ibis models)

#### DuckDB "cannot open file" error

If you see `IO Error: Cannot open file "...": No such file or directory` when using a DuckDB
gateway, your `config.yaml` contains a relative `database:` path (e.g. `db/myproject.db`).
SQLMesh resolves this path against the working directory of the process that loads it, not
against `project_path`.

Run the connector from the SQLMesh project directory:

```bash
cd /path/to/sqlmesh_project
datahub ingest -c recipe.yml
```

Or switch to an absolute path in your SQLMesh `config.yaml`:

```yaml
gateways:
  local:
    connection:
      type: duckdb
      database: /absolute/path/to/myproject.db
```
