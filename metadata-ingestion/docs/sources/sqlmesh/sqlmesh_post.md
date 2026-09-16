### Capabilities

#### Supported model kinds

All SQLMesh model kinds are supported: `FULL`, `INCREMENTAL_BY_TIME_RANGE`,
`INCREMENTAL_BY_UNIQUE_KEY`, `INCREMENTAL_BY_PARTITION`, `SCD_TYPE_2`, `VIEW`, `SEED`,
`EXTERNAL`, and `EMBEDDED`. Each maps to a DataHub dataset subtype (`Model`, `Seed`,
`Source`, or `Embedded`).

#### Data quality assertions

SQLMesh audits become DataHub **`CUSTOM`** assertions attached to the SQLMesh
logical URN (sibling stitching surfaces them on the warehouse sibling in the
UI):

- Each built-in audit (`not_null`, `unique_values`,
  `unique_combination_of_columns`, `number_of_rows`, `forall`,
  `accepted_range`, `accepted_values`) and any unrecognised audit name becomes
  an `AssertionType.CUSTOM` assertion with `customAssertion.type = "SQLMesh"`.
- Useful semantics (scope / operator / aggregation / fields / kwargs) are
  carried as custom properties. The source plugin does **not** invent a SQL
  statement for DataHub to evaluate — SQLMesh executes the audits.
- Pass/fail history comes from an external `audit_results_path` JSON
  (`sqlmesh audit --output`). Failures can emit Incidents when
  `emit_incidents_on_failure` is enabled.

#### Freshness and volume signals

The source plugin does **not** emit `FRESHNESS` or `VOLUME` assertion definitions
(there is no Cloud path that auto-attaches a monitor to an ingested assertion).
Instead it emits the timeseries signals users can point monitors at:

- **`OperationAspect`** with `customOperationType=SQLMESH_FINGERPRINT_REBUILD`
  from `snapshot.updated_ts` (when the SQLMesh state store is reachable)
- **`DatasetProfile.rowCount`** from a warehouse `COUNT(*)` against the
  physical fingerprint table (when the gateway engine adapter is reachable)

`sqlmesh.cron` / `interval_unit` are still ingested as dataset custom
properties for reference; they are **not** mapped into a freshness SLA.

#### Stateful ingestion

When `stateful_ingestion.enabled: true`, the source plugin tracks emitted URNs across runs
and soft-deletes entities that have been removed from the SQLMesh project.

### Limitations

- **Python models**: Python-based SQLMesh models (ibis, pandas) are supported for
  schema extraction when `columns_to_types` is defined, but column-level lineage is
  not available.
- **Audit run results**: Pass/fail status on the Validation tab requires an external
  JSON results file (`audit_results_path`). The source plugin does not execute audits
  itself at ingestion time.
- **Sibling merging**: Sibling stitching requires the warehouse source plugin to be running
  and producing URNs that match this source plugin's output. Verify URN alignment using
  `preview_urns: true` before full ingestion. The warehouse sibling edge is patched
  (not overwritten) so an existing dbt sibling relationship is preserved.

### Troubleshooting

#### URNs do not stitch with warehouse entities

Run with `preview_urns: true` to print a sample of generated sqlmesh and warehouse URN
pairs before emitting. Compare these against the URNs your warehouse source plugin produces.
Common causes:

- `target_platform_instance` mismatch — must be identical in both recipes
- Missing `default_catalog` — needed when model names are two-part but warehouse URNs
  are three-part
- Case mismatch — enable `convert_urns_to_lowercase: true` for Snowflake and other
  case-folding warehouses

#### Context fails to load

If ingestion logs `Could not initialize SQLMesh context`, check:

- The `project_path` points to a valid SQLMesh project directory (contains a
  `config.yaml` or `config.py`)
- The specified `gateway` name matches a gateway defined in the project config
- All Python dependencies for Python models are installed in the ingestion environment
  (for example `ibis-framework` for ibis models)

#### DuckDB "cannot open file" error

If you see `IO Error: Cannot open file "...": No such file or directory` when using a DuckDB
gateway, your `config.yaml` contains a relative `database:` path (e.g. `db/myproject.db`).
SQLMesh resolves this path against the working directory of the process that loads it, not
against `project_path`.

Run the source plugin from the SQLMesh project directory:

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
