### Capabilities

#### Supported model kinds

All SQLMesh model kinds are supported: `FULL`, `INCREMENTAL_BY_TIME_RANGE`,
`INCREMENTAL_BY_UNIQUE_KEY`, `INCREMENTAL_BY_PARTITION`, `SCD_TYPE_2`, `VIEW`, `SEED`,
`EXTERNAL`, and `EMBEDDED`. Each maps to a DataHub dataset subtype (`Model`, `Seed`,
`Source`, or `Embedded`).

#### Multi-project recipes

A single recipe can ingest multiple SQLMesh projects. Each entry in `projects` can
override `target_platform`, `target_platform_instance`, `sqlmesh_platform_instance`,
and `default_catalog` independently:

```yaml
source:
  type: sqlmesh
  config:
    projects:
      - project_path: /projects/snowflake
        gateway: snowflake_prod
        default_catalog: analytics
      - project_path: /projects/bigquery
        gateway: bigquery_prod
        default_catalog: my-gcp-project
        sqlmesh_platform_instance: bq_project
    env: PROD
```

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
