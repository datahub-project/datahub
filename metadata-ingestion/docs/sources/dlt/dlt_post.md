### Capabilities

| Capability                        | Status                                   | Notes                                                                          |
| --------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------ |
| DataFlow / DataJob                | ✅ Always                                | One DataFlow per pipeline, one DataJob per destination table                   |
| Outlet lineage                    | ✅ Always (when `include_lineage: true`) | Requires `destination_platform_map` to match your destination connector        |
| Inlet lineage                     | ✅ User-configured                       | dlt does not store source identity; configure via `source_dataset_urns`        |
| Column-level lineage              | ✅ Partial                               | Only for tables with exactly one inlet and one outlet (unambiguous 1:1 copy)   |
| Run history (DataProcessInstance) | ⚙️ Opt-in                                | Requires `include_run_history: true` + dlt installed + destination credentials |
| Deletion detection                | ✅ Via stateful ingestion                | Removes DataFlow/DataJob when pipeline is deleted from `pipelines_dir`         |
| Ownership                         | ❌ Not supported                         | dlt state does not contain owner information                                   |

#### Lineage Stitching

For outlet lineage to connect to your destination's Dataset URNs, `destination_platform_map` must match the environment and platform instance used by your destination connector.

If your Postgres connector uses `env: PROD` and no `platform_instance`:

```yaml
destination_platform_map:
  postgres:
    env: PROD
    platform_instance: null
    database: my_database # required for 3-part URN: database.schema.table
```

**Why `database` is needed for SQL destinations**: dlt stores the schema name (`dataset_name`) but not the database name. Postgres URNs in DataHub use a 3-part format (`database.schema.table`). Supply `database` to match those URNs.

Cloud warehouses (BigQuery, Snowflake) use the project or account as `platform_instance` instead:

```yaml
destination_platform_map:
  bigquery:
    platform_instance: "my-gcp-project"
    env: PROD
  snowflake:
    platform_instance: "my-account"
    env: PROD
```

#### Inlet Lineage (Upstream Sources)

dlt does not record where data came from — only where it went. To enable upstream lineage, manually configure Dataset URNs.

For REST API pipelines (all tables share the same source):

```yaml
source_dataset_urns:
  my_pipeline:
    - "urn:li:dataset:(urn:li:dataPlatform:salesforce,contacts,PROD)"
```

For `sql_database` pipelines (each table maps 1:1 to a source table):

```yaml
source_table_dataset_urns:
  my_pipeline:
    my_table:
      - "urn:li:dataset:(urn:li:dataPlatform:postgres,prod_db.public.my_table,PROD)"
```

#### Run History

Run history requires the dlt package to be installed in the DataHub ingestion environment and destination credentials to be accessible:

```bash
pip install "dlt[postgres]"   # or dlt[bigquery], dlt[snowflake], etc.
```

Credentials are read from `~/.dlt/secrets.toml` (dlt's standard location) or environment variables:

```bash
export DESTINATION__POSTGRES__CREDENTIALS__HOST=localhost
export DESTINATION__POSTGRES__CREDENTIALS__DATABASE=my_db
export DESTINATION__POSTGRES__CREDENTIALS__USERNAME=dlt
export DESTINATION__POSTGRES__CREDENTIALS__PASSWORD=secret
```

The `run_history_config` time window is respected — configure `start_time` and `end_time` to limit which loads are ingested:

```yaml
include_run_history: true
run_history_config:
  start_time: "-7 days"
  end_time: "now"
```

### Limitations

#### Inlet lineage requires manual configuration

dlt's state files do not store source-system connection details. Inlet (upstream) Dataset URNs must be configured by the user via `source_dataset_urns` or `source_table_dataset_urns`.

#### Run history requires destination access

Querying `_dlt_loads` requires the dlt package and destination credentials. When dlt is not installed or credentials are missing, the connector still emits DataFlow / DataJob / outlet lineage but skips run history.

#### Ownership is not supported

dlt does not record pipeline owners.

### Troubleshooting

#### No entities emitted

- Check that `pipelines_dir` points to a directory containing subdirectories with `schemas/` inside them
- Run `datahub ingest -c recipe.yml --test-source-connection` to verify the path is readable

#### Lineage not stitching

- Verify `destination_platform_map` env/instance/database match exactly what your destination connector uses
- Check the destination Dataset URNs in DataHub and compare to what the dlt connector constructs
- For Postgres: ensure `database` is set in `destination_platform_map.postgres`

#### Run history empty

- Confirm `include_run_history: true` is set
- Confirm the dlt package is installed: `python -c "import dlt; print(dlt.__version__)"`
- Confirm destination credentials are in `~/.dlt/secrets.toml` or environment variables
- Check DataHub ingestion logs for warnings from the dlt connector

#### Nested child tables (e.g. `orders__items`)

dlt automatically unnests nested JSON into child tables using double-underscore naming. These appear as separate DataJobs with `parent_table` set in their custom properties. This is expected behavior.
