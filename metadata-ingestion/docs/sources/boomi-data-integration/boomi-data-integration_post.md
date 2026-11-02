### Capabilities

#### Rivers and jobs

For every environment, the source plugin lists the rivers and then reads each river's definition. Each river becomes a DataFlow with the custom properties `river_type`, `river_status`, `run_type`, `schedule`, `datasource_id`, `source` and `target`, and the river description.

- A source-to-target river gets one `load` DataJob whose outputs are the target tables.
- An action or connector executor river gets one `run` DataJob.
- A logic river gets one DataJob per step, named by the step ID. Containers (`run_once`, `loop`, `condition`) are flattened. Inside a sequential container each step depends on the previous one; steps in a parallel container and the branches of a condition all depend on whatever ran before the container. A `river` step records the river it runs in the `river_id` property. Disabled steps are skipped.

#### Target lineage

The target platform comes from the river's target type. `connection_type_platform_map` overrides it per target type, and an entry in `connection_to_platform` for the target connection overrides both.

| Target             | DataHub platform | Table name                                                     |
| ------------------ | ---------------- | -------------------------------------------------------------- |
| Snowflake          | `snowflake`      | `database.schema.table`                                        |
| BigQuery           | `bigquery`       | `project.dataset.table`, project from `connection_to_platform` |
| Redshift           | `redshift`       | `schema.table`                                                 |
| Postgres (RDS)     | `postgres`       | `schema.table`                                                 |
| Azure SQL, Synapse | `mssql`          | `schema.table`                                                 |
| Athena             | `athena`         | `schema.table`                                                 |
| Databricks         | `databricks`     | `catalog.schema.table`                                         |

- For Redshift, Postgres and SQL Server targets, set `database` on the connection in `connection_to_platform` to get `database.schema.table`, which is how those source plugins name tables.
- For Databricks, `database` is used when the river has no catalog.
- Single-table rivers use the target table name. Multi-table rivers emit one table per selected source table, using its target table name or, if unset, the source table name.
- Table names are lowercased unless `convert_lineage_urns_to_lowercase` is `false`, or `convert_urns_to_lowercase` is `false` on the connection in `connection_to_platform`.
- Logic SQL query steps that write to a table get the same treatment, with the platform taken from the step type, for example `snowflake_sql_query`.

### Limitations

- Source-side lineage is not extracted yet, so `load` jobs only have outputs.
- The API documents that it returns HTTP 400 when asked for the definition of a river created in the UI. The source plugin still tries every river. Rivers without a definition are ingested from the list fields only, with no lineage or logic steps, and counted in `rivers_without_definition`.
- When a multi-table river sets a target prefix, its targets are skipped and listed in `targets_prefix_skipped`, because how the prefix combines with each table name is not documented.
- S3, GCS, Blob Storage and OneLake targets get no lineage.
- BigQuery targets get no lineage unless the target connection is in `connection_to_platform` with the project as `database`.
- Run history and owners are not ingested yet.

### Troubleshooting

#### Warning "Environment not accessible"

The token returned HTTP 401 or 403 for that environment. Tokens are scoped to one environment, so add a token for it to `environment_tokens`, or deny the environment with `environment_pattern`.

#### Warning "River definitions unavailable"

The rivers listed in `rivers_without_definition_ids` could not be read. They are ingested without lineage. This is expected for rivers created in the UI.

#### Missing target lineage

Check `targets_missing_bigquery_project`, `targets_missing_location`, `targets_prefix_skipped` and `targets_unsupported` in the ingestion report.
