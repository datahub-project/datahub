### Capabilities

#### Rivers and jobs

For every environment, the source plugin lists the rivers and then reads each river's definition. Each river becomes a DataFlow with the custom properties `river_type`, `river_status`, `schedule` and `datasource_id` where the API returns them, and the river description when present. Source-to-target rivers whose definition can be read also get `run_type`, `source` and `target`.

A logic river's DataFlow links to the river in the Data Integration console. Other river types get no link yet, because their console path has not been verified. The console host is only known for the `us` region. For other regions, set `console_url` to the base URL of your console, or no link is emitted.

- A source-to-target river gets one `load` DataJob whose outputs are the target tables.
- An action or connector executor river gets one `run` DataJob with subtype `Run`.
- A logic river gets one DataJob per step, named by the step ID. Containers (`run_once`, `loop`, `condition`) are flattened. Inside a sequential container each step depends on the previous one; steps in a parallel container and the branches of a condition all depend on whatever ran before the container. A condition branch that runs no step passes its predecessors through. A `river` step records the river it runs in the `river_id` property. Disabled steps are skipped.

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
- Single-table rivers use the target table name. Multi-table rivers emit one table per selected source table, using its target table name or, if unset, the source table name. A multi-table river with no selected tables gets no lineage.
- Table names are lowercased unless `convert_lineage_urns_to_lowercase` is `false`, or `convert_urns_to_lowercase` is `false` on the connection in `connection_to_platform`.
- Logic SQL query steps that write to a table get the same treatment, with the platform taken from the step type, for example `snowflake_sql_query`. Table-target steps of other types, or without a table name, are listed in `targets_unsupported` or `targets_missing_location`.

#### Run history

With `include_run_history` (on by default), the source plugin reads the runs of the last `run_history_days` days (7 by default, at most 14, because Data Integration keeps run logs for two weeks) and emits each as a DataProcessInstance. It first reads the environment's activity summary for the window, so runs are only listed for rivers that ran.

- A source-to-target river's runs belong to its `load` job, and an action or connector executor river's runs to its `run` job. A logic river run covers every step, so it belongs to the river's DataFlow.
- A multi-table river returns one row per table, all with the same `run_group_id`. These rows become one run that starts with the first table and ends with the last. The run is marked failed if any table failed, unless a table is still pending or running or has an unknown status, in which case the run gets a start event only.
- Each river keeps its `max_runs_per_river` most recent runs (50 by default). `runs_scanned` and `runs_over_cap` count runs, so a multi-table run counts once.
- A run started by a logic river is linked to that logic run as its parent when the API returns the parent run ID and both runs are in the window and within their river's `max_runs_per_river`.
- Runs triggered by `scheduled` or `rescheduled` are scheduled runs, and other triggers are ad hoc. A run started by a logic river takes the trigger of the run that started the chain.
- Each run has the properties `status`, `trigger_name`, `units`, `rpu`, `error_description` (first 500 characters), `target_name` (or `table_count` for a multi-table run), `run_group_id` and `sub_river_id` where the API returns them.
- Each run links to its run in the console when the river link is emitted and the run has a `run_group_id`.

| Data Integration status | DataHub result                                                                |
| ----------------------- | ----------------------------------------------------------------------------- |
| `succeeded`             | Success                                                                       |
| `failed`                | Failure                                                                       |
| `canceled`, `skipped`   | Skipped                                                                       |
| `pending`, `running`    | Start event only, until a later ingestion sees it finish                      |
| Anything else           | Start event only, with an "Unknown Boomi Data Integration run status" warning |

### Limitations

- Source-side lineage is not extracted yet, so `load` jobs only have outputs.
- The API documents that it returns HTTP 400 when asked for the definition of a river created in the UI. The source plugin still tries every river. Rivers without a definition are ingested from the list fields only, with no lineage or logic steps, and counted in `rivers_without_definition`.
- When a multi-table river sets a target prefix, its targets are skipped and listed in `targets_prefix_skipped`, because how the prefix combines with each table name is not documented.
- S3, GCS, Blob Storage and OneLake targets get no lineage.
- BigQuery targets get no lineage unless the target connection is in `connection_to_platform` with the project as `database`.
- Owners are not ingested yet.
- Runs older than two weeks are not available from the API.
- A job's lineage is only written when the source plugin finds at least one target, so lineage added in the UI or by other sources is not overwritten with an empty list. If a river's target later stops resolving, for example because it moved to object storage or `include_lineage` was turned off, the lineage from earlier runs stays until it is removed in DataHub.

### Troubleshooting

#### Warning "Environment not accessible"

With `api_token`, the token returned HTTP 401 or 403 for an environment it can list but not read. Tokens are scoped to one environment, so add an entry for that environment to `environment_tokens` to ingest it.

#### Failure "Configured environment not accessible"

An environment in `environment_tokens` could not be read, so it was skipped and stale entity removal did not run. Replace its token, or remove the entry from `environment_tokens` if you no longer want to ingest it. `environment_pattern` does not help here, because the source plugin reads each configured environment before applying the pattern.

#### Warning "River definitions unavailable"

The rivers listed in `rivers_without_definition_ids` could not be read. They are ingested without lineage. This is expected for rivers created in the UI.

#### Warning "Cannot read Boomi Data Integration run history"

The token for the environment whose `environment_id` is in the warning's context does not have the `activity:list` scope, so run history was skipped for that environment. Add the scope to the token, or set `include_run_history: false`.

#### Missing target lineage

Check `targets_missing_bigquery_project`, `targets_missing_location`, `targets_prefix_skipped` and `targets_unsupported` in the ingestion report.
