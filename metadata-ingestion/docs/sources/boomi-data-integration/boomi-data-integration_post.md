### Capabilities

#### Rivers and jobs

For every environment, the source plugin lists the rivers and then reads each river's definition. Each river becomes a DataFlow with the custom properties `river_type`, `river_status`, `schedule` and `datasource_id` where the API returns them, and the river description when present. Source-to-target rivers whose definition can be read also get `run_type`, `source` and `target`.

A logic river's DataFlow links to the river in the Data Integration console. Other river types get no link yet, because their console path has not been verified. The console host is only known for the `us` region. For other regions, set `console_url` to the base URL of your console, or no link is emitted.

- A source-to-target river gets one `load` DataJob whose inputs are the source tables and whose outputs are the target tables.
- An action or connector executor river gets one `run` DataJob with subtype `Run`.
- A logic river gets one DataJob per step, named by the step ID. Containers (`run_once`, `loop`, `condition`) are flattened. Inside a sequential container each step depends on the previous one; steps in a parallel container and the branches of a condition all depend on whatever ran before the container. A condition branch that runs no step passes its predecessors through. Disabled steps are skipped.
- A `river` step records the river it runs in the `river_id` property and also depends on that river's job: the `load` job of a source-to-target river, the `run` job of an action or connector executor river, or the steps that can run last in a logic river, such as every branch of a final parallel container or condition. The link is only made when the called river is ingested in the same run and environment. Calls to rivers that were filtered out, failed or are in another environment are counted in `river_step_links_unresolved`.

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

#### Source lineage

The source platform comes from the river's source type. `connection_type_platform_map` and an entry in `connection_to_platform` for the source connection override it, as for targets.

| Source                                    | DataHub platform               | Table name                                                           |
| ----------------------------------------- | ------------------------------ | -------------------------------------------------------------------- |
| Postgres, SQL Server, Snowflake, Redshift | `postgres`, `mssql`, and so on | `database.schema.table`, database from `connection_to_platform`      |
| BigQuery                                  | `bigquery`                     | `project.dataset.table`, project from `connection_to_platform`       |
| MySQL, MariaDB                            | `mysql`, `mariadb`             | `database.table`, database from the river's schema name              |
| Oracle                                    | `oracle`                       | `schema.table`, or `database.schema.table` when `database` is mapped |

- The `connection_to_platform` database or project is required for multi-table rivers and for SQL that leaves it out of table names. Fully qualified table names in custom queries and logic SQL steps resolve without it.
- Multi-table rivers read each selected table of each schema in the river.
- Custom query rivers read the tables in their SQL. The query is parsed with the source platform's dialect, using `database` and `default_schema` from the source connection in `connection_to_platform` for unqualified names.
- Logic SQL query and script steps read the tables in their SQL, parsed with the step's warehouse dialect and the step connection's `database` and `default_schema`. Tables an `INSERT`, `UPDATE`, `MERGE` or `CREATE TABLE` in the SQL writes become outputs too; for script steps they are the only outputs. An `UPDATE`'s own target is not listed as an input, but in a script a table written by one statement and read by a later one is.
- A SQL table is only used when its name has as many parts as the platform's URNs, so a table missing its database or schema is skipped rather than given a name that would not join.
- Source tables of a multi-table river with a target prefix still get lineage, even though the targets are skipped.

#### Column lineage

Column lineage is on by default and can be turned off with `include_column_lineage: false`. It is only emitted where both columns are known:

- **Multi-table rivers:** the river lists only the columns you renamed, retyped, excluded or added. Each selected, renamed or retyped column gets an edge from the source column to its target name (`alias`, or the source name). Calculated columns and Data Integration metadata columns get no edge, and columns the river does not list get none either.
- **Custom query rivers and logic SQL query steps loading a table:** each named output column of the `SELECT` maps to the column of the same name in the target table. A river or step with its own field mapping gets no column lineage.
- **`INSERT ... SELECT`:** target columns are matched to the `SELECT` by position in the `INSERT` column list. An `INSERT` without a column list gets no column lineage. Script steps with more than one statement get table lineage only.

When the source plugin can reach DataHub, it matches columns to the dataset schemas already ingested, ignoring case, and keeps their stored field paths. Columns a known dataset does not have are dropped. Skips are counted by reason in `column_lineage_skipped`, for example `missing_target` when a multi-table river's target table is not resolved and `invalid_column` for a column entry with an unexpected shape.

#### Owners

With `extract_owners: true`, the river creator becomes a data owner and the last editor, when different, a technical owner, on the river's DataFlow and DataJobs. Rivers only carry display names, so the source plugin reads the account's users (this needs the `user:list` scope) and caches the first successful response; after a failed request it tries the next environment's token and matches names to emails, ignoring case. Names that match no user, or only a user without an email, are skipped and counted in `owners_unknown_names`. Names shared by more than one user, including a user without an email, are skipped and counted in `owners_ambiguous_names`. Owners are `urn:li:corpuser:<email>`.

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

- Source lineage covers database sources only. Rivers from SaaS sources such as Salesforce or HubSpot get target lineage only and are counted in `sources_unsupported`. Single-table and predefined-report rivers from database sources also get target lineage only and are counted under `unsupported_run_type` in `source_lineage_skipped`.
- Observed targets from run activity are not used.
- The API documents that it returns HTTP 400 when asked for the definition of a river created in the UI. The source plugin still tries every river. Rivers without a definition are ingested from the list fields only, with no lineage or logic steps, and counted in `rivers_without_definition`.
- When a multi-table river sets a target prefix, its targets are skipped and listed in `targets_prefix_skipped`, because how the prefix combines with each table name is not documented.
- S3, GCS, Blob Storage and OneLake targets get no lineage.
- BigQuery targets get no lineage unless the target connection is in `connection_to_platform` with the project as `database`.
- Runs older than two weeks are not available from the API.
- A job's lineage is only written when the source plugin finds at least one source or target table, so lineage added in the UI or by other sources is not overwritten with an empty list. If a river's target later stops resolving, for example because it moved to object storage or `include_lineage` was turned off, the lineage from earlier runs stays until it is removed in DataHub.

### Troubleshooting

#### Warning "Environment not accessible"

With `api_token`, the token returned HTTP 401 or 403 for an environment it can list but not read. Tokens are scoped to one environment, so add an entry for that environment to `environment_tokens` to ingest it.

#### Failure "Configured environment not accessible"

An environment in `environment_tokens` could not be read, so it was skipped and stale entity removal did not run. Replace its token, or remove the entry from `environment_tokens` if you no longer want to ingest it. `environment_pattern` does not help here, because the source plugin reads each configured environment before applying the pattern.

#### Warning "River definitions unavailable"

The rivers listed in `rivers_without_definition_ids` could not be read. They are ingested without lineage. This is expected for rivers created in the UI.

#### Warning "Cannot read Boomi Data Integration run history"

The token for the environment whose `environment_id` is in the warning's context does not have the `activity:list` scope, so run history was skipped for that environment. Add the scope to the token, or set `include_run_history: false`.

#### Warning "Failed to list Boomi Data Integration activities" or "Failed to list river runs"

The API returned an error while listing runs, so run history was skipped for that environment or river. Rivers and jobs are still ingested, and stale entity removal still runs, because runs are not part of stale state. The next run picks the history up again.

#### "Too many pages"

A listing returned a next page past the source plugin's limit of 500 pages, so later records were not ingested. For environments and rivers this is a failure, and stale entity removal does not run. For run history it is a warning, and only the runs past the limit are skipped. Lower `run_history_days` if it keeps happening on run history.

#### Missing target lineage

Check `targets_missing_bigquery_project`, `targets_missing_location`, `targets_prefix_skipped` and `targets_unsupported` in the ingestion report.

#### Warning "Source database unknown"

A multi-table river reads from a platform whose URNs need a database, and its source connection is not in `connection_to_platform` or has no `database`. Add the connection ID with `platform` and `database`. Affected rivers are listed in `sources_missing_database`.

#### Warning "SQL table not fully qualified" or "Failed to parse SQL"

A custom query or logic SQL step names a table without its database or schema, or its SQL could not be parsed. For unqualified tables, set `database` and `default_schema` on the connection in `connection_to_platform`; they are counted under `unqualified_sql_table` in `source_lineage_skipped`. Rivers and steps whose SQL could not be parsed are listed in `sql_parse_failures` and counted under `sql_parse_failed`.

#### River step not linked to the river it runs

The called river was not ingested in the same run and environment, for example because `river_pattern` or `group_pattern` filtered it out or its definition failed. Check `river_step_links_unresolved` in the ingestion report and the filters.

#### Warning "Users list not readable"

`extract_owners` is on but the users list could not be read. The warning says whether the token was rejected (HTTP 401) or lacks the `user:list` scope (HTTP 403); add the scope or replace the token. For other errors, check the error in the warning. You can also turn `extract_owners` off.
