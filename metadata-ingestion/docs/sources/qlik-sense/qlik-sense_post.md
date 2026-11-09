### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Lineage from load scripts

On-prem, the connector always reads each app's load script. On Qlik Cloud it reads the script when the app's data model has no tables, or when some of its tables have no upstream lineage, for example apps loaded through the data load editor. Tables already traced by the data model are not added again from the script. A `SELECT` that runs after `LIB CONNECT TO`, `ODBC CONNECT TO`, or `OLEDB CONNECT TO` becomes a Qlik table with lineage to the tables it reads, as long as its data connection resolves to a platform and the SQL can be parsed. The upstream platform and database come from the data connection. Use `data_connection_to_platform_instance` when they cannot be inferred, or when the upstream source uses a platform instance or env:

```yaml
data_connection_to_platform_instance:
  "My SQL Server connection":
    platform: mssql
    database: sales_db
    default_schema: dbo
  "My Databricks connection":
    platform: databricks
    database: main
```

Variables set with `SET`, or with `LET` and a quoted string, are expanded in connection names and SQL. Variables set with `LET` from an expression, and scripts pulled in with `$(Include=...)` or `$(Must_Include=...)`, are not read. After an include, the connection and all variables are treated as unknown. A `SUB` body is read where the script calls it, with the connection and variables in effect at that `CALL`, and a `SUB` that is never called adds no tables. Quoted or numeric arguments of a `CALL` are bound to the `SUB` parameters for that call, and any other argument leaves its parameter unknown. The variable of a `FOR` or `FOR EACH` loop is unknown inside the loop. Nothing after an unconditional `EXIT SCRIPT` is read. An unlabelled `SELECT` table is named after the table that follows `FROM`, cut to 32 characters, as Qlik names it. `--` comments in SQL are ignored. A `CALL` of a `SUB` the script does not define makes the connection and all variables unknown, as an include does. After either, the table loaded last is unknown too, so a `Concatenate` or `Join` without a table name is skipped until another table is loaded. A `SELECT` that runs while the connection is unknown is skipped. Rows added with `Concatenate (Table)` or fields added with `Join (Table)` belong to the named table, and without a table name to the table loaded just before. Conditions are not evaluated: a connection or variable set inside an `IF` or `SWITCH` block is kept after the block only when every branch sets the same value. An `IF` without `ELSE` or a `SWITCH` without `DEFAULT` can run no branch, so the value from before the block must match as well.

A connection named in `LIB CONNECT TO` resolves through `data_connection_to_platform_instance`, or through a data connection the connector can list, whose platform comes from its connector type or connection string. The platform is never guessed from the connection name, so an unlisted name without a mapping does not resolve. On Qlik Cloud, a space-prefixed name resolves to the connection in that space, and a name without a space prefix resolves only to the connection with that name in the app's own space (the personal space for personal apps), even when another space holds a connection with that name. Map the name in `data_connection_to_platform_instance` when the app's own connection is not visible to the API key. An inline `ODBC CONNECT TO` or `OLEDB CONNECT TO` connection string resolves from its driver or provider. Only the properties that name where the data lives (driver, provider, server, host, port, data source, database, catalog, schema, DSN, warehouse, and account) are kept when an inline connection string is reported or ingested, and URLs in them lose their user name, password, and query string.

A `SELECT` whose connection resolves to no platform is skipped, and the connection is listed under `unresolved_data_connections` in the ingestion report. A `SELECT` that cannot be parsed, for example one that uses an unexpanded variable in a table name, is skipped and reported as a warning. On SQL Server, Snowflake, PostgreSQL, Redshift, Databricks, BigQuery, MySQL, MariaDB, and Oracle, a table whose database or schema is neither in the SQL nor resolved for its connection is left out of lineage and reported as a warning. Set `database` and `default_schema` for the connection to include it.

On Qlik Cloud, only the Owner and users with **Can manage** or **Can operate** in a managed space can read an app's load script. When the Engine denies access to the script, the app is still ingested and the run reports a warning. Charts of an app whose script was denied get their inputs from the data model. Charts of an app whose data model or load script could not be read for another reason keep the inputs from their last read. Any other failure to read the script marks the app as partly read, which keeps stale metadata removal from deleting its lineage.

#### Charts in containers

Charts inside a container object's tabs are ingested as charts of the sheet. An untitled chart takes its tab label as its title. Objects with no data, such as buttons, filter panes and variable inputs, are skipped.

#### Large sites

On-prem, each app is opened in the Engine without its data. On Qlik Cloud, the Engine loads each app's data when the app is opened, so every worker holds an app in memory, and a rate-limited Engine connection is not retried and leaves its app unread. Raise `max_workers` on Qlik Cloud with care. `max_workers` sets how many apps and datasets of one space or stream are read at the same time. It defaults to 4 on-prem and to 1 on Qlik Cloud. Spaces and streams are read one after another. Each worker holds its own Engine session under the configured user. `max_workers` applies to Qlik Cloud too, where a rate-limited (HTTP 429) REST request is retried up to 5 times. Each retry waits for the `Retry-After` time when Qlik sends one, and otherwise backs off for 0, 2, 4, 8 and 16 seconds. A request that is still rate-limited after that fails, and the space, item or app it belonged to is reported as unread or partly read, which keeps stale metadata removal from deleting its metadata. Lower `max_workers` if this happens.

With `skip_unchanged_apps: true` in the `on_prem` block and stateful ingestion enabled, later runs only open apps that changed in the Engine. An app is read again when the app or any of its sheets, stories or master items is saved, when it is reloaded, published or moves stream, when its owner changes, when the recipe or a data connection changes, or after a connector upgrade. It is also read again if the last read of it failed, if its app container in DataHub is missing or does not carry the fingerprint of that read (stored as the container's `ingestionFingerprint` custom property), or if any entity that read produced is missing from DataHub or soft-deleted. Aspects that failed to write for an entity that exists are not detected; set `stateful_ingestion.ignore_old_state: true` for one run to read every app again. Entities of a skipped app stay in place and are not soft-deleted. When an app is only partly read and its last read was recorded, nothing from the partial read is emitted, the entities from its last read are kept, and it is read again on the next run. When its last read was not recorded, for example because the previous run had `skip_unchanged_apps` turned off, the partial read is emitted and the run reports a failure, which keeps stale metadata removal from deleting anything.

Column-level lineage is resolved against the upstream schemas in DataHub at read time. After ingesting an upstream source for the first time, set `stateful_ingestion.ignore_old_state: true` for one run so every app is read again.

```yaml
max_workers: 4
on_prem:
  # server and certificates as above
  skip_unchanged_apps: true
stateful_ingestion:
  enabled: true
  remove_stale_metadata: true
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
