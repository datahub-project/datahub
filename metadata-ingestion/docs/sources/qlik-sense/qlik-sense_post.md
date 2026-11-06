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

Variables set with `SET`, or with `LET` and a quoted string, are expanded in connection names and SQL. Variables set with `LET` from an expression, and scripts pulled in with `$(Include=...)` or `$(Must_Include=...)`, are not read. Conditions are not evaluated: a connection or variable set inside an `IF` or `SWITCH` block is kept after the block only when every branch sets the same value.

A `SELECT` whose connection resolves to no platform is skipped, and the connection is listed under `unresolved_data_connections` in the ingestion report. A `SELECT` that cannot be parsed, for example one that uses an unexpanded variable in a table name, is skipped and reported as a warning.

#### Charts in containers

Charts inside a container object's tabs are ingested as charts of the sheet. An untitled chart takes its tab label as its title. Objects with no data, such as buttons, filter panes and variable inputs, are skipped.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
