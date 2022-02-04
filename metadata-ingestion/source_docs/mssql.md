# Microsoft SQL Server

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[mssql]'`.

We have two options for the underlying library used to connect to SQL Server: (1) [python-tds](https://github.com/denisenkom/pytds) and (2) [pyodbc](https://github.com/mkleehammer/pyodbc). The TDS library is pure Python and hence easier to install, but only PyODBC supports encrypted connections.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, views and tables
- Column types associated with each table/view
- Table, row, and column statistics via optional [SQL profiling](./sql_profiles.md)

| Capability        | Status | Details                                  | 
|-------------------|--------|------------------------------------------|
| Platform Instance | ✔️     | [link](../../docs/platform-instances.md) |
| Data Containers   | ✔️     |                                          |
| Data Domains      | ✔️     | [link](../../docs/domains.md)            |


## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: mssql
  config:
    # Coordinates
    host_port: localhost:1433
    database: DemoDatabase

    # Credentials
    username: user
    password: pass

sink:
  # sink configs
```

<details>
  <summary>Example: using ingestion with ODBC and encryption</summary>

This requires you to have already installed the Microsoft ODBC Driver for SQL Server.
See https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15

```yml
source:
  type: mssql
  config:
    # Coordinates
    host_port: localhost:1433
    database: DemoDatabase

    # Credentials
    username: admin
    password: password

    # Options
    use_odbc: "True"
    uri_args:
      driver: "ODBC Driver 17 for SQL Server"
      Encrypt: "yes"
      TrustServerCertificate: "Yes"
      ssl: "True"

sink:
  # sink configs
```

</details>

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

As a SQL-based service, the Athena integration is also supported by our SQL profiler. See [here](./sql_profiles.md) for more details on configuration.

| Field                          | Required | Default            | Description                                                                                                                                                                             |
|--------------------------------|----------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `username`                     |          |                    | MSSQL username.                                                                                                                                                                         |
| `password`                     |          |                    | MSSQL password.                                                                                                                                                                         |
| `host_port`                    |          | `"localhost:1433"` | MSSQL host URL.                                                                                                                                                                         |
| `database`                     |          |                    | MSSQL database.                                                                                                                                                                         |
| `database_alias`               |          |                    | Alias to apply to database when ingesting.                                                                                                                                              |
| `use_odbc`                     |          | `False`            | See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc.                                                                                      |
| `uri_args.<uri_arg>`           |          |                    | Arguments to URL-encode when connecting. See https://docs.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver15.                                   |
| `env`                          |          | `"PROD"`           | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `platform_instance`            |          | None               | The Platform instance to use while constructing URNs.                                                                                                                                   |
| `options.<option>`             |          |                    | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `table_pattern.allow`          |          |                    | List of regex patterns for tables to include in ingestion.                                                                                                                              |
| `table_pattern.deny`           |          |                    | List of regex patterns for tables to exclude from ingestion.                                                                                                                            |
| `table_pattern.ignoreCase`     |          | `True`             | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `schema_pattern.allow`         |          |                    | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.deny`          |          |                    | List of regex patterns for schemas to exclude from ingestion.                                                                                                                           |
| `schema_pattern.ignoreCase`    |          | `True`             | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `view_pattern.allow`           |          |                    | List of regex patterns for views to include in ingestion.                                                                                                                               |
| `view_pattern.deny`            |          |                    | List of regex patterns for views to exclude from ingestion.                                                                                                                             |
| `view_pattern.ignoreCase`      |          | `True`             | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `include_tables`               |          | `True`             | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`                |          | `True`             | Whether views should be ingested.                                                                                                                                                       |
| `domain.domain_key.allow`      |          |                    | List of regex patterns for tables/schemas to set domain_key domain key (domain_key can be any string like `sales`. There can be multiple domain key specified.                          |
| `domain.domain_key.deny`       |          |                    | List of regex patterns for tables/schemas to not assign domain_key. There can be multiple domain key specified.                                                                         |
| `domain.domain_key.ignoreCase` |          | `True`             | Whether to ignore case sensitivity during pattern matching.There can be multiple domain key specified.                                                                                  |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
