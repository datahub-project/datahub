# Oracle

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[oracle]'`.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, and tables
- Column types associated with each table
- Table, row, and column statistics via optional [SQL profiling](./sql_profiles.md)

Using the Oracle source requires that you've also installed the correct drivers; see the [cx_Oracle docs](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html). The easiest one is the [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html).

| Capability        | Status | Details                                  | 
|-------------------|--------|------------------------------------------|
| Data Containers   | ✔️     |                                          |
| Data Domains      | ✔️     | [link](../../docs/domains.md)            |

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: oracle
  config:
    # Coordinates
    host_port: localhost:5432
    database: dbname

    # Credentials
    username: user
    password: pass

    # Options
    service_name: svc # omit database if using this option

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

Exactly one of `database` or `service_name` is required.

As a SQL-based service, the Athena integration is also supported by our SQL profiler. See [here](./sql_profiles.md) for more details on configuration.

| Field                          | Required                       | Default  | Description                                                                                                                                                                                                                                                                       |
|--------------------------------|--------------------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `username`                     |                                |          | Oracle username. For more details on authentication, see the documentation: https://docs.sqlalchemy.org/en/14/dialects/oracle.html#dialect-oracle-cx_oracle-connect <br /> and https://cx-oracle.readthedocs.io/en/latest/user_guide/connection_handling.html#connection-strings. |
| `password`                     |                                |          | Oracle password.                                                                                                                                                                                                                                                                  |
| `host_port`                    |                                |          | Oracle host URL.                                                                                                                                                                                                                                                                  |
| `database`                     | If `service_name` is not set   |          | If using, omit `service_name`.                                                                                                                                                                                                                                                    |
| `service_name`                 | If `database_alias` is not set |          | Oracle service name. If using, omit `database`.                                                                                                                                                                                                                                   |
| `sqlalchemy_uri`               |                                |          | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters. |
| `database_alias`               |                                |          | Alias to apply to database when ingesting.                                                                                                                                                                                                                                        |
| `env`                          |                                | `"PROD"` | Environment to use in namespace when constructing URNs.                                                                                                                                                                                                                           |
| `options.<option>`             |                                |          | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.                                                                                           |
| `table_pattern.allow`          |                                |          | List of regex patterns for tables to include in ingestion.                                                                                                                                                                                                                        |
| `table_pattern.deny`           |                                |          | List of regex patterns for tables to exclude from ingestion.                                                                                                                                                                                                                      |
| `table_pattern.ignoreCase`     |                                | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                       |
| `schema_pattern.allow`         |                                |          | List of regex patterns for schemas to include in ingestion.                                                                                                                                                                                                                       |
| `schema_pattern.deny`          |                                |          | List of regex patterns for schemas to exclude from ingestion.                                                                                                                                                                                                                     |
| `schema_pattern.ignoreCase`    |                                | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                       |
| `view_pattern.allow`           |                                |          | List of regex patterns for views to include in ingestion.                                                                                                                                                                                                                         |
| `view_pattern.deny`            |                                |          | List of regex patterns for views to exclude from ingestion.                                                                                                                                                                                                                       |
| `view_pattern.ignoreCase`      |                                | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                                                                                                                       |
| `include_tables`               |                                | `True`   | Whether tables should be ingested.                                                                                                                                                                                                                                                |
| `include_views`                |                                | `True`   | Whether views should be ingested.                                                                                                                                                                                                                                                 |
| `domain.domain_key.allow`      |                                |          | List of regex patterns for tables/schemas to set domain_key domain key (domain_key can be any string like `sales`. There can be multiple domain key specified.                                                                                                                    |
| `domain.domain_key.deny`       |                                |          | List of regex patterns for tables/schemas to not assign domain_key. There can be multiple domain key specified.                                                                                                                                                                   |
| `domain.domain_key.ignoreCase` |                                | `True`   | Whether to ignore case sensitivity during pattern matching.There can be multiple domain key specified.                                                                                                                                                                            |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
