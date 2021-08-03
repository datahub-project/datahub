# Oracle

## Setup

To install this plugin, run `pip install 'acryl-datahub[oracle]'`.

## Capabilities

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table

Using the Oracle source requires that you've also installed the correct drivers; see the [cx_Oracle docs](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html). The easiest one is the [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html).

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: oracle
  config:
    # For more details on authentication, see the documentation:
    # https://docs.sqlalchemy.org/en/14/dialects/oracle.html#dialect-oracle-cx_oracle-connect and
    # https://cx-oracle.readthedocs.io/en/latest/user_guide/connection_handling.html#connection-strings.
    username: user
    password: pass
    host_port: localhost:5432
    database: dbname
    service_name: svc # omit database if using this option
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                  | Required | Default  | Description                                                                                                                                                                             |
| ---------------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `username`             |          |          |                                                                                                                                                                                         |
| `password`             |          |          |                                                                                                                                                                                         |
| `host_port`            |          |          |                                                                                                                                                                                         |
| `database`             |          |          |                                                                                                                                                                                         |
| `service_name`         |          |          |                                                                                                                                                                                         |
| `env`                  | ❌       | `"PROD"` | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `options.<option>`     | ❌       |          | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `table_pattern.allow`  | ❌       |          | Regex pattern for tables to include in ingestion.                                                                                                                                       |
| `table_pattern.deny`   | ❌       |          | Regex pattern for tables to exclude from ingestion.                                                                                                                                     |
| `schema_pattern.allow` | ❌       |          | Regex pattern for schemas to include in ingestion.                                                                                                                                      |
| `schema_pattern.deny`  | ❌       |          | Regex pattern for schemas to exclude from ingestion.                                                                                                                                    |
| `view_pattern.allow`   | ❌       |          | Regex pattern for views to include in ingestion.                                                                                                                                        |
| `view_pattern.deny`    | ❌       |          | Regex pattern for views to exclude from ingestion.                                                                                                                                      |
| `include_tables`       | ❌       | `True`   | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`        | ❌       | `True`   | Whether views should be ingested.                                                                                                                                                       |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
