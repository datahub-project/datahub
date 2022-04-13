# Trino

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[trino]'`.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, and tables
- Column types and schema associated with each table
- Table, row, and column statistics via optional [SQL profiling](./sql_profiles.md)

| Capability        | Status | Details                                  | 
|-------------------|--------|------------------------------------------|
| Data Containers   | ✔️     |                                          |
| Data Domains      | ✔️     | [link](../../docs/domains.md)            |

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: trino
  config:
    # Coordinates
    host_port: localhost:5300
    database: dbname

    # Credentials
    username: foo
    password: datahub

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

As a SQL-based service, the Trino integration is also supported by our SQL profiler. See [here](./sql_profiles.md) for more details on configuration.

| Field                          | Required | Default  | Description                                                                                                                                                                             |
|--------------------------------|----------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `username`                     | ✅        |          | Trino username.                                                                                                                                                                         |
| `password`                     |          |          | Trino password.                                                                                                                                                                         |
| `host_port`                    | ✅        |          | Trino host URL.                                                                                                                                                                         |
| `database`                     | ✅        |          | Trino database (catalog).                                                                                                                                                               |
| `sqlalchemy_uri`               |          |          | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.
| `database_alias`               |          |          | Alias to apply to database when ingesting.                                                                                                                                              |
| `env`                          |          | `"PROD"` | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `options.<option>`             |          |          | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `table_pattern.allow`          |          |          | List of regex patterns for tables to include in ingestion.                                                                                                                              |
| `table_pattern.deny`           |          |          | List of regex patterns for tables to exclude from ingestion.                                                                                                                            |
| `table_pattern.ignoreCase`     |          | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `schema_pattern.allow`         |          |          | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.deny`          |          |          | List of regex patterns for schemas to exclude from ingestion.                                                                                                                           |
| `schema_pattern.ignoreCase`    |          | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `view_pattern.allow`           |          |          | List of regex patterns for views to include in ingestion.                                                                                                                               |
| `view_pattern.deny`            |          |          | List of regex patterns for views to exclude from ingestion.                                                                                                                             |
| `view_pattern.ignoreCase`      |          | `True`   | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `include_tables`               |          | `True`   | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`                |          | `True`   | Whether views should be ingested.                                                                                                                                                       |
| `domain.domain_key.allow`      |          |          | List of regex patterns for tables/schemas to set domain_key domain key (domain_key can be any string like `sales`. There can be multiple domain key specified.                          |
| `domain.domain_key.deny`       |          |          | List of regex patterns for tables/schemas to not assign domain_key. There can be multiple domain key specified.                                                                         |
| `domain.domain_key.ignoreCase` |          | `True`   | Whether to ignore case sensitivity during pattern matching.There can be multiple domain key specified.                                                                                  |

## Compatibility

Coming soon!

## Trino Usage Stats

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

### Starburst Trino Usage Stats
If you are using Starburst Trino you can collect usage stats the following way.
#### Prerequsities 
1. You need to setup Event Logger which saves audit logs into a Postgres db and setup this db as a catalog in Trino
Here you can find more info about how to setup:
https://docs.starburst.io/354-e/security/event-logger.html#security-event-logger--page-root
https://docs.starburst.io/354-e/security/event-logger.html#analyzing-the-event-log

2. Install starbust-trino-usage plugin 
Run pip install 'acryl-datahub[starburst-trino-usage]'.

#### Usage stats ingestion job
Here is a sample recipe to ingest usage data:
```
source:
    type: starburst-trino-usage
    config:
    # Coordinates
    host_port: yourtrinohost:port
    # The name of the catalog from getting the usage 
    database: hive
    # Credentials
    username: trino_username
    password: trino_password
    email_domain: test.com
    audit_catalog: audit
    audit_schema: audit_schema

sink:
    type: "datahub-rest"
    config:
        server: "http://localhost:8080"
```
### Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

By default, we extract usage stats for the last day, with the recommendation that this source is executed every day.

| Field                           | Required | Default                                                        | Description                                                     |
|---------------------------------|----------|----------------------------------------------------------------|-----------------------------------------------------------------|
| `database`                      | yes      |                                                                | The name of the catalog from getting the usage                  |
| `audit_catalog`                 | yes      |                                                                | The catalog name where the audit table can be found             |
| `audit_schema`                  | yes      |                                                                | The schema name where the audit table can be found              |
| `email_domain`                  | yes      |                                                                | The email domain which will be appended to the users            |
| `env`                           |          | `"PROD"`                                                       | Environment to use in namespace when constructing URNs.         |
| `bucket_duration`               |          | `"DAY"`                                                        | Duration to bucket usage events by. Can be `"DAY"` or `"HOUR"`. |
| `start_time`                    |          | Last full day in UTC (or hour, depending on `bucket_duration`) | Earliest date of usage logs to consider.                        |
| `end_time`                      |          | Last full day in UTC (or hour, depending on `bucket_duration`) | Latest date of usage logs to consider.                          |
| `top_n_queries`                 |          | `10`                                                           | Number of top queries to save to each table.                    |
| `user_email_pattern.allow`      |          | *                                                              | List of regex patterns for user emails to include in usage.     |
| `user_email_pattern.deny`       |          |                                                                | List of regex patterns for user emails to exclude from usage.   |
| `user_email_pattern.ignoreCase` |          | `True`                                                         | Whether to ignore case sensitivity during pattern matching.     |
| `format_sql_queries`            |          | `False`                                                        | Whether to format sql queries                                   |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
