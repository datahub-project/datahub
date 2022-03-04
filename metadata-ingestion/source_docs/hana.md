# SAP HANA

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[hana]'`.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, and tables
- Column types and schema associated with each table
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
  type: hana
  config:
    # Coordinates
    host_port: localhost:39041
    database: dbname

    # Credentials
    username: SYSTEM
    password: example

sink:
  # sink configs
```

## Config details

Like all SQL-based sources, the SAP HANA integration supports:
- Stale Metadata Deletion: See [here](./stateful_ingestion.md) for more details on configuration.
- SQL Profiling: See [here](./sql_profiles.md) for more details on configuration.

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                          | Required | Default            | Description                                                                                                                                                                             |
|--------------------------------|----------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `username`                     |          |                    | SAP HANA username.                                                                                                                                                                         |
| `password`                     |          |                    | SAP HANA password.                                                                                                                                                                         |
| `host_port`                    |          | `"localhost:3306"` | SAP HANA host URL.                                                                                                                                                                         |
| `database`                     |          |                    | SAP HANA database.                                                                                                                                                                         |
| `database_alias`               |          |                    | Alias to apply to database when ingesting.                                                                                                                                              |
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

## Notes
The implementation uses the [SQLAlchemy Dialect for SAP HANA](https://github.com/SAP/sqlalchemy-hana). The SQLAlchemy Dialect for SAP HANA is an open-source project hosted at GitHub that is actively maintained by SAP SE, and is not part of a licensed SAP HANA edition or option. It is provided under the terms of the project license.  Please notice that sqlalchemy-hana isn't an official SAP product and isn't covered by SAP support.

## Compatibility

Under the hood, [SQLAlchemy Dialect for SAP HANA](https://github.com/SAP/sqlalchemy-hana) uses the SAP HANA Python Driver hdbcli. Therefore it is compatible with HANA or HANA express versions since HANA SPS 2.

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
