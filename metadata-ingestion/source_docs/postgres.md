# PostgreSQL

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[postgres]'`.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, views, and tables
- Column types associated with each table
- Also supports PostGIS extensions
- database_alias (optional) can be used to change the name of database to be ingested

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: postgres
  config:
    # Coordinates
    host_port: localhost:5432
    database: DemoDatabase

    # Credentials
    username: user
    password: pass

    # Options
    database_alias: DatabaseNameToBeIngested

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                  | Required | Default  | Description                                                                                                                                                                             |
| ---------------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `username`             |          |          | PostgreSQL username.                                                                                                                                                                    |
| `password`             |          |          | PostgreSQL password.                                                                                                                                                                    |
| `host_port`            | ✅       |          | PostgreSQL host URL.                                                                                                                                                                    |
| `database`             |          |          | PostgreSQL database.                                                                                                                                                                    |
| `database_alias`       |          |          | Alias to apply to database when ingesting.                                                                                                                                              |
| `env`                  |          | `"PROD"` | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `options.<option>`     |          |          | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `table_pattern.allow`  |          |          | Regex pattern for tables to include in ingestion.                                                                                                                                       |
| `table_pattern.deny`   |          |          | Regex pattern for tables to exclude from ingestion.                                                                                                                                     |
| `schema_pattern.allow` |          |          | Regex pattern for schemas to include in ingestion.                                                                                                                                      |
| `schema_pattern.deny`  |          |          | Regex pattern for schemas to exclude from ingestion.                                                                                                                                    |
| `view_pattern.allow`   |          |          | Regex pattern for views to include in ingestion.                                                                                                                                        |
| `view_pattern.deny`    |          |          | Regex pattern for views to exclude from ingestion.                                                                                                                                      |
| `include_tables`       |          | `True`   | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`        |          | `True`   | Whether views should be ingested.                                                                                                                                                       |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
