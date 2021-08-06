# Other SQLAlchemy databases

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[sqlalchemy]'`.

The `sqlalchemy` source is useful if we don't have a pre-built source for your chosen
database system, but there is an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/)
defined elsewhere. In order to use this, you must `pip install` the required dialect packages yourself.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, views, and tables
- Column types associated with each table

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: sqlalchemy
  config:
    # Coordinates
    connect_uri: "dialect+driver://username:password@host:port/database"

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                  | Required | Default  | Description                                                                                                                                                                             |
| ---------------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `platform`             | ✅       |          | Name of platform being ingested, used in constructing URNs.                                                                                                                             |
| `connect_uri`          | ✅       |          | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls                                                                                    |
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
