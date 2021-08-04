# Other SQLAlchemy databases

## Setup

To install this plugin, run `pip install 'acryl-datahub[sqlalchemy]'`.

The `sqlalchemy` source is useful if we don't have a pre-built source for your chosen
database system, but there is an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/)
defined elsewhere. In order to use this, you must `pip install` the required dialect packages yourself.

## Capabilities

This plugin extracts the following:

- List of schemas and tables
- Column types associated with each table

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: sqlalchemy
  config:
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
    connect_uri: "dialect+driver://username:password@host:port/database"

    # Any options specified here will be passed to SQLAlchemy's create_engine as kwargs.
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.
    # Many of these options are specific to the underlying database driver, so that library's
    # documentation will be a good reference for what is supported. To find which dialect is likely
    # in use, consult this table: https://docs.sqlalchemy.org/en/14/dialects/index.html.
    options:
      # driver_option: some-option

    include_views: True # whether to include views, defaults to True
    include_tables: True # whether to include views, defaults to True
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

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
