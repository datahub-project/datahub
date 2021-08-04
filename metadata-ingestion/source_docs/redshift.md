# Redshift

## Setup

To install this plugin, run `pip install 'acryl-datahub[redshift]'`.

## Capabilities

This plugin extracts the following:

- Databases, schema, and tables and associated metadata
- Column types associated with each table
- Also supports PostGIS extensions

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

```yml
source:
  type: redshift
  config:
    username: user
    password: pass
    host_port: example.something.us-west-2.redshift.amazonaws.com:5439
    database: DemoDatabase

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

<details>
  <summary>Extra options when running Redshift behind a proxy</summary>

This requires you to have already installed the Microsoft ODBC Driver for SQL Server.
See https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15

```yml
source:
  type: redshift
  config:
    host_port: my-proxy-hostname:5439

    options:
      connect_args:
        sslmode: "prefer" # or "require" or "verify-ca"
        sslrootcert: ~ # needed to unpin the AWS Redshift certificate
```

</details>

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                  | Required | Default  | Description                                                                                                                                                                             |
| ---------------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `username`             |          |          | Redshift username.                                                                                                                                                                      |
| `password`             |          |          | Redshift password.                                                                                                                                                                      |
| `host_port`            | ✅       |          | Redshift host URL.                                                                                                                                                                      |
| `database`             |          |          | Redshift database.                                                                                                                                                                      |
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

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
