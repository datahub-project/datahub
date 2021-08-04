# LookML

## Setup

To install this plugin, run `pip install 'acryl-datahub[lookml]'`.

Note! This plugin uses a package that requires Python 3.7+!

## Capabilities

This plugin extracts the following:

- LookML views from model files
- Name, upstream table names, dimensions, measures, and dimension groups

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: "lookml"
  config:
    base_folder: /path/to/model/files # where the *.model.lkml and *.view.lkml files are stored
    connection_to_platform_map: # mappings between connection names in the model files to platform names
      connection_name: platform_name (or platform_name.database_name) # for ex. my_snowflake_conn: snowflake.my_database

    platform_name: "looker" # optional, default is "looker"

    # Regex pattern to allow/deny models. If left blank, will ingest all.
    model_pattern:
      deny:
        # ...
      allow:
        # ...

    # Regex pattern to allow/deny views. If left blank, will ingest all.
    view_pattern:
      deny:
        # ...
      allow:
        # ...

    env: "PROD" # optional, default is "PROD"
    parse_table_names_from_sql: False # see note below
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                          | Required | Default    | Description                                                             |
| ---------------------------------------------- | -------- | ---------- | ----------------------------------------------------------------------- |
| `base_folder`                                  | ✅       |            | Where the `*.model.lkml` and `*.view.lkml` files are stored.            |
| `connection_to_platform_map.<connection_name>` | ✅       |            | Mappings between connection names in the model files to platform names. |
| `platform_name`                                |          | `"looker"` | Platform to use in namespace when constructing URNs.                    |
| `model_pattern.allow`                          |          |            | Regex pattern for models to include in ingestion.                       |
| `model_pattern.deny`                           |          |            | Regex pattern for models to exclude from ingestion.                     |
| `view_pattern.allow`                           |          |            | Regex pattern for views to include in ingestion.                        |
| `view_pattern.deny`                            |          |            | Regex pattern for views to exclude from ingestion.                      |
| `env`                                          |          | `"PROD"`   | Environment to use in namespace when constructing URNs.                 |
| `parse_table_names_from_sql`                   |          | `False`    | See note below.                                                         |

Note! The integration can use [`sql-metadata`](https://pypi.org/project/sql-metadata/) to try to parse the tables the
views depends on. As these SQL's can be complicated, and the package doesn't official support all the SQL dialects that
Looker supports, the result might not be correct. This parsing is disabled by default, but can be enabled by setting
`parse_table_names_from_sql: True`.

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
