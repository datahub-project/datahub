# LookML

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[lookml]'`.

Note! This plugin uses a package that requires Python 3.7+!

## Capabilities

This plugin extracts the following:

- LookML views from model files
- Name, upstream table names, dimensions, measures, and dimension groups

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: "lookml"
  config:
    # Coordinates
    base_folder: /path/to/model/files

    # Options
    connection_to_platform_map:
      connection_name: platform_name (or platform_name.database_name) # for ex. my_snowflake_conn: snowflake.my_database

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                          | Required | Default    | Description                                                             |
| ---------------------------------------------- | -------- | ---------- | ----------------------------------------------------------------------- |
| `base_folder`                                  | ✅       |            | Where the `*.model.lkml` and `*.view.lkml` files are stored.            |
| `connection_to_platform_map.<connection_name>` | ✅       |            | Mappings between connection names in the model files to platform names. |
| `platform_name`                                |          | `"looker"` | Platform to use in namespace when constructing URNs.                    |
| `model_pattern.allow`                          |          |            | List of regex patterns for models to include in ingestion.                       |
| `model_pattern.deny`                           |          |            | List of regex patterns for models to exclude from ingestion.                     |
| `model_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |
| `view_pattern.allow`                           |          |            | List of regex patterns for views to include in ingestion.                        |
| `view_pattern.deny`                            |          |            | List of regex patterns for views to exclude from ingestion.                      |
| `view_pattern.ignoreCase`  |          | `True` | Whether to ignore case sensitivity during pattern matching.                                                                                                                                  |
| `env`                                          |          | `"PROD"`   | Environment to use in namespace when constructing URNs.                 |
| `parse_table_names_from_sql`                   |          | `False`    | See note below.                                                         |
| `sql_parser`                                   |          | `datahub.utilities.sql_parser.DefaultSQLParser`    | See note below.                                                         |

Note! The integration can use an SQL parser to try to parse the tables the views depends on. This parsing is disabled by default, 
but can be enabled by setting `parse_table_names_from_sql: True`.  The default parser is based on the [`sql-metadata`](https://pypi.org/project/sql-metadata/) package. 
As this package doesn't officially support all the SQL dialects that Looker supports, the result might not be correct. You can, however, implement a
custom parser and take it into use by setting the `sql_parser` configuration value. A custom SQL parser must inherit from `datahub.utilities.sql_parser.SQLParser`
and must be made available to Datahub by ,for example, installing it. The configuration then needs to be set to `module_name.ClassName` of the parser.

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
