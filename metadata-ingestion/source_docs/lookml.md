# LookML

To install this plugin, run `pip install 'acryl-datahub[lookml]'`.

Note! This plugin uses a package that requires Python 3.7+!

This plugin extracts the following:

- LookML views from model files
- Name, upstream table names, dimensions, measures, and dimension groups

```yml
source:
  type: "lookml"
  config:
    base_folder: /path/to/model/files # where the *.model.lkml and *.view.lkml files are stored
    connection_to_platform_map: # mappings between connection names in the model files to platform names
      connection_name: platform_name (or platform_name.database_name) # for ex. my_snowflake_conn: snowflake.my_database
    model_pattern: {}
    view_pattern: {}
    env: "PROD" # optional, default is "PROD"
    parse_table_names_from_sql: False # see note below
    platform_name: "looker" # optional, default is "looker"
```

Note! The integration can use [`sql-metadata`](https://pypi.org/project/sql-metadata/) to try to parse the tables the
views depends on. As these SQL's can be complicated, and the package doesn't official support all the SQL dialects that
Looker supports, the result might not be correct. This parsing is disabled by default, but can be enabled by setting
`parse_table_names_from_sql: True`.
