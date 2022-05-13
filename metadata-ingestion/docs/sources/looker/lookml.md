#### Configuration Notes

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.
You need to ensure that the API key is attached to a user that has Admin privileges. If that is not possible, read the configuration section to provide an offline specification of the `connection_to_platform_map` and the `project_name`.

:::note
The integration can use an SQL parser to try to parse the tables the views depends on. 
:::
This parsing is disabled by default,
but can be enabled by setting `parse_table_names_from_sql: True`.  The default parser is based on the [`sqllineage`](https://pypi.org/project/sqllineage/) package.
As this package doesn't officially support all the SQL dialects that Looker supports, the result might not be correct. You can, however, implement a
custom parser and take it into use by setting the `sql_parser` configuration value. A custom SQL parser must inherit from `datahub.utilities.sql_parser.SQLParser`
and must be made available to Datahub by ,for example, installing it. The configuration then needs to be set to `module_name.ClassName` of the parser.
