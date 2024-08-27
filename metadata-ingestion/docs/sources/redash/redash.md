Note! The integration can use an SQL parser to try to parse the tables the chart depends on. This parsing is disabled by default, 
but can be enabled by setting `parse_table_names_from_sql: true`.  The default parser is based on the [`sqllineage`](https://pypi.org/project/sqllineage/) package.
As this package doesn't officially support all the SQL dialects that Redash supports, the result might not be correct. You can, however, implement a
custom parser and take it into use by setting the `sql_parser` configuration value. A custom SQL parser must inherit from `datahub.utilities.sql_parser.SQLParser`
and must be made available to Datahub by ,for example, installing it. The configuration then needs to be set to `module_name.ClassName` of the parser.
