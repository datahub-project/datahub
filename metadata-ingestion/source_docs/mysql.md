# MySQL

To install this plugin, run `pip install 'acryl-datahub[mysql]'`.

This plugin extracts the following:

- List of databases and tables
- Column types and schema associated with each table

```yml
source:
  type: mysql
  config:
    username: root
    password: example
    database: dbname
    host_port: localhost:3306
    table_pattern:
      deny:
        # Note that the deny patterns take precedence over the allow patterns.
        - "performance_schema"
      allow:
        - "schema1.table2"
      # Although the 'table_pattern' enables you to skip everything from certain schemas,
      # having another option to allow/deny on schema level is an optimization for the case when there is a large number
      # of schemas that one wants to skip and you want to avoid the time to needlessly fetch those tables only to filter
      # them out afterwards via the table_pattern.
    schema_pattern:
      deny:
        - "garbage_schema"
      allow:
        - "schema1"
```
