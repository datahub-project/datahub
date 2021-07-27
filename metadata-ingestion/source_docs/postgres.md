# PostgreSQL

To install this plugin, run `pip install 'acryl-datahub[postgres]'`.

Extracts:

- List of databases, schema, and tables
- Column types associated with each table
- Also supports PostGIS extensions
- database_alias (optional) can be used to change the name of database to be ingested

```yml
source:
  type: postgres
  config:
    username: user
    password: pass
    host_port: localhost:5432
    database: DemoDatabase
    database_alias: DatabaseNameToBeIngested
    include_views: True # whether to include views, defaults to True
    # table_pattern/schema_pattern is same as above
    # options is same as above
```
