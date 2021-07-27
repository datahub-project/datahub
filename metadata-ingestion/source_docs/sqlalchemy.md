# Other databases using SQLAlchemy

To install this plugin, run `pip install 'acryl-datahub[sqlalchemy]'`.

The `sqlalchemy` source is useful if we don't have a pre-built source for your chosen
database system, but there is an [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/)
defined elsewhere. In order to use this, you must `pip install` the required dialect packages yourself.

Extracts:

- List of schemas and tables
- Column types associated with each table

```yml
source:
  type: sqlalchemy
  config:
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
    connect_uri: "dialect+driver://username:password@host:port/database"
    options: {} # same as above
    schema_pattern: {} # same as above
    table_pattern: {} # same as above
    include_views: True # whether to include views, defaults to True
```
