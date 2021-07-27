# Oracle

To install this plugin, run `pip install 'acryl-datahub[oracle]'`.

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table

Using the Oracle source requires that you've also installed the correct drivers; see the [cx_Oracle docs](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html). The easiest one is the [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html).

```yml
source:
  type: oracle
  config:
    # For more details on authentication, see the documentation:
    # https://docs.sqlalchemy.org/en/14/dialects/oracle.html#dialect-oracle-cx_oracle-connect and
    # https://cx-oracle.readthedocs.io/en/latest/user_guide/connection_handling.html#connection-strings.
    username: user
    password: pass
    host_port: localhost:5432
    database: dbname
    service_name: svc # omit database if using this option
    include_views: True # whether to include views, defaults to True
    # table_pattern/schema_pattern is same as above
    # options is same as above
```
