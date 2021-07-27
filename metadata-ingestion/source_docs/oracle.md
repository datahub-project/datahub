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

    # Any options specified here will be passed to SQLAlchemy's create_engine as kwargs.
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.
    # Many of these options are specific to the underlying database driver, so that library's
    # documentation will be a good reference for what is supported. To find which dialect is likely
    # in use, consult this table: https://docs.sqlalchemy.org/en/14/dialects/index.html.
    options:
      # driver_option: some-option

    # Tables to allow/deny
    table_pattern:
      deny:
        # Note that the deny patterns take precedence over the allow patterns.
        - "bad_table"
        - "junk_table"
        # Can also be a regular expression
        - "(old|used|deprecated)_table"
      allow:
        - "good_table"
        - "excellent_table"

      # Although the 'table_pattern' enables you to skip everything from certain schemas,
      # having another option to allow/deny on schema level is an optimization for the case when there is a large number
      # of schemas that one wants to skip and you want to avoid the time to needlessly fetch those tables only to filter
      # them out afterwards via the table_pattern.
    schema_pattern:
      deny:
        - "bad_schema"
        - "junk_table"
      allow:
        - "good_schema"
        - "excellent_schema"

    include_views: True # whether to include views, defaults to True
```
