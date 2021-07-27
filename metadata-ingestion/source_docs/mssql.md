# Microsoft SQL Server Metadata

To install this plugin, run `pip install 'acryl-datahub[mssql]'`.

We have two options for the underlying library used to connect to SQL Server: (1) [python-tds](https://github.com/denisenkom/pytds) and (2) [pyodbc](https://github.com/mkleehammer/pyodbc). The TDS library is pure Python and hence easier to install, but only PyODBC supports encrypted connections.

This plugin extracts the following:

- List of databases, schema, tables and views
- Column types associated with each table/view

```yml
source:
  type: mssql
  config:
    username: user
    password: pass
    host_port: localhost:1433
    database: DemoDatabase

    # Any options specified here will be passed to SQLAlchemy's create_engine as kwargs.
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.
    # Many of these options are specific to the underlying database driver, so that library's
    # documentation will be a good reference for what is supported. To find which dialect is likely
    # in use, consult this table: https://docs.sqlalchemy.org/en/14/dialects/index.html.
    options:
      charset: "utf8"

    # Tables to allow/deny
    table_pattern:
      deny:
        # Note that the deny patterns take precedence over the allow patterns.
        - "^.*\\.sys_.*" # deny all tables that start with sys_
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
        # ...
      allow:
        # ...

    # Same format as table_pattern, used for filtering views
    view_pattern:
      deny:
        # ...
      allow:
        # ...

    include_views: True # whether to include views, defaults to True
    include_tables: True # whether to include views, defaults to True

    # If set to true, we'll use the pyodbc library. This requires you to have
    # already installed the Microsoft ODBC Driver for SQL Server.
    # See https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15
    use_odbc: False
    uri_args: {}
```

<details>
  <summary>Example: using ingestion with ODBC and encryption</summary>

This requires you to have already installed the Microsoft ODBC Driver for SQL Server.
See https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15

```yml
source:
  type: mssql
  config:
    # See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc
    use_odbc: True
    username: user
    password: pass
    host_port: localhost:1433
    database: DemoDatabase
    include_views: True # whether to include views, defaults to True
    uri_args:
      # See https://docs.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver15
      driver: "ODBC Driver 17 for SQL Server"
      Encrypt: "yes"
      TrustServerCertificate: "Yes"
      ssl: "True"
      # Trusted_Connection: "yes"
```

</details>
