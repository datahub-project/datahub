# Redshift

To install this plugin, run `pip install 'acryl-datahub[redshift]'`.

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table
- Also supports PostGIS extensions

```yml
source:
  type: redshift
  config:
    username: user
    password: pass
    host_port: example.something.us-west-2.redshift.amazonaws.com:5439
    database: DemoDatabase

    # Any options specified here will be passed to SQLAlchemy's create_engine as kwargs.
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.
    # Many of these options are specific to the underlying database driver, so that library's
    # documentation will be a good reference for what is supported. To find which dialect is likely
    # in use, consult this table: https://docs.sqlalchemy.org/en/14/dialects/index.html.
    options:
      # driver_option: some-option

    # Tables to allow/deny. If left blank, will ingest all.
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

    # If left blank, will ingest all.
    schema_pattern:
      deny:
        # ...
      allow:
        # ...

    # Same format as table_pattern, used for filtering views. If left blank, will ingest all.
    view_pattern:
      deny:
        # ...
      allow:
        # ...

    include_views: True # whether to include views, defaults to True
    include_tables: True # whether to include views, defaults to True
```

<details>
  <summary>Extra options when running Redshift behind a proxy</summary>

This requires you to have already installed the Microsoft ODBC Driver for SQL Server.
See https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15

```yml
source:
  type: redshift
  config:
    # username, password, database, etc...
    host_port: my-proxy-hostname:5439
    options:
      connect_args:
        sslmode: "prefer" # or "require" or "verify-ca"
        sslrootcert: ~ # needed to unpin the AWS Redshift certificate
```

</details>

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
