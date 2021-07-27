# Redshift `redshift`

Extracts:

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
    include_views: True # whether to include views, defaults to True
    # table_pattern/schema_pattern is same as above
    # options is same as above
```

<details>
  <summary>Extra options when running Redshift behind a proxy</summary>

This requires you to have already installed the Microsoft ODBC Driver for SQL Server.
See https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver15

```yml
source:
  type: redshift
  config:
    # username, password, database, etc are all the same as above
    host_port: my-proxy-hostname:5439
    options:
      connect_args:
        sslmode: "prefer" # or "require" or "verify-ca"
        sslrootcert: ~ # needed to unpin the AWS Redshift certificate
```

</details>

