# Hive

To install this plugin, run `pip install 'acryl-datahub[hive]'`.

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table
- Detailed table and storage information

```yml
source:
  type: hive
  config:
    # For more details on authentication, see the PyHive docs:
    # https://github.com/dropbox/PyHive#passing-session-configuration.
    # LDAP, Kerberos, etc. are supported using connect_args, which can be
    # added under the `options` config parameter.
    #scheme: 'hive+http' # set this if Thrift should use the HTTP transport
    #scheme: 'hive+https' # set this if Thrift should use the HTTP with SSL transport
    username: user # optional
    password: pass # optional
    host_port: localhost:10000
    database: DemoDatabase # optional, defaults to 'default'

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
        # ...
      allow:
        # ...

    # Same format as table_pattern, used for filtering views
    view_pattern:
      deny:
        # ...
      allow:
        # ...

    include_tables: True # whether to include views, defaults to True
```

<details>
  <summary>Example: using ingestion with Azure HDInsight</summary>

```yml
# Connecting to Microsoft Azure HDInsight using TLS.
source:
  type: hive
  config:
    scheme: "hive+https"
    host_port: <cluster_name>.azurehdinsight.net:443
    username: admin
    password: "<password>"
    options:
      connect_args:
        http_path: "/hive2"
        auth: BASIC
    # table_pattern/schema_pattern is same as above
```

</details>
