# Snowflake

To install this plugin, run `pip install 'acryl-datahub[snowflake]'`.

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table

```yml
source:
  type: snowflake
  config:
    username: user
    password: pass
    host_port: account_name

    warehouse: "COMPUTE_WH" # optional
    role: "sysadmin" # optional

    # Any options specified here will be passed to SQLAlchemy's create_engine as kwargs.
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.
    # Many of these options are specific to the underlying database driver, so that library's
    # documentation will be a good reference for what is supported. To find which dialect is likely
    # in use, consult this table: https://docs.sqlalchemy.org/en/14/dialects/index.html.
    options:
      # driver_option: some-option

    # Regexe filters for databases to allow/deny
    database_pattern:
      # The escaping of the $ symbol helps us skip the environment variable substitution.
      allow:
        - ^MY_DEMO_DATA.*
        - ^ANOTHER_DB_REGEX
      deny:
        - ^SNOWFLAKE\$
        - ^SNOWFLAKE_SAMPLE_DATA\$

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

    include_views: True # whether to include views, defaults to True
    include_tables: True # whether to include views, defaults to True
```

:::tip

You can also get fine-grained usage statistics for Snowflake using the `snowflake-usage` source described below.

:::

# Snowflake Usage Stats

To install this plugin, run `pip install 'acryl-datahub[snowflake-usage]'`.

This plugin extracts the following:

- Fetch a list of queries issued
- Fetch a list of tables and columns accessed (excludes views)
- Aggregate these statistics into buckets, by day or hour granularity

Note: the user/role must have access to the account usage table. The "accountadmin" role has this by default, and other roles can be [granted this permission](https://docs.snowflake.com/en/sql-reference/account-usage.html#enabling-account-usage-for-other-roles).

Note: the underlying access history views that we use are only available in Snowflake's enterprise edition or higher.

```yml
source:
  type: snowflake-usage
  config:
    username: user
    password: pass
    host_port: account_name
    role: ACCOUNTADMIN
    env: PROD

    bucket_duration: "DAY"
    start_time: ~ # defaults to the last full day in UTC (or hour)
    end_time: ~ # defaults to the last full day in UTC (or hour)

    top_n_queries: 10 # number of queries to save for each table
```

:::note

This source only does usage statistics. To get the tables, views, and schemas in your Snowflake warehouse, ingest using the `snowflake` source described above.

:::
