# Snowflake `snowflake`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table

```yml
source:
  type: snowflake
  config:
    username: user
    password: pass
    host_port: account_name
    database_pattern:
      # The escaping of the $ symbol helps us skip the environment variable substitution.
      allow:
        - ^MY_DEMO_DATA.*
        - ^ANOTHER_DB_REGEX
      deny:
        - ^SNOWFLAKE\$
        - ^SNOWFLAKE_SAMPLE_DATA\$
    warehouse: "COMPUTE_WH" # optional
    role: "sysadmin" # optional
    include_views: True # whether to include views, defaults to True
    # table_pattern/schema_pattern is same as above
    # options is same as above
```

:::tip

You can also get fine-grained usage statistics for Snowflake using the `snowflake-usage` source.

:::


# Snowflake Usage Stats `snowflake-usage`

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

This source only does usage statistics. To get the tables, views, and schemas in your Snowflake warehouse, ingest using the `snowflake` source.

:::
