# Google BigQuery

To install this plugin, run `pip install 'acryl-datahub[bigquery]'`.

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table

```yml
source:
  type: bigquery
  config:
    project_id: project # optional - can autodetect from environment

    # Any options specified here will be passed to SQLAlchemy's create_engine as kwargs.
    # See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.
    # Many of these options are specific to the underlying database driver, so that library's
    # documentation will be a good reference for what is supported. To find which dialect is likely
    # in use, consult this table: https://docs.sqlalchemy.org/en/14/dialects/index.html.
    options:
      # See https://github.com/mxmzdlv/pybigquery#authentication for details.
      credentials_path: "/path/to/keyfile.json" # optional

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

You can also get fine-grained usage statistics for BigQuery using the `bigquery-usage` source described below.

:::

# Google BigQuery Usage Stats

To install this plugin, run `pip install 'acryl-datahub[bigquery-usage]'`.

- Fetch a list of queries issued
- Fetch a list of tables and columns accessed
- Aggregate these statistics into buckets, by day or hour granularity

Note: the client must have one of the following OAuth scopes, and should be authorized on all projects you'd like to ingest usage stats from.

- https://www.googleapis.com/auth/logging.read
- https://www.googleapis.com/auth/logging.admin
- https://www.googleapis.com/auth/cloud-platform.read-only
- https://www.googleapis.com/auth/cloud-platform

```yml
source:
  type: bigquery-usage
  config:
    projects: # optional - can autodetect a single project from the environment
      - project_id_1
      - project_id_2
    options:
      # See https://googleapis.dev/python/logging/latest/client.html for details.
      credentials: ~ # optional - see docs

    # Common usage stats options
    bucket_duration: "DAY"
    start_time: ~ # defaults to the last full day in UTC (or hour)
    end_time: ~ # defaults to the last full day in UTC (or hour)

    top_n_queries: 10 # number of queries to save for each table

    env: PROD

    # Additional options to pass to google.cloud.logging_v2.client.Client
    extra_client_options:

    # To account for the possibility that the query event arrives after
    # the read event in the audit logs, we wait for at least `query_log_delay`
    # additional events to be processed before attempting to resolve BigQuery
    # job information from the logs. If `query_log_delay` is None, it gets treated
    # as an unlimited delay, which prioritizes correctness at the expense of memory usage.
    query_log_delay:

    # Correction to pad start_time and end_time with.
    # For handling the case where the read happens within our time range but the query
    # completion event is delayed and happens after the configured end time.
    max_query_duration:
```

:::note

This source only does usage statistics. To get the tables, views, and schemas in your BigQuery project, use the `bigquery` source.

:::
