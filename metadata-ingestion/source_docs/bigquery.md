# Google BigQuery `bigquery`

Extracts:

- List of databases, schema, and tables
- Column types associated with each table

```yml
source:
  type: bigquery
  config:
    project_id: project # optional - can autodetect from environment
    options: # options is same as above
      # See https://github.com/mxmzdlv/pybigquery#authentication for details.
      credentials_path: "/path/to/keyfile.json" # optional
    include_views: True # whether to include views, defaults to True
    # table_pattern/schema_pattern is same as above
```

:::tip

You can also get fine-grained usage statistics for BigQuery using the `bigquery-usage` source.

:::


# Google BigQuery Usage Stats `bigquery-usage`

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
    env: PROD

    bucket_duration: "DAY"
    start_time: ~ # defaults to the last full day in UTC (or hour)
    end_time: ~ # defaults to the last full day in UTC (or hour)

    top_n_queries: 10 # number of queries to save for each table
```

:::note

This source only does usage statistics. To get the tables, views, and schemas in your BigQuery project, use the `bigquery` source.

:::
