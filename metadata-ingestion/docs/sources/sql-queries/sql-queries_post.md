### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Example Queries File

```json
{"query": "SELECT x FROM my_table", "timestamp": 1689232738.051, "user": "user_a", "downstream_tables": [], "upstream_tables": ["my_database.my_schema.my_table"]}
{"query": "INSERT INTO my_table VALUES (1, 'a')", "timestamp": 1689232737.669, "user": "user_b", "downstream_tables": ["my_database.my_schema.my_table"], "upstream_tables": []}
```

**Note:** This uses newline-delimited JSON format (NDJSON), where each line is a separate JSON object.

#### Query File Format

The source expects newline-delimited JSON (NDJSON), with one query object per line. Common fields:

- `query` (required): SQL text to parse
- `timestamp` (optional): query execution timestamp
- `user` (optional): actor used for usage attribution
- `session_id` (optional): session key used to resolve temporary tables across related queries
- `default_db` / `default_schema` (optional): per-record defaults for resolving unqualified table names. They override the source-level `default_db` / `default_schema`, so a single run can mix records from different databases. Blank values fall back to the source-level defaults.
- `downstream_tables` / `upstream_tables` (optional): explicit lineage hints; when both are provided, SQL parsing is bypassed and the lineage is used directly

#### Reading Queries from Elasticsearch / OpenSearch

Instead of a file, the source can read query records directly from an Elasticsearch or OpenSearch index via `query_source`. This is useful when executed SQL is shipped to a search cluster (for example, database audit logs forwarded to Elasticsearch) rather than kept in a file. Exactly one of `query_file` and `query_source` must be set.

```yaml
source:
  type: sql-queries
  config:
    platform: mariadb
    query_source:
      type: elasticsearch
      index: audit-*
      connection:
        host: https://es.internal:9200
        username: "${ES_USER}"
        password: "${ES_PASSWORD}"
        use_ssl: true
        verify_certs: true
      # Map Elasticsearch document fields to query-record fields. Dotted paths
      # are supported for nested documents.
      field_mappings:
        query: object
        timestamp: "@timestamp"
        user: username
        session_id: connectionid
        default_schema: database
      email_domain: example.com # bare usernames become <user>@example.com
      unescape_sql: true # undo backslash-escaped quotes stored by audit logs
      # Elasticsearch query DSL added to the read. This backend has no time
      # window of its own, so bound the range here.
      query_filter:
        range:
          "@timestamp":
            gte: "now-30d"
```

Notes:

- `connection` takes the same settings as the `elasticsearch` source, so username/password, API key, mutual TLS and AWS IAM (SigV4) authentication through `connection.aws_config` are all available. `connection.aws_config` is separate from the top-level `aws_config`, which is only used to read an `s3://` `query_file`.
- Every run reads every matching document. Use `query_filter` to limit the time range, for example to the period since the previous run.
- Documents whose mapped `query` field is empty or absent are skipped, so an index that mixes query records with non-query events (such as connect/disconnect audit entries) can be read directly without a filter. If no document has a value there, the run fails, because that almost always means `field_mappings.query` names the wrong field. A mapped optional field that no document has is reported as a warning. On an index where most documents are not queries, add a `query_filter` that selects the query events to cut the volume read.
- When a `timestamp` field is mapped, records are read in ascending timestamp order so session temporary-table lineage resolves correctly.
- Requests time out after `request_timeout_seconds` (60 by default) and are retried `max_retries` times after a timeout or connection error.

#### Reading Queries from a SQL Table

If query records are kept in a database table, such as a MySQL/MariaDB audit log or a query history table, the source can read them with `query_source.type: sql_table`. It runs one SELECT through SQLAlchemy and maps the result columns onto query-record fields.

```yaml
source:
  type: sql-queries
  config:
    platform: mysql
    query_source:
      type: sql_table
      connection:
        scheme: mysql+pymysql
        host_port: audit-db.internal:3306
        database: audit
        username: "${AUDIT_DB_USER}"
        password: "${AUDIT_DB_PASSWORD}"
      # One row per query record, oldest first. Bound the time range here.
      query: >-
        SELECT event_time, user_host, thread_id, argument
        FROM mysql.general_log
        WHERE command_type = 'Query'
          AND event_time >= NOW() - INTERVAL 1 DAY
        ORDER BY event_time
      field_mappings:
        query: argument
        timestamp: event_time
        user: user_host
        session_id: thread_id
      email_domain: example.com
```

Notes:

- The `query` is executed as written. Connect with a dedicated read-only account that can only read the audit table; the database grants are the authorization boundary, not this connector.
- `connection` accepts either the individual fields shown above or a full `sqlalchemy_uri`. Install the driver for your database alongside the `sql-queries` extra.
- Every run executes the same `query`, so put the time range in its `WHERE` clause. `ORDER BY` the timestamp column so session temporary-table lineage resolves correctly.
- Column names in `field_mappings` are matched exactly, then case-insensitively, since databases differ in how they case unquoted names. A mapped column the query doesn't return fails the run and lists the columns it did return.
- Binary columns (such as `BLOB` SQL text) are decoded as UTF-8. Timestamps without a time zone are read as UTC, so convert local-time columns to UTC in the `query`.

#### Reading Queries from Google SecOps

If database audit logs are already ingested into [Google SecOps](https://cloud.google.com/security/products/security-operations) (Chronicle), the source can read the statements from the parsed UDM events with `query_source.type: secops`. It calls the Chronicle API [`udmSearch`](https://docs.cloud.google.com/chronicle/docs/reference/rest/v1/projects.locations.instances/udmSearch) method.

```yaml
source:
  type: sql-queries
  config:
    platform: bigquery
    # The usage window also sets the time range of UDM events that are read.
    usage:
      start_time: "-7 days"
    query_source:
      type: secops
      project_id: my-secops-project
      location: us # selects https://us-chronicle.googleapis.com
      instance_id: 11111111-2222-3333-4444-555555555555
      credential: # omit to use Application Default Credentials
        project_id: my-secops-project
        private_key_id: "${SECOPS_PRIVATE_KEY_ID}"
        private_key: "${SECOPS_PRIVATE_KEY}"
        client_email: datahub-reader@my-secops-project.iam.gserviceaccount.com
        client_id: "123456789012345678901"
      # Selects the events that carry SQL. Every matching event is read.
      udm_query: 'metadata.log_type = "MY_DB_AUDIT"'
      # Dotted paths into each event's `udm`, in the camelCase the API returns.
      field_mappings:
        query: target.process.commandLine
        timestamp: metadata.eventTimestamp
        user: principal.user.userid
        default_db: target.resource.name
      email_domain: example.com
```

Notes:

- The service account needs the `chronicle.events.udmSearch` permission on the SecOps instance, for example through the Chronicle API Viewer role (`roles/chronicle.viewer`).
- Events are read between `usage.start_time` and `usage.end_time`, which default to the last full day. Relative values such as `-7 days` are accepted.
- Which UDM field holds the SQL depends on the log type's parser, so `field_mappings.query` has no default. Open a sample event in UDM Search to find it. A path through a repeated field, such as `principal.user.emailAddresses`, takes its first element.
- `udmSearch` returns at most 10,000 events per call and has no paging. The source reads the time range in `window_minutes` windows (60 by default) and splits any window that hits the cap in half, down to `min_window_seconds`. If a window that short still hits the cap, the extra events are skipped, a warning is reported, and the report counts it under `secops_windows_truncated`. Narrow `udm_query` if that happens.
- Windows are read oldest first, and events in a window are sorted by the mapped `timestamp`, so session temporary-table lineage resolves correctly. Events without a usable timestamp are counted and reported.
- Rate-limit (429) and server (5xx) responses are retried up to `max_retries` times with backoff. Any other error response fails the run, and the report includes the API's error message.

#### Incremental Lineage

When stateful ingestion is enabled, lineage extraction can progress incrementally over new query records while preserving historical checkpoints.

#### Temporary Table Support

`session_id` enables correlation of temporary-table creation and downstream usage across query sequences, improving lineage continuity.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
