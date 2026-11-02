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
- `default_db` / `default_schema` (optional): per-record defaults for resolving unqualified table names. They override the source-level `default_db` / `default_schema`, so a single run can mix records from different databases.
- `downstream_tables` / `upstream_tables` (optional): explicit lineage hints; when both are provided, SQL parsing is bypassed and the lineage is used directly

#### Reading Queries from Elasticsearch / OpenSearch

Instead of a file, the source can read query records directly from an Elasticsearch or OpenSearch index via `query_source`. This is useful when executed SQL is shipped to a search cluster (for example, database audit logs forwarded to Elasticsearch) rather than kept in a file. `query_file` and `query_source` are mutually exclusive.

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
      # Optional Elasticsearch query DSL merged into the read, e.g. to scope a time range.
      query_filter:
        range:
          "@timestamp":
            gte: "now-30d"
```

Notes:

- Only the `connection` fields of a standard Elasticsearch source are reused, so every authentication method that source supports (username/password, API key, mutual TLS) is available here.
- Documents whose mapped `query` field is empty or absent are skipped, so an index that mixes query records with non-query events (such as connect/disconnect audit entries) can be read directly without a filter.
- When a `timestamp` field is mapped, records are read in ascending timestamp order so session temporary-table lineage resolves correctly.
- Reading from Elasticsearch needs `opensearch-py`, which is not installed by the base `sql-queries` extra. Install it with the `elasticsearch` extra: `pip install 'acryl-datahub[sql-queries,elasticsearch]'`.

#### Incremental Lineage

When stateful ingestion is enabled, lineage extraction can progress incrementally over new query records while preserving historical checkpoints.

#### Temporary Table Support

`session_id` enables correlation of temporary-table creation and downstream usage across query sequences, improving lineage continuity.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
