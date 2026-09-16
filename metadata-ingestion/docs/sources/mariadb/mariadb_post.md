### Capabilities

- Ingests MariaDB tables and views with schema and column metadata.
- Supports optional profiling and SQL-based lineage extraction when enabled.
- Supports secure connections through SSL client configuration.
- Optionally derives usage statistics and query-based lineage from `performance_schema`.

#### Usage and Lineage

When `include_usage_statistics` is enabled, queries are read from query history (filtered to the
configured `usage` time window) and parsed to produce dataset usage statistics, top queries, and
query-based table/column lineage. The `usage_source` config selects the history source:

- **`performance_schema`** (default): reads normalized digests from
  `events_statements_summary_by_digest`, filtered by each digest's `LAST_SEEN`. No setup or
  overhead, but no per-user attribution and query text is normalized.
- **`general_log`**: reads literal statements from `mysql.general_log`, filtered by `event_time`.
  Provides per-user attribution and exact query text at the cost of general-log overhead.

### Limitations

- Lineage completeness depends on available query logs and SQL parsing coverage.
- Profiling on very large schemas or wide tables can increase extraction time.
- SSL setup depends on valid certificate paths and MariaDB server TLS configuration.

#### Usage Statistics Caveats (`performance_schema` mode)

These caveats apply to the default `usage_source: performance_schema`. The `general_log` mode is not
affected — it provides per-user attribution, exact query text, and per-execution timestamps.

- **No per-user attribution.** `events_statements_summary_by_digest` aggregates executions across
  all users, so usage statistics report query counts but cannot break them down by user.
- **Counts are cumulative.** Each digest's `COUNT_STAR` accumulates since the table was last reset
  (server restart or `TRUNCATE performance_schema.events_statements_summary_by_digest`), not per
  time bucket. All executions are attributed to the bucket of the digest's `LAST_SEEN`.
- **Normalized query text only.** Digest text has literals replaced with `?` and is truncated to
  `performance_schema_max_digest_length`, so top queries are shown as templates rather than literal
  statements.
- **Some lineage is lost.** Digest text collapses variadic argument lists to `...` (e.g.
  `VALUES (...)`, `SUBSTRING_INDEX(col, ?, ...)`), which the SQL parser cannot read, so lineage from
  those statements is skipped. Usage counts are unaffected. Use `general_log` if you need lineage
  from such statements.

### Troubleshooting

- **Permission errors**: verify the ingestion user has required metadata grants on all included databases and schemas.
- **Connection failures**: validate `host_port`, database name, and whether the runtime can reach the MariaDB instance.
- **SSL handshake issues**: confirm `ssl_ca`, `ssl_cert`, and `ssl_key` paths and certificate compatibility with the server.
