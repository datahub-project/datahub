### Capabilities

- Ingests MariaDB tables and views with schema and column metadata.
- Supports optional profiling and SQL-based lineage extraction when enabled.
- Supports secure connections through SSL client configuration.
- Optionally derives usage statistics and query-based lineage from `performance_schema`.

#### Usage and Lineage from `performance_schema`

When `include_usage_statistics` is enabled, queries are read from
`performance_schema.events_statements_summary_by_digest` (filtered to the configured `usage` time
window using each digest's `LAST_SEEN`) and parsed to produce dataset usage statistics, top
queries, and query-based table/column lineage. This relies only on the always-on statements digest
and does not require the general query log.

### Limitations

- Lineage completeness depends on available query logs and SQL parsing coverage.
- Profiling on very large schemas or wide tables can increase extraction time.
- SSL setup depends on valid certificate paths and MariaDB server TLS configuration.

#### Usage Statistics Caveats

- **No per-user attribution.** `events_statements_summary_by_digest` aggregates executions across
  all users, so usage statistics report query counts but cannot break them down by user.
- **Counts are cumulative.** Each digest's `COUNT_STAR` accumulates since the table was last reset
  (server restart or `TRUNCATE performance_schema.events_statements_summary_by_digest`), not per
  time bucket. All executions are attributed to the bucket of the digest's `LAST_SEEN`.
- **Normalized query text only.** Digest text has literals replaced with `?` and is truncated to
  `performance_schema_max_digest_length`, so top queries are shown as templates rather than literal
  statements.

### Troubleshooting

- **Permission errors**: verify the ingestion user has required metadata grants on all included databases and schemas.
- **Connection failures**: validate `host_port`, database name, and whether the runtime can reach the MariaDB instance.
- **SSL handshake issues**: confirm `ssl_ca`, `ssl_cert`, and `ssl_key` paths and certificate compatibility with the server.
