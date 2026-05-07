### Capabilities

- Ingests MariaDB tables and views with schema and column metadata.
- Supports optional profiling and SQL-based lineage extraction when enabled.
- Supports secure connections through SSL client configuration.

### Limitations

- Lineage completeness depends on available query logs and SQL parsing coverage.
- Profiling on very large schemas or wide tables can increase extraction time.
- SSL setup depends on valid certificate paths and MariaDB server TLS configuration.

### Troubleshooting

- **Permission errors**: verify the ingestion user has required metadata grants on all included databases and schemas.
- **Connection failures**: validate `host_port`, database name, and whether the runtime can reach the MariaDB instance.
- **SSL handshake issues**: confirm `ssl_ca`, `ssl_cert`, and `ssl_key` paths and certificate compatibility with the server.
