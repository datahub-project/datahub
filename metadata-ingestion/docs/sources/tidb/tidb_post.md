### Capabilities

- Ingests TiDB tables and views with schema and column metadata.
- Supports optional profiling and SQL-based lineage extraction when enabled.
- Supports secure connections through TLS client configuration.

#### Migration from MySQL Connector

If you were previously ingesting TiDB using the MySQL connector, switch to the dedicated TiDB connector so assets are modeled with the TiDB platform:

**Configuration changes:**

- Change `type: mysql` -> `type: tidb`
- Change port: `3306` -> `4000` unless your TiDB deployment uses a custom port

**Important:** Dataset URNs will change from `platform:mysql` to `platform:tidb`. This creates new entities in DataHub. Enable stateful ingestion with `remove_stale_metadata: true` to automatically clean up old MySQL-based entities.

### Limitations

- TiDB does not support stored procedures or functions, so stored procedure ingestion is disabled for this source.
- TiDB sequences (`CREATE SEQUENCE`) are not ingested. They are a TiDB-native object type with no MySQL equivalent and are not exposed through the MySQL-compatible table reflection path this source relies on.
- TiDB-specific table flavors (partitioned, cached, and global temporary tables) are ingested as regular tables, consistent with how TiDB reports them (`TABLE_TYPE = 'BASE TABLE'`).
- Lineage completeness depends on available view definitions and SQL parsing coverage.
- Profiling on very large schemas or wide tables can increase extraction time.

### Troubleshooting

- **Permission errors**: verify the ingestion user has required metadata grants on all included databases and schemas.
- **Connection failures**: validate `host_port`, database name, and whether the runtime can reach the TiDB instance.
- **TLS handshake issues**: confirm `ssl_ca`, `ssl_cert`, and `ssl_key` paths and certificate compatibility with the server.
