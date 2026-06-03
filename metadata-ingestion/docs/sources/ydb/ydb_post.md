### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

- **No schema layer.** YDB organizes tables in a directory tree rather than schemas. Directory paths are part of the table name (for example, `jaffle_shop/customers`), and a connection is bound to a single database, so ingestion targets one `database` at a time.
- **No foreign keys.** YDB does not expose foreign-key constraints, so no foreign-key lineage is emitted.

### Troubleshooting

If ingestion fails, validate connectivity to the endpoint, the `database` path, and read permissions first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
