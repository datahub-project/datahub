### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### Connection Issues

**Problem**: `Could not connect to HiveServer2`

**Solutions**:

- Verify `host_port` is correct and accessible
- Check firewall rules allow traffic on the Hive port
- Confirm HiveServer2 service is running: `beeline -u jdbc:hive2://<host>:<port>`

#### Authentication Failures

**Problem**: `Authentication failed`

**Solutions**:

- Verify username and password are correct
- Check authentication method matches your Hive configuration
- For Kerberos: Ensure valid ticket exists (`klist`)
- Review HiveServer2 logs for detailed error messages

#### Missing Tables

**Problem**: Not all tables appear in DataHub

**Solutions**:

- Verify user has SELECT permissions on missing tables
- Check if tables are in filtered databases
- Review warnings in ingestion logs
- Ensure tables are not temporary or views with complex definitions

#### Storage Lineage Not Appearing

**Problem**: No storage lineage relationships visible

**Solutions**:

- Verify `emit_storage_lineage: true` is set
- Check tables have defined storage locations: `DESCRIBE FORMATTED <table>`
- Review logs for "Failed to parse storage location" warnings
- See the "Storage Lineage" section above for more troubleshooting tips

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
