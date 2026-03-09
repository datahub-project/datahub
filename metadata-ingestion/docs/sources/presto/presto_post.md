### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### Connection Issues

**Problem**: `Could not connect to Presto`

**Solutions**:

- Verify `host_port` is correct and points to the Presto coordinator
- Check firewall rules allow traffic on the Presto port
- Confirm Presto service is running: `curl http://<host>:<port>/v1/info`
- Check Presto logs for connection errors

#### Authentication Failures

**Problem**: `Authentication failed`

**Solutions**:

- Verify username and password are correct
- Check authentication method matches Presto configuration
- For Kerberos: Ensure valid ticket exists (`klist`)
- Review Presto coordinator logs: `/var/log/presto/`

#### Missing Catalogs or Tables

**Problem**: Not all catalogs/tables appear in DataHub

**Solutions**:

- Verify user has access to catalogs: `SHOW CATALOGS;` in Presto
- Check if catalogs are filtered by `database_pattern`
- Ensure catalog connectors are properly configured in Presto
- Review warnings in DataHub ingestion logs

#### Slow Ingestion

**Problem**: Metadata extraction takes too long

**Solutions**:

- Use catalog/schema filtering to reduce scope
- Disable profiling or limit to specific tables
- Enable stateful ingestion
- Ensure Presto cluster has adequate resources
- Check Presto query queue and resource groups

#### View Lineage Not Appearing

**Problem**: No lineage for Presto views

**Solutions**:

- Complex Presto SQL may have limited lineage extraction
- For Hive-backed views, consider using the [Hive Metastore connector](../hive-metastore) with `mode: presto`
- Review logs for SQL parsing warnings
- Simplify view definitions if possible

#### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
