### Setup

This integration extracts metadata from Apache HBase via the Thrift API using the `happybase` Python library, including information about:

- Namespaces
- Tables
- Column families and their properties
- Table configurations

You'll need to have HBase Thrift server running and accessible with appropriate permissions.

#### Prerequisites

1. **Install Required Python Packages**:

   ```bash
   pip install 'acryl-datahub[hbase]'
   ```

   This will install the `happybase` package required for connecting to HBase.

2. **HBase Thrift Server**:

   - Ensure the HBase Thrift server is running (typically on port 9090).
   - Start the Thrift server if not already running:
     ```bash
     hbase thrift start -p 9090
     ```

3. **Network Access**:

   - The host running DataHub ingestion must have network access to the HBase Thrift server.
   - Verify connectivity:
     ```bash
     telnet <hbase-host> 9090
     ```

4. **Permissions**:
   - The user/service account must have read access to:
     - System tables for metadata extraction
     - Target namespaces and tables you want to ingest

#### Authentication

The current implementation supports unauthenticated connections to HBase Thrift server.

:::note

For production deployments, it's recommended to use secure connections and ensure your HBase Thrift server is properly secured with network-level access controls.

:::

:::info

The connector extracts column family metadata but does not sample individual column qualifiers. This ensures efficient metadata extraction without impacting HBase performance.

:::
