### Setup

This integration extracts metadata from Apache HBase via the Thrift API, including information about:

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

   This will install `happybase` and `thrift` packages required for connecting to HBase.

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

- **No Authentication**: By default, the connector uses no authentication.
- **Kerberos**: Set `auth_mechanism: "KERBEROS"` in the configuration.
- **Custom Authentication**: Specify your authentication mechanism in the `auth_mechanism` field.

:::note

For production deployments, it's recommended to use secure authentication mechanisms and SSL/TLS connections.

:::

:::caution

The connector samples column qualifiers to extract schema information. For tables with many column qualifiers, adjust the `max_column_qualifiers` parameter to control the sampling size and avoid performance issues.

:::
