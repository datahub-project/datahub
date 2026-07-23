### Overview

The `hive` module ingests metadata from Hive into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Metadata for databases, schemas, and tables
- Column types associated with each table
- Detailed table and storage information
- Table, row, and column statistics via optional SQL profiling.

#### Related Documentation

- [Hive Source Configuration](hive_recipe.yml) - Configuration examples
- [Hive Metastore Connector](../hive-metastore) - Alternative connector for direct metastore access
- [PyHive Documentation](https://github.com/dropbox/PyHive) - Underlying connection library

### Prerequisites

1. **Network Access**: Access to HiveServer2 on port 10000 (or 10001 for TLS)

2. **User Account**: Hive user with read permissions on target databases and tables

3. **Dependencies**: Install PyHive connectivity:
   ```bash
   pip install 'acryl-datahub[hive]'
   ```

#### Required Permissions

The Hive user account used by DataHub needs the following permissions:

##### Minimum Permissions (Metadata Only)

```sql
-- Grant SELECT on all databases you want to ingest
GRANT SELECT ON DATABASE <database_name> TO USER <datahub_user>;

-- Grant SELECT on tables/views for schema extraction
GRANT SELECT ON TABLE <database_name>.* TO USER <datahub_user>;
```

##### Additional Permissions for Storage Lineage

If you plan to enable storage lineage, the connector needs to read table location information:

```sql
-- Grant DESCRIBE on tables to read storage locations
GRANT SELECT ON <database_name>.* TO USER <datahub_user>;
```

##### Recommendations

- **Read-only Access**: DataHub only needs read permissions. Never grant `INSERT`, `UPDATE`, `DELETE`, or `DROP` privileges.
- **Database Filtering**: If you only need to ingest specific databases, use the `database` config parameter to limit scope and reduce the permissions required.

#### Authentication

The Hive connector supports multiple authentication methods through PyHive. Configure authentication using the recipe parameters described below.

##### Basic Authentication (Username/Password)

The simplest authentication method using a username and password:

```yaml
source:
  type: hive
  config:
    host_port: hive.company.com:10000
    username: datahub_user
    password: ${HIVE_PASSWORD} # Use environment variables for sensitive data
```

##### LDAP Authentication

For LDAP-based authentication:

```yaml
source:
  type: hive
  config:
    host_port: hive.company.com:10000
    username: datahub_user
    password: ${LDAP_PASSWORD}
    options:
      connect_args:
        auth: LDAP
```

##### Kerberos Authentication

For Kerberos-secured Hive clusters:

```yaml
source:
  type: hive
  config:
    host_port: hive.company.com:10000
    options:
      connect_args:
        auth: KERBEROS
        kerberos_service_name: hive
```

**Requirements**:

- Valid Kerberos ticket (use `kinit` before running ingestion)
- Kerberos configuration file (`/etc/krb5.conf` or specified via `KRB5_CONFIG` environment variable)
- PyKerberos or requests-kerberos package installed

##### TLS/SSL Connection

For secure connections over HTTPS:

```yaml
source:
  type: hive
  config:
    host_port: hive.company.com:10001
    scheme: "hive+https"
    username: datahub_user
    password: ${HIVE_PASSWORD}
    options:
      connect_args:
        auth: BASIC
```

##### Azure HDInsight

For Microsoft Azure HDInsight clusters:

```yaml
source:
  type: hive
  config:
    host_port: <cluster_name>.azurehdinsight.net:443
    scheme: "hive+https"
    username: admin
    password: ${HDINSIGHT_PASSWORD}
    options:
      connect_args:
        http_path: "/hive2"
        auth: BASIC
```

##### Databricks (via PyHive)

For Databricks clusters using the Hive connector:

```yaml
source:
  type: hive
  config:
    host_port: <workspace-url>:443
    scheme: "databricks+pyhive"
    username: token # or your Databricks username
    password: ${DATABRICKS_TOKEN} # Personal access token or password
    options:
      connect_args:
        http_path: "sql/protocolv1/o/xxxyyyzzzaaasa/1234-567890-hello123"
```

**Note**: For comprehensive Databricks support, consider using the dedicated [Databricks Unity Catalog](../databricks) connector instead, which provides enhanced features.
