### Overview

The `presto` module ingests metadata from Presto into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

:::info Presto vs. Presto-on-Hive

There are **two different ways** to ingest Presto metadata into DataHub, depending on your use case:

**Option 1: Presto Connector (This Source)**

**Use when**: You want to connect directly to Presto to extract metadata from **all catalogs** (not just Hive).

**Capabilities**:

- Extracts tables and views from all Presto catalogs (Hive, PostgreSQL, MySQL, Cassandra, etc.)
- Supports table and view metadata
- Supports data profiling
- Extracts view SQL definitions
- **Does NOT support storage lineage** (no access to underlying storage locations)
- Limited view lineage for complex Presto-specific SQL

**Configuration**:

```yaml
source:
  type: presto # ← This connector
  config:
    host_port: presto-coordinator.company.com:8080
    username: datahub_user
    password: ${PRESTO_PASSWORD}
```

**Option 2: Hive Metastore Connector with Presto Mode**

**Use when**: You want to ingest **Presto views that use the Hive metastore** and need storage lineage.

**Capabilities**:

- Extracts Presto views stored in Hive metastore
- **Supports storage lineage** from S3/HDFS/Azure to Hive tables to Presto views
- Better Presto view definition parsing
- Column-level lineage support
- Faster metadata extraction (direct database access)
- Only works with Hive-backed catalogs

**Configuration**:

```yaml
source:
  type: hive-metastore # ← Use this for storage lineage
  config:
    host_port: metastore-db.company.com:5432
    database: metastore
    scheme: "postgresql+psycopg2"
    mode: presto # ← Set mode to 'presto'

    # Enable storage lineage
    emit_storage_lineage: true
    hive_storage_lineage_direction: upstream
```

**For complete details**, see:

- [Hive Metastore Connector Documentation](../hive-metastore)
  :::

#### Related Documentation

- [Presto Configuration Examples](presto_recipe.yml)
- [Hive Metastore Connector](../hive-metastore) - For Presto-on-Hive with storage lineage
- [Trino Connector](../trino) - Similar connector for Trino (Presto's successor)
- [PyHive Documentation](https://github.com/dropbox/PyHive) - Underlying connection library

### Prerequisites

1. **Network Access**: Access to Presto coordinator on port 8080 (or 443 for HTTPS)

2. **User Account**: Presto user with permissions to query metadata

3. **Dependencies**: Install PyHive connectivity:
   ```bash
   pip install 'acryl-datahub[presto]'
   ```

The Presto user account used by DataHub needs minimal permissions:

```sql
-- Presto uses catalog-level permissions
-- The user needs SELECT access to system information tables
-- This is typically granted by default to all users
```

**Recommendation**: Use a read-only service account with access to all catalogs you want to ingest.

#### Authentication

##### Basic Authentication (Username/Password)

The most common authentication method:

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    username: datahub_user
    password: ${PRESTO_PASSWORD}
    database: hive # Optional: default catalog
```

##### LDAP Authentication

For LDAP-based authentication:

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    username: datahub_user
    password: ${LDAP_PASSWORD}
    database: hive
```

##### HTTPS/TLS Connection

For secure connections:

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:443
    username: datahub_user
    password: ${PRESTO_PASSWORD}
    database: hive
    options:
      connect_args:
        protocol: https
```

##### Kerberos Authentication

For Kerberos-secured Presto clusters:

```yaml
source:
  type: presto
  config:
    host_port: presto.company.com:8080
    database: hive
    options:
      connect_args:
        auth: KERBEROS
        kerberos_service_name: presto
```

**Requirements**:

- Valid Kerberos ticket (use `kinit` before running ingestion)
- PyKerberos package installed
