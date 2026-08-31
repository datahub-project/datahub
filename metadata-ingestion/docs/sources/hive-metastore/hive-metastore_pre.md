### Overview

The `hive-metastore` module ingests metadata from Hive Metastore into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

- Extracts metadata from Hive Metastore.
- Supports two connection methods selected via connection_type:
  - sql: Direct connection to HMS backend database (MySQL/PostgreSQL)
  - thrift: Connection to HMS Thrift API with Kerberos support
- Features:
  - Table and view metadata extraction
  - Schema field types including complex types (struct, map, array)
  - Storage lineage to S3, HDFS, Azure, GCS
  - View lineage via SQL parsing
  - Stateful ingestion for stale entity removal

#### Related Documentation

- [Hive Metastore Configuration](hive-metastore_recipe.yml) - Configuration examples
- [Hive Connector](../hive) - Alternative connector via HiveServer2
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/) - Underlying database connection library

### Prerequisites

The Hive Metastore connector supports two connection modes:

1. **SQL Mode (Default)**: Connects directly to the Hive metastore database (MySQL, PostgreSQL, etc.)
2. **Thrift Mode**: Connects to Hive Metastore via the Thrift API (port 9083), with Kerberos support

Choose your connection mode based on your environment:

| Feature            | SQL Mode (default)               | Thrift Mode                      |
| ------------------ | -------------------------------- | -------------------------------- |
| **Use when**       | Direct database access available | Only HMS Thrift API accessible   |
| **Authentication** | Database credentials             | Kerberos/SASL or unauthenticated |
| **Port**           | Database port (3306/5432)        | Thrift port (9083)               |
| **Dependencies**   | Database drivers                 | `pymetastore`, `thrift-sasl`     |

**Requirements:**

1. **Database Access**: Direct read access to the Hive metastore database (MySQL or PostgreSQL)

2. **Network Access**: Access to metastore database on configured port

3. **Database Driver**: Install the appropriate Python driver:

   ```bash
   # For PostgreSQL metastore
   pip install 'acryl-datahub[hive]' psycopg2-binary

   # For MySQL metastore
   pip install 'acryl-datahub[hive]' PyMySQL
   ```

4. **Metastore Schema**: Typically `public` (PostgreSQL) or database name (MySQL)

#### Required Database Permissions

The database user account used by DataHub needs read-only access to the Hive metastore tables.

##### PostgreSQL Metastore

```sql
-- Create a dedicated read-only user for DataHub
CREATE USER datahub_user WITH PASSWORD 'secure_password';

-- Grant connection privileges
GRANT CONNECT ON DATABASE metastore TO datahub_user;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO datahub_user;

-- Grant SELECT on metastore tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_user;

-- Grant SELECT on future tables (for metastore upgrades)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;
```

##### MySQL Metastore

```sql
-- Create a dedicated read-only user for DataHub
CREATE USER 'datahub_user'@'%' IDENTIFIED BY 'secure_password';

-- Grant SELECT privileges on metastore database
GRANT SELECT ON metastore.* TO 'datahub_user'@'%';

-- Apply changes
FLUSH PRIVILEGES;
```

##### Required Metastore Tables

DataHub queries the following metastore tables:

| Table            | Purpose                                       |
| ---------------- | --------------------------------------------- |
| `DBS`            | Database/schema information                   |
| `TBLS`           | Table metadata                                |
| `TABLE_PARAMS`   | Table properties (including view definitions) |
| `SDS`            | Storage descriptor (location, format)         |
| `COLUMNS_V2`     | Column metadata                               |
| `PARTITION_KEYS` | Partition information                         |
| `SERDES`         | Serialization/deserialization information     |

**Recommendation**: Grant `SELECT` on all metastore tables to ensure compatibility with different Hive versions and for future DataHub enhancements.

#### Authentication

##### PostgreSQL

**Standard Connection**:

```yaml
source:
  type: hive-metastore
  config:
    host_port: metastore-db.company.com:5432
    database: metastore
    username: datahub_user
    password: ${METASTORE_PASSWORD}
    scheme: "postgresql+psycopg2"
```

**SSL Connection**:

```yaml
source:
  type: hive-metastore
  config:
    host_port: metastore-db.company.com:5432
    database: metastore
    username: datahub_user
    password: ${METASTORE_PASSWORD}
    scheme: "postgresql+psycopg2"
    options:
      connect_args:
        sslmode: require
        sslrootcert: /path/to/ca-cert.pem
```

##### MySQL

**Standard Connection**:

```yaml
source:
  type: hive-metastore
  config:
    host_port: metastore-db.company.com:3306
    database: metastore
    username: datahub_user
    password: ${METASTORE_PASSWORD}
    scheme: "mysql+pymysql" # Default if not specified
```

**SSL Connection**:

```yaml
source:
  type: hive-metastore
  config:
    host_port: metastore-db.company.com:3306
    database: metastore
    username: datahub_user
    password: ${METASTORE_PASSWORD}
    scheme: "mysql+pymysql"
    options:
      connect_args:
        ssl:
          ca: /path/to/ca-cert.pem
          cert: /path/to/client-cert.pem
          key: /path/to/client-key.pem
```

##### Amazon RDS (PostgreSQL or MySQL)

For AWS RDS-hosted metastore databases:

```yaml
source:
  type: hive-metastore
  config:
    host_port: metastore.abc123.us-east-1.rds.amazonaws.com:5432
    database: metastore
    username: datahub_user
    password: ${RDS_PASSWORD}
    scheme: "postgresql+psycopg2" # or 'mysql+pymysql'
    options:
      connect_args:
        sslmode: require # RDS requires SSL
```

##### Azure Database for PostgreSQL/MySQL

```yaml
source:
  type: hive-metastore
  config:
    host_port: metastore-server.postgres.database.azure.com:5432
    database: metastore
    username: datahub_user@metastore-server # Note: Azure requires @server-name suffix
    password: ${AZURE_DB_PASSWORD}
    scheme: "postgresql+psycopg2"
    options:
      connect_args:
        sslmode: require
```
