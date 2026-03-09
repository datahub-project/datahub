### Overview

The `redshift` module ingests metadata from Redshift into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Requires specific database privileges for metadata extraction, lineage, and usage statistics.

#### Permission Overview

Three categories of permissions:

1. **System Table Access** - Access to Redshift system tables for lineage and usage statistics
2. **System View Access** - Access to system views for metadata discovery
3. **Data Access** - Access to user schemas and tables for profiling and classification

#### System Table and View Permissions

Execute as a superuser or user with grant privileges:

```sql
-- Core system access (required for lineage and usage statistics)
ALTER USER datahub WITH SYSLOG ACCESS UNRESTRICTED;

-- Core metadata extraction (always required)
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub;    -- Database information and properties
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub;      -- Schema information within databases
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub;      -- External schemas (Spectrum, federated)
GRANT SELECT ON pg_catalog.svv_table_info TO datahub;           -- Table metadata, statistics, and properties
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub;       -- External table definitions (Spectrum)
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub;      -- External table column information
GRANT SELECT ON pg_catalog.pg_class_info TO datahub;            -- Table creation timestamps and basic info

-- Essential pg_catalog tables for table discovery
GRANT SELECT ON pg_catalog.pg_class TO datahub;                 -- Table and view definitions
GRANT SELECT ON pg_catalog.pg_namespace TO datahub;             -- Schema namespace information
GRANT SELECT ON pg_catalog.pg_description TO datahub;           -- Table and column descriptions/comments
GRANT SELECT ON pg_catalog.pg_database TO datahub;              -- Database catalog information
GRANT SELECT ON pg_catalog.pg_attribute TO datahub;             -- Column definitions and properties
GRANT SELECT ON pg_catalog.pg_attrdef TO datahub;               -- Column default values
GRANT SELECT ON pg_catalog.svl_user_info TO datahub;            -- User information for ownership

-- Datashare lineage (enabled by default)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub;           -- Cross-cluster datashare information

-- Choose ONE based on your Redshift type:
-- For Provisioned Clusters:
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub;              -- Materialized view information (provisioned)

-- For Serverless Workgroups:
-- GRANT SELECT ON pg_catalog.svv_user_info TO datahub;          -- User information (serverless alternative)
-- GRANT SELECT ON pg_catalog.svv_mv_info TO datahub;           -- Materialized view information (serverless)

-- Schema access (required to read tables in each schema)
GRANT USAGE ON SCHEMA <schema_to_ingest> TO datahub;             -- Replace with actual schema names
```

#### Core System Views (Always Required)

These system views are accessed in all DataHub configurations:

```sql
-- Schema discovery
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub;      -- Schema information within databases
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub;      -- External schemas (Spectrum, federated)

-- Database information
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub;    -- Database information and properties

-- Table metadata and statistics
GRANT SELECT ON pg_catalog.svv_table_info TO datahub;           -- Table metadata, statistics, and properties

-- External table support
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub;       -- External table definitions (Spectrum)
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub;      -- External table column information

-- Table creation timestamps
GRANT SELECT ON pg_catalog.pg_class_info TO datahub;            -- Table creation timestamps and basic info
```

#### Conditional System Tables

Fire-grained permissions based on your Redshift configuration and desired features

##### Shared Database (Datashare Consumer)

```sql
-- Required when is_shared_database = True
GRANT SELECT ON pg_catalog.svv_redshift_tables TO datahub;       -- Table information in shared databases
GRANT SELECT ON pg_catalog.svv_redshift_columns TO datahub;      -- Column information in shared databases
```

##### Redshift Serverless Workgroups

```sql
-- Required for serverless workgroups
GRANT SELECT ON pg_catalog.svv_user_info TO datahub;            -- User information (serverless alternative)
GRANT SELECT ON pg_catalog.svv_mv_info TO datahub;             -- Materialized view information (serverless)
```

##### Redshift Provisioned Clusters

```sql
-- Required for provisioned clusters
GRANT SELECT ON pg_catalog.svl_user_info TO datahub;            -- User information for ownership (superuser table)
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub;              -- Materialized view information (provisioned)
```

##### Datashares Lineage

```sql
-- Required when include_share_lineage: true (default)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub;           -- Cross-cluster datashare information
```

#### Recommended Permission Set

For a typical provisioned cluster with default settings:

```sql
-- Core system access
ALTER USER datahub WITH SYSLOG ACCESS UNRESTRICTED;

-- Always required
GRANT SELECT ON pg_catalog.svv_redshift_databases TO datahub;    -- Database information and properties
GRANT SELECT ON pg_catalog.svv_redshift_schemas TO datahub;      -- Schema information within databases
GRANT SELECT ON pg_catalog.svv_external_schemas TO datahub;      -- External schemas (Spectrum, federated)
GRANT SELECT ON pg_catalog.svv_table_info TO datahub;           -- Table metadata, statistics, and properties
GRANT SELECT ON pg_catalog.svv_external_tables TO datahub;       -- External table definitions (Spectrum)
GRANT SELECT ON pg_catalog.svv_external_columns TO datahub;      -- External table column information
GRANT SELECT ON pg_catalog.pg_class_info TO datahub;            -- Table creation timestamps and basic info

-- Essential pg_catalog tables for table discovery
GRANT SELECT ON pg_catalog.pg_class TO datahub;                 -- Table and view definitions
GRANT SELECT ON pg_catalog.pg_namespace TO datahub;             -- Schema namespace information
GRANT SELECT ON pg_catalog.pg_description TO datahub;           -- Table and column descriptions/comments
GRANT SELECT ON pg_catalog.pg_database TO datahub;              -- Database catalog information
GRANT SELECT ON pg_catalog.pg_attribute TO datahub;             -- Column definitions and properties
GRANT SELECT ON pg_catalog.pg_attrdef TO datahub;               -- Column default values
GRANT SELECT ON pg_catalog.svl_user_info TO datahub;            -- User information for ownership (superuser table)

-- Datashares (since include_share_lineage defaults to true)
GRANT SELECT ON pg_catalog.svv_datashares TO datahub;           -- Cross-cluster datashare information

-- Provisioned cluster materialized views
GRANT SELECT ON pg_catalog.stv_mv_info TO datahub;              -- Materialized view information (provisioned)

-- Schema access (required to read tables in each schema)
GRANT USAGE ON SCHEMA <schema_to_ingest> TO datahub;             -- Replace with actual schema names
```

##### Data Access Privileges (Required for Data Profiling and Classification)

**Important**: The system table permissions above only provide access to metadata. To enable data profiling, classification, or any feature that reads actual table data, you must grant additional privileges:

```sql
-- Grant USAGE privilege on schemas (required to access schema objects)
GRANT USAGE ON SCHEMA public TO datahub;
GRANT USAGE ON SCHEMA your_schema_name TO datahub;

-- Grant SELECT privilege on existing tables for data access
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub;
GRANT SELECT ON ALL TABLES IN SCHEMA your_schema_name TO datahub;

-- Grant privileges on future objects (recommended for production)
-- IMPORTANT: These must be run by each user who will create tables/views
-- OR by a superuser with FOR ROLE clause

-- Option 1: If you (as admin) will create all future tables/views:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON VIEWS TO datahub;
ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema_name GRANT SELECT ON TABLES TO datahub;
ALTER DEFAULT PRIVILEGES IN SCHEMA your_schema_name GRANT SELECT ON VIEWS TO datahub;

-- Option 2: If other users will create tables/views, run this for each user:
-- ALTER DEFAULT PRIVILEGES FOR ROLE other_user_name IN SCHEMA public GRANT SELECT ON TABLES TO datahub;
-- ALTER DEFAULT PRIVILEGES FOR ROLE other_user_name IN SCHEMA public GRANT SELECT ON VIEWS TO datahub;

-- Option 3: For all future users (requires superuser):
-- ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA public GRANT SELECT ON TABLES TO datahub;
-- ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA public GRANT SELECT ON VIEWS TO datahub;
```

:::caution Data Access vs Metadata Access

**The permissions are split into two categories:**

1. **System table permissions** (above) - Required for metadata extraction, lineage, and usage statistics
2. **Data access permissions** (this section) - Required for data profiling, classification, and any feature that reads actual table content

**Default privileges only apply to objects created by the user who ran the ALTER DEFAULT PRIVILEGES command.** If multiple users create tables in your schemas, you need to:

1. **Run the commands as each user**, OR
2. **Use `FOR ROLE other_user_name`** for each user who creates objects, OR
3. **Use `FOR ALL ROLES`** (requires superuser privileges)

**Common gotcha**: If User A runs `ALTER DEFAULT PRIVILEGES` and User B creates a table, DataHub won't have access to User B's table unless you used Option 2 or 3 above.

**Alternative approach**: Instead of default privileges, consider using a scheduled job to periodically grant access to new tables:

```sql
-- Run this periodically to catch new tables
GRANT SELECT ON ALL TABLES IN SCHEMA your_schema_name TO datahub;
```

:::

##### Optional: Datashare Privileges

To enable cross-cluster lineage through datashares, grant the following privileges:

```sql
-- Grant SHARE privilege on datashares (replace with actual datashare names)
GRANT SHARE ON your_datashare_name TO datahub;
```
