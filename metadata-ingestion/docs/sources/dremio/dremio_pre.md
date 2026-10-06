### Overview

Dremio is a data lakehouse platform that provides SQL query capabilities across diverse data sources without copying data. The DataHub integration connects directly to Dremio via REST API and system table queries to discover and catalog your data assets.

This module captures physical datasets (tables), virtual datasets (views), spaces, folders, and data sources as containers. Optional capabilities include column-level lineage from view definitions, query-based lineage from job history, data profiling, and stateful deletion of removed entities.

### Prerequisites

You need a running Dremio instance with API access, a user account with appropriate permissions, and network connectivity between DataHub and Dremio.

#### Authentication

Generate a Personal Access Token for programmatic access:

- Log in to your Dremio instance
- Navigate to your user profile (top-right corner)
- Select **Generate API Token** to create a Personal Access Token
- Save the token securely — it is only displayed once

#### Required Permissions

Your user account needs the following Dremio privileges:

**Container Access (Required for container discovery)**

```sql
-- Grant access to view and use sources and spaces
GRANT READ METADATA ON SOURCE <source_name> TO USER <username>
GRANT READ METADATA ON SPACE <space_name> TO USER <username>

-- Or grant at system level for all containers
GRANT READ METADATA ON SYSTEM TO USER <username>
```

**System Tables Access (Required for metadata extraction)**

```sql
-- Grant access to system tables for dataset metadata
GRANT SELECT ON SYSTEM TO USER <username>
```

**Dataset Access (Optional — only needed for data profiling)**

```sql
-- For data profiling (only if profiling is enabled)
GRANT SELECT ON SOURCE <source_name> TO USER <username>
GRANT SELECT ON SPACE <space_name> TO USER <username>
```

**Query Lineage Access (Optional — only if `include_query_lineage: true`)**

```sql
-- Required for query lineage extraction
GRANT VIEW JOB HISTORY ON SYSTEM TO USER <username>
```

What each permission does:

- `READ METADATA`: Access to view containers and their metadata via API calls
- `SELECT ON SYSTEM`: Access to system tables (`SYS.*`, `INFORMATION_SCHEMA.*`) for dataset metadata
- `SELECT ON SOURCE/SPACE`: Read actual data for profiling (optional)
- `VIEW JOB HISTORY`: Access query history tables for lineage (optional)

#### External Data Source Verification

If your Dremio instance connects to external data sources (AWS S3, databases, etc.), ensure that:

- Dremio has proper credentials configured for those sources
- The DataHub user can access datasets from those external sources
- Network connectivity exists between Dremio and external sources
