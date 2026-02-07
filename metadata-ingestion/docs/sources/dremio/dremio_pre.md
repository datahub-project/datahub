### Setup

This integration extracts metadata directly from Dremio using both REST API calls and SQL queries.

**Prerequisites:**

- A running Dremio instance with API access enabled
- A user account with appropriate privileges (detailed below)
- Network connectivity from DataHub to your Dremio instance

#### Steps to Get the Required Information

1. **Generate a Personal Access Token**:

   - Log in to your Dremio instance
   - Navigate to your user profile (top-right corner)
   - Select **Generate API Token** to create a Personal Access Token for programmatic access
   - Save the token securely as it will only be displayed once

2. **Configure Required Permissions**:

   Your user account needs specific Dremio privileges for the DataHub connector to function properly:

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

   **Dataset Access (Optional - only needed for data profiling)**

   ```sql
   -- For data profiling (only if profiling is enabled)
   GRANT SELECT ON SOURCE <source_name> TO USER <username>
   GRANT SELECT ON SPACE <space_name> TO USER <username>
   ```

   **Query Lineage Access (Optional - only if include_query_lineage: true)**

   ```sql
   -- Required for query lineage extraction
   GRANT VIEW JOB HISTORY ON SYSTEM TO USER <username>
   ```

   **What each permission does:**

   - `READ METADATA`: Access to view containers and their metadata via API calls
   - `SELECT ON SYSTEM`: Access to system tables (`SYS.*`, `INFORMATION_SCHEMA.*`) for dataset metadata
   - `SELECT ON SOURCE/SPACE`: Read actual data for profiling (optional)
   - `VIEW JOB HISTORY`: Access query history tables for lineage (optional)

3. **Verify External Data Source Access**:

   If your Dremio instance connects to external data sources (AWS S3, databases, etc.), ensure that:

   - Dremio has proper credentials configured for those sources
   - The DataHub user can access datasets from those external sources
   - Network connectivity exists between Dremio and external sources
