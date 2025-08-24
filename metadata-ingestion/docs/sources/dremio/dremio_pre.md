### Prerequisites

To execute this source, your Dremio user will need specific privileges to read metadata from your Dremio environment. The required privileges depend on what data you want to extract and your Dremio deployment type (Cloud, Software, or Enterprise).

#### Dremio Access Control Overview

Dremio uses a **Role-Based Access Control (RBAC)** model with the following key concepts:

- **Users**: Identity entities that can represent humans or applications
- **Roles**: Collections of privileges assigned based on job functions
- **Privileges**: Specific permissions that define allowed operations
- **Securable Objects**: Entities like sources, tables, views, folders, and projects

For detailed information, see the [Dremio Access Control documentation](https://docs.dremio.com/cloud/security/access-control/).

#### Required Privileges for DataHub Integration

The DataHub connector requires different privilege levels depending on the features you want to use:

##### Basic Metadata Extraction (Minimum Required)

For basic schema and table metadata extraction, create a DataHub-specific role with these privileges:

**For Dremio Cloud:**

```sql
-- Create a DataHub role
CREATE ROLE datahub_role;

-- Grant basic privileges for metadata extraction
GRANT USAGE ON PROJECT "<your-project>" TO ROLE datahub_role;
GRANT SELECT ON ALL DATASETS IN PROJECT "<your-project>" TO ROLE datahub_role;

-- Grant privileges on specific sources (repeat for each source)
GRANT USAGE ON SOURCE "<your-source>" TO ROLE datahub_role;
GRANT SELECT ON ALL DATASETS IN SOURCE "<your-source>" TO ROLE datahub_role;

-- Create DataHub user and assign role
CREATE USER datahub_user;
GRANT ROLE datahub_role TO USER datahub_user;
```

**For Dremio Software/Enterprise:**

```sql
-- Create a DataHub role
CREATE ROLE datahub_role;

-- Grant privileges on spaces and sources
GRANT USAGE ON SPACE "<your-space>" TO ROLE datahub_role;
GRANT SELECT ON ALL DATASETS IN SPACE "<your-space>" TO ROLE datahub_role;

GRANT USAGE ON SOURCE "<your-source>" TO ROLE datahub_role;
GRANT SELECT ON ALL DATASETS IN SOURCE "<your-source>" TO ROLE datahub_role;

-- Create DataHub user and assign role
CREATE USER datahub_user PASSWORD '<secure-password>';
GRANT ROLE datahub_role TO USER datahub_user;
```

##### Advanced Features (Optional)

**For Query Lineage and Usage Statistics:**

```sql
-- Additional privileges for query history access
GRANT SELECT ON sys.jobs TO ROLE datahub_role;
GRANT SELECT ON sys.queries TO ROLE datahub_role;
GRANT SELECT ON sys.query_profiles TO ROLE datahub_role;

-- For Dremio Cloud, you may need organization-level access
GRANT SELECT ON sys.organization.jobs TO ROLE datahub_role;
GRANT SELECT ON sys.organization.queries TO ROLE datahub_role;
```

**For Data Profiling:**

```sql
-- Profiling requires SELECT access to actual table data
-- This is already included in the basic setup above
-- Ensure the role has SELECT privileges on tables you want to profile
```

**For System Tables and Intelligent Chunking:**

The connector requires access to system tables for two key features:

1. **System Tables Ingestion** (if `include_system_tables` is enabled)
2. **Intelligent View Chunking** (automatically detects Dremio's `single_field_size_bytes` limit)

```sql
-- Grant access to system schemas (required for both features)
GRANT USAGE ON SCHEMA sys TO ROLE datahub_role;
GRANT SELECT ON ALL TABLES IN SCHEMA sys TO ROLE datahub_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA sys TO ROLE datahub_role;

GRANT USAGE ON SCHEMA information_schema TO ROLE datahub_role;
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO ROLE datahub_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA information_schema TO ROLE datahub_role;

-- Specific tables required for intelligent chunking
-- (these are included in the grants above, but listed for clarity)
GRANT SELECT ON sys.options TO ROLE datahub_role;                    -- Enterprise/Software/Community
GRANT SELECT ON sys.project.options TO ROLE datahub_role;           -- Cloud
GRANT SELECT ON information_schema.options TO ROLE datahub_role;     -- Alternative detection method
```

**Note on Intelligent Chunking:**

- The connector automatically detects Dremio's `single_field_size_bytes` configuration limit across all editions
- This enables optimal processing of large view definitions without truncation
- Detection methods vary by edition: `sys.options` (Enterprise/Software/Community), `sys.project.options` (Cloud)
- If system table access is not available, the connector gracefully falls back to a safe 32KB chunk size
- For best performance with large views, ensure the DataHub user can access the appropriate system tables

#### Privilege Inheritance and Scope

Dremio's privilege system follows a hierarchical inheritance model:

- **Project Level**: Granting privileges at the project level provides access to all current and future objects within that project
- **Source Level**: Privileges on a source apply to all datasets within that source
- **Folder Level**: Privileges on a folder apply to all datasets within that folder and its subfolders
- **Dataset Level**: Privileges can be granted on individual tables or views

**Important Notes:**

- Users must have privileges on parent objects to access child objects
- Even if you only need access to a single table, you must have USAGE privileges on its parent containers (source, folder, project)
- The PUBLIC role is assigned to all users by default and provides basic USAGE privileges

#### Authentication Methods

DataHub supports multiple authentication methods for Dremio:

##### Username and Password Authentication

The simplest method using Dremio username and password:

```yaml
source:
  type: dremio
  config:
    username: datahub_user
    password: your_secure_password
```

##### Personal Access Token (PAT) Authentication

Recommended for Dremio Cloud and production environments:

```yaml
source:
  type: dremio
  config:
    authentication_method: PAT
    username: datahub_user # Your Dremio username
    password: your_personal_access_token # PAT token as password
```

To create a Personal Access Token:

1. Log into Dremio
2. Go to Account Settings → Personal Access Tokens
3. Click "New Token"
4. Set appropriate expiration and scope
5. Copy the generated token

#### Network Access Requirements

Ensure DataHub can reach your Dremio instance:

**For Dremio Cloud:**

- Default port: 443 (HTTPS)
- Ensure your DataHub deployment can reach `*.dremio.cloud`

**For Dremio Software:**

- Default port: 9047
- Ensure firewall rules allow access from DataHub to your Dremio coordinator nodes

**For Dremio Enterprise:**

- Same as Software, but may have additional load balancer configurations
- Coordinate with your Dremio administrator for proper endpoints

#### Minimum Dremio Versions

- **Dremio Cloud**: All versions supported
- **Dremio Software**: Version 21.0+ required
- **Dremio Enterprise**: Version 21.0+ required

Versions prior to 21.0 are not supported and may not work correctly with this connector.

#### Performance Considerations

For large Dremio deployments, consider these settings:

1. **Enable File-Backed Caching** to prevent OOM errors:

   ```yaml
   enable_file_backed_cache: true
   file_backed_cache_size: 1000
   ```

2. **Configure SQL Chunking** for large view definitions:

   ```yaml
   enable_sql_chunking: true
   max_view_definition_length: 50000
   ```

3. **Limit Concurrent Processing**:
   ```yaml
   max_workers: 5
   ```

#### Troubleshooting Common Issues

**Permission Denied Errors:**

- Verify the user has the required privileges listed above
- Check that parent object privileges are granted (project, source, folder)
- Ensure the user is not only in the PUBLIC role

**Connection Timeouts:**

- Verify network connectivity and firewall rules
- Check if Dremio instance is running and accessible
- For Dremio Cloud, ensure correct project ID is specified

**Missing Objects:**

- Check schema_pattern, table_pattern, and view_pattern filters
- Verify the user has access to the objects you expect to see
- Review include_system_tables setting if system objects are missing

For more detailed troubleshooting, enable debug logging:

```yaml
source:
  type: dremio
  config:
    # ... other config
    # Enable debug logging in your DataHub configuration
```
