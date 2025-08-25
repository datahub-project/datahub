## Dremio Access Control and DataHub Integration

This guide explains how Dremio's access control system affects what metadata DataHub can extract and how to configure proper permissions for optimal integration.

### Understanding Dremio's RBAC Model

Dremio uses a **Role-Based Access Control (RBAC)** model with four key components:

1. **Users**: Identity entities (human or service accounts)
2. **Roles**: Collections of privileges based on job functions
3. **Privileges**: Specific permissions defining allowed operations
4. **Securable Objects**: Entities like projects, sources, folders, tables, and views

For complete details, see the [Dremio Access Control documentation](https://docs.dremio.com/cloud/security/access-control/).

### How Access Control Affects DataHub Ingestion

DataHub's ability to extract metadata depends entirely on the privileges granted to the DataHub service account. The connector will only discover and ingest objects that the configured user can access.

#### Inheritance and Scope Impact

Dremio's privilege inheritance directly affects what DataHub can see:

- **Project Level**: Privileges at the project level grant access to all current and future objects within that project
- **Source Level**: Privileges on a source apply to all datasets within that source
- **Folder Level**: Privileges on a folder apply to all datasets within that folder and subfolders
- **Dataset Level**: Individual table/view privileges provide access to specific objects only

**Critical Rule**: Users must have privileges on parent objects to access child objects. Even if you grant access to a specific table, the user must also have USAGE privileges on its containing folder, source, and project.

#### Common Access Scenarios

##### Scenario 1: Full Project Access

```sql
-- DataHub user can see everything in the project
GRANT USAGE ON PROJECT "Analytics" TO USER datahub_user;
GRANT SELECT ON ALL DATASETS IN PROJECT "Analytics" TO USER datahub_user;
```

**Result**: DataHub ingests all sources, folders, and datasets within the Analytics project.

##### Scenario 2: Source-Specific Access

```sql
-- DataHub user can only see specific sources
GRANT USAGE ON SOURCE "data-lake-prod" TO USER datahub_user;
GRANT SELECT ON ALL DATASETS IN SOURCE "data-lake-prod" TO USER datahub_user;
```

**Result**: DataHub only ingests metadata from the data-lake-prod source. Other sources in the project are invisible.

##### Scenario 3: Folder-Level Restrictions

```sql
-- DataHub user can only see specific folders
GRANT USAGE ON FOLDER "Analytics.Finance" TO USER datahub_user;
GRANT SELECT ON ALL DATASETS IN FOLDER "Analytics.Finance" TO USER datahub_user;
```

**Result**: DataHub only ingests datasets within the Finance folder. Other folders are not visible.

##### Scenario 4: Dataset-Level Granular Access

```sql
-- DataHub user can only see specific tables
GRANT SELECT ON TABLE "Analytics.Finance.revenue_summary" TO USER datahub_user;
GRANT SELECT ON TABLE "Analytics.Finance.cost_analysis" TO USER datahub_user;
```

**Result**: DataHub only ingests the specified tables. However, the user still needs USAGE privileges on parent containers.

### Privilege Requirements by Feature

#### Basic Metadata Extraction

**Required Privileges:**

- `USAGE` on project, source, and folder containers
- `SELECT` on tables and views for schema metadata

**SQL Example:**

```sql
-- Minimum privileges for basic metadata
GRANT USAGE ON PROJECT "Analytics" TO USER datahub_user;
GRANT SELECT ON ALL DATASETS IN PROJECT "Analytics" TO USER datahub_user;
```

#### Query Lineage and Usage Statistics

**Additional Privileges Required:**

- `SELECT` on system tables: `sys.jobs`, `sys.queries`, `sys.query_profiles`
- For Dremio Cloud: `SELECT` on `sys.organization.jobs`, `sys.organization.queries`

**SQL Example:**

```sql
-- Additional privileges for query lineage
GRANT SELECT ON sys.jobs TO USER datahub_user;
GRANT SELECT ON sys.queries TO USER datahub_user;
GRANT SELECT ON sys.query_profiles TO USER datahub_user;

-- For Dremio Cloud
GRANT SELECT ON sys.organization.jobs TO USER datahub_user;
GRANT SELECT ON sys.organization.queries TO USER datahub_user;
```

#### Data Profiling

**Additional Privileges Required:**

- `SELECT` access to actual table data (not just metadata)
- This is typically included in the basic `SELECT` privileges above

#### System Tables Access

**Additional Privileges Required (if `include_system_tables: true`):**

- `USAGE` and `SELECT` on `sys` schema
- `USAGE` and `SELECT` on `information_schema`

**SQL Example:**

```sql
-- System tables access
GRANT USAGE ON SCHEMA sys TO USER datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA sys TO USER datahub_user;
GRANT SELECT ON ALL VIEWS IN SCHEMA sys TO USER datahub_user;

GRANT USAGE ON SCHEMA information_schema TO USER datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO USER datahub_user;
GRANT SELECT ON ALL VIEWS IN SCHEMA information_schema TO USER datahub_user;
```

### Impact on DataHub Features

#### Container Discovery

DataHub discovers containers (projects, sources, folders) hierarchically. If a user lacks access to a parent container, child objects become invisible even if they have direct privileges on them.

**Example Issue:**

```sql
-- This won't work as expected
GRANT SELECT ON TABLE "Analytics.Finance.revenue" TO USER datahub_user;
-- Missing: GRANT USAGE ON PROJECT "Analytics" TO USER datahub_user;
-- Missing: GRANT USAGE ON FOLDER "Analytics.Finance" TO USER datahub_user;
```

**Result**: DataHub cannot access the table because it can't navigate the container hierarchy.

#### Filtering Behavior

DataHub's filtering patterns (`schema_pattern`, `table_pattern`) work on top of Dremio's access control:

1. **First**: Dremio access control determines what objects are visible
2. **Then**: DataHub filters apply to the visible objects

**Example:**

- User has access to `Analytics.Finance.*` and `Analytics.Marketing.*`
- DataHub config: `schema_pattern.allow: [".*Finance.*"]`
- **Result**: Only Finance objects are ingested, Marketing objects are filtered out

#### Lineage Extraction

Query-based lineage requires access to query history tables. Without these privileges:

- View definitions are still extracted for direct lineage
- Query-based lineage and usage statistics are skipped
- No impact on basic schema metadata extraction

### Best Practices for DataHub Integration

#### 1. Use Dedicated Service Account

Create a dedicated user for DataHub rather than using personal accounts:

```sql
-- Dremio Cloud
CREATE USER datahub_service;
GRANT ROLE datahub_role TO USER datahub_service;

-- Dremio Software/Enterprise
CREATE USER datahub_service PASSWORD 'secure_random_password';
GRANT ROLE datahub_role TO USER datahub_service;
```

#### 2. Follow Principle of Least Privilege

Grant only the minimum privileges needed for your use case:

```sql
-- For basic metadata only
GRANT USAGE ON PROJECT "Production" TO ROLE datahub_role;
GRANT SELECT ON ALL DATASETS IN PROJECT "Production" TO ROLE datahub_role;

-- Add query lineage only if needed
GRANT SELECT ON sys.jobs TO ROLE datahub_role;
GRANT SELECT ON sys.queries TO ROLE datahub_role;
```

#### 3. Use Role-Based Management

Create roles for different DataHub access levels:

```sql
-- Basic metadata role
CREATE ROLE datahub_basic_role;
GRANT USAGE ON PROJECT "Production" TO ROLE datahub_basic_role;
GRANT SELECT ON ALL DATASETS IN PROJECT "Production" TO ROLE datahub_basic_role;

-- Enhanced role with lineage
CREATE ROLE datahub_enhanced_role;
GRANT ROLE datahub_basic_role TO ROLE datahub_enhanced_role;
GRANT SELECT ON sys.jobs TO ROLE datahub_enhanced_role;
GRANT SELECT ON sys.queries TO ROLE datahub_enhanced_role;
```

#### 4. Regular Privilege Auditing

Periodically review and audit DataHub user privileges:

```sql
-- Check current privileges for DataHub user
SHOW GRANTS TO USER datahub_service;
SHOW GRANTS TO ROLE datahub_role;
```

#### 5. Monitor Access Patterns

Use Dremio's audit logs to monitor DataHub access patterns and identify any permission issues.

### Troubleshooting Access Issues

#### Missing Objects in DataHub

**Symptoms**: Expected tables/views don't appear in DataHub
**Diagnosis**:

1. Check if user has USAGE privileges on parent containers
2. Verify SELECT privileges on the specific objects
3. Review DataHub filtering patterns

**Solution**:

```sql
-- Verify and grant missing privileges
SHOW GRANTS TO USER datahub_service;
GRANT USAGE ON PROJECT "YourProject" TO USER datahub_service;
GRANT USAGE ON SOURCE "YourSource" TO USER datahub_service;
```

#### Partial Lineage Information

**Symptoms**: Some lineage connections are missing
**Diagnosis**: Check query history access privileges
**Solution**:

```sql
-- Grant query history access
GRANT SELECT ON sys.jobs TO USER datahub_service;
GRANT SELECT ON sys.queries TO USER datahub_service;
```

#### Permission Denied Errors

**Symptoms**: DataHub ingestion fails with permission errors
**Diagnosis**: User lacks basic access to configured objects
**Solution**: Review and grant appropriate privileges following the hierarchy requirements

### Security Considerations

#### 1. Credential Management

- Use Personal Access Tokens (PAT) instead of passwords for authentication
- Rotate credentials regularly
- Store credentials securely (environment variables, secret managers)

#### 2. Network Security

- Restrict DataHub service account to specific IP ranges if possible
- Use SSL/TLS for connections
- Consider VPN or private network connections for sensitive environments

#### 3. Audit and Monitoring

- Enable Dremio audit logging to track DataHub access
- Monitor for unusual access patterns
- Set up alerts for permission changes affecting DataHub users

### Example: Complete Setup for Production Environment

```sql
-- 1. Create DataHub role with comprehensive privileges
CREATE ROLE datahub_production_role;

-- 2. Grant project-level access
GRANT USAGE ON PROJECT "Production" TO ROLE datahub_production_role;
GRANT SELECT ON ALL DATASETS IN PROJECT "Production" TO ROLE datahub_production_role;

-- 3. Grant query history access for lineage
GRANT SELECT ON sys.jobs TO ROLE datahub_production_role;
GRANT SELECT ON sys.queries TO ROLE datahub_production_role;
GRANT SELECT ON sys.query_profiles TO ROLE datahub_production_role;

-- 4. Create dedicated service user
CREATE USER datahub_prod_service;
GRANT ROLE datahub_production_role TO USER datahub_prod_service;

-- 5. Verify privileges
SHOW GRANTS TO USER datahub_prod_service;
SHOW GRANTS TO ROLE datahub_production_role;
```

This setup provides DataHub with comprehensive access to production data while maintaining security best practices and following the principle of least privilege.
