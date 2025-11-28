### Prerequisites

If you want to ingest MSSQL Jobs and stored procedures (with code) the user credentials needs the proper privileges.

The DataHub MSSQL source automatically detects your environment and uses the optimal method:

- **RDS/Managed SQL Server**: Uses stored procedures (recommended for managed environments)
- **On-premises SQL Server**: Uses direct table access (typically faster when available)
- **Automatic fallback**: If the primary method fails, it automatically tries the alternative approach

#### Permissions for All Environments

```sql
-- Core permissions for stored procedures (required)
USE MSDB
GRANT SELECT ON OBJECT::msdb.dbo.sysjobsteps TO 'USERNAME'
GRANT SELECT ON OBJECT::msdb.dbo.sysjobs TO 'USERNAME'

-- Stored procedure permissions (required for RDS/managed environments)
GRANT EXECUTE ON msdb.dbo.sp_help_job TO 'USERNAME'
GRANT EXECUTE ON msdb.dbo.sp_help_jobstep TO 'USERNAME'

-- Permissions for stored procedure code and lineage
USE 'DATA_DB_NAME'
GRANT VIEW DEFINITION TO 'USERNAME'
GRANT SELECT ON OBJECT::sys.sql_expression_dependencies TO 'USERNAME'
```

#### RDS SQL Server Specific Notes

For **Amazon RDS SQL Server** environments, the stored procedure approach is preferred and typically the only method that works due to restricted table access. Ensure the following permissions are granted:

```sql
-- Essential for RDS environments
GRANT EXECUTE ON msdb.dbo.sp_help_job TO 'USERNAME'
GRANT EXECUTE ON msdb.dbo.sp_help_jobstep TO 'USERNAME'
```

#### On-Premises SQL Server Notes

For **on-premises SQL Server** installations, direct table access is typically available and faster. The source will automatically use direct queries when possible, with stored procedures as fallback.

#### Troubleshooting Permissions

If you encounter permission errors:

1. **RDS environments**: Ensure stored procedure execute permissions are granted
2. **On-premises environments**: Verify both table select and stored procedure execute permissions
3. **Mixed environments**: Grant all permissions listed above for maximum compatibility

The DataHub source will automatically handle fallback between methods and provide detailed error messages with specific permission requirements if issues occur.

---

## URN Case Sensitivity and Lineage

### Understanding `convert_urns_to_lowercase`

The MSSQL source provides a `convert_urns_to_lowercase` configuration option that controls how table and view names are represented in DataHub URNs (Uniform Resource Names). This setting is crucial for ensuring lineage works correctly.

**Default Behavior** (`convert_urns_to_lowercase: false`):

- Preserves the original case of table and view names as they appear in SQL Server
- Recommended for most use cases, especially when:
  - Your database uses case-sensitive collation
  - You have tables with mixed-case names (e.g., `MyTable`, `CustomerData`)
  - You want URNs to match the actual object names in SQL Server

**Lowercase Mode** (`convert_urns_to_lowercase: true`):

- Converts all table and view names to lowercase in URNs
- Useful when:
  - You want consistent lowercase naming across all platforms
  - You're migrating from an older DataHub setup that used lowercase URNs
  - You want to avoid potential case-sensitivity issues

### Example Configuration

```yaml
source:
  type: mssql
  config:
    host_port: "localhost:1433"
    database: "MyDatabase"
    username: "datahub_user"
    password: "secure_password"

    # Preserve original case (recommended)
    convert_urns_to_lowercase: false

    # Enable lineage extraction
    include_view_lineage: true
    include_stored_procedures: true
```

### How Lineage Works

DataHub's lineage system relies on **exact URN matching**. For lineage to work correctly between tables and views:

1. **Table URN**: Generated when ingesting table metadata

   ```
   urn:li:dataset:(urn:li:dataPlatform:mssql,MyDB.dbo.CustomerTable,PROD)
   ```

2. **View Lineage URN**: Generated when parsing view definitions

   ```
   urn:li:dataset:(urn:li:dataPlatform:mssql,MyDB.dbo.CustomerView,PROD)
   └─ Upstream: urn:li:dataset:(urn:li:dataPlatform:mssql,MyDB.dbo.CustomerTable,PROD)
   ```

3. **URNs Must Match**: The upstream URN in the view lineage must exactly match the table URN

**With `convert_urns_to_lowercase: false`** (Recommended):

```
✅ Table:    MyDB.dbo.CustomerTable
✅ Lineage:  MyDB.dbo.CustomerTable  → Lineage works!
```

**With `convert_urns_to_lowercase: true`**:

```
✅ Table:    mydb.dbo.customertable
✅ Lineage:  mydb.dbo.customertable  → Lineage works!
```

### Important: Changing Configuration

⚠️ **Warning**: Switching the `convert_urns_to_lowercase` setting after initial ingestion will create duplicate entities in DataHub.

**Example of the Problem**:

```
Initial ingestion with convert_urns_to_lowercase: true
  → Creates: urn:li:dataset:(...,mydb.dbo.customertable,PROD)

Re-ingestion with convert_urns_to_lowercase: false
  → Creates: urn:li:dataset:(...,MyDB.dbo.CustomerTable,PROD)

Result: Two separate entities for the same table!
```

**If You Must Change the Setting**:

1. **Soft delete** old entities before re-ingesting (recommended)
2. Perform a **full cleanup** and re-ingestion
3. Use DataHub's **entity deletion** APIs to remove old URNs

### Case-Sensitive Collations

If your SQL Server uses a **case-sensitive collation** (e.g., `SQL_Latin1_General_CP1_CS_AS`):

- Set `convert_urns_to_lowercase: false` to preserve case distinctions
- This allows DataHub to distinguish between `CustomerTable` and `customertable` if both exist
- Lineage will work correctly as long as the case matches between table and view definitions

### Troubleshooting Lineage Issues

**Problem**: Lineage not showing up between views and tables

**Solution**:

1. Check that `convert_urns_to_lowercase` is set consistently
2. Verify table URNs in DataHub match the case used in view definitions
3. Ensure `include_view_lineage: true` is set in your configuration
4. Check the ingestion logs for any SQL parsing errors

**Example Query to Check URNs**:

```graphql
query {
  dataset(
    urn: "urn:li:dataset:(urn:li:dataPlatform:mssql,MyDB.dbo.MyView,PROD)"
  ) {
    urn
    upstreamLineage {
      upstreams {
        dataset {
          urn
          name
        }
      }
    }
  }
}
```

### Best Practices

1. **Choose Once**: Decide on `convert_urns_to_lowercase` setting before initial ingestion
2. **Be Consistent**: Use the same setting across all MSSQL sources in your organization
3. **Document Your Choice**: Record which setting you're using for future reference
4. **Test Lineage**: After ingestion, verify that lineage appears correctly in the DataHub UI
5. **Use Quoted Identifiers**: In SQL Server, use brackets `[TableName]` or quotes `"TableName"` to preserve case in view definitions

---
