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

The `convert_urns_to_lowercase` option controls whether table/view names preserve their original case in DataHub URNs. The default is `false` (preserve case), which is recommended for most setups.

**When to use each setting:**

- `false` (default): Preserves original case. Use when you have mixed-case names like `MyTable` or case-sensitive collations.
- `true`: Converts names to lowercase. Use for consistent lowercase naming or when migrating from older DataHub setups.

**Example:**

```yaml
source:
  type: mssql
  config:
    host_port: "localhost:1433"
    database: "MyDatabase"
    convert_urns_to_lowercase: false # preserve case (default)
    include_view_lineage: true
```

**Warning:** Changing this setting after initial ingestion creates duplicate entities. If you must change it, soft-delete existing entities first or perform a full cleanup.

**Lineage resolution:** DataHub matches lineage using exact URN strings. This fix ensures view lineage URNs match table URNs when `convert_urns_to_lowercase: false`, so lineage resolves correctly.

---
