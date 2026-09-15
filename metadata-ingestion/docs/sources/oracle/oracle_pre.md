### Overview

The `oracle` module ingests metadata from Oracle into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The Oracle source extracts metadata from Oracle databases, including:

- **Tables and Views**: Standard relational tables and views with column information, constraints, and comments
- **Stored Procedures**: Functions, procedures, and packages with source code, arguments, and dependency tracking
- **Materialized Views**: Materialized views with proper lineage and refresh information
- **Lineage**: Automatic lineage generation from stored procedure definitions and materialized view queries via SQL parsing
- **Usage Statistics**: Query execution statistics and table access patterns (when audit data is available)
- **Operations**: Data modification events (CREATE, INSERT, UPDATE, DELETE) from audit trail data

The connector uses the `python-oracledb` driver and supports both thin mode (default, no Oracle client required) and thick mode (requires Oracle client installation).

As a SQL-based service, the Oracle integration is also supported by our SQL profiler for table and column statistics.

### Prerequisites

#### Data Dictionary Mode/Views

Oracle supports two extraction modes via the `data_dictionary_mode` option:

- **`ALL` (default)**: Queries `ALL_*` views — extracts only objects accessible to the ingestion user
- **`DBA`**: Queries `DBA_*` views — extracts all schema objects in the database (requires elevated privileges)

The following table contains a brief description of what each data dictionary view is used for:

| Data Dictionary View                               | What's it used for?                                   |
| -------------------------------------------------- | ----------------------------------------------------- |
| `ALL_TABLES` or `DBA_TABLES`                       | Get list of all relational tables in the database     |
| `ALL_VIEWS` or `DBA_VIEWS`                         | Get list of all views in the database                 |
| `ALL_TAB_COMMENTS` or `DBA_TAB_COMMENTS`           | Get comments on tables and views                      |
| `ALL_TAB_COLS` or `DBA_TAB_COLS`                   | Get description of the columns of tables and views    |
| `ALL_COL_COMMENTS` or `DBA_COL_COMMENTS`           | Get comments on the columns of tables and views       |
| `ALL_TAB_IDENTITY_COLS` or `DBA_TAB_IDENTITY_COLS` | Get table identity columns                            |
| `ALL_CONSTRAINTS` or `DBA_CONSTRAINTS`             | Get constraint definitions on tables                  |
| `ALL_CONS_COLUMNS` or `DBA_CONS_COLUMNS`           | Get list of columns that are specified in constraints |
| `ALL_USERS` or `DBA_USERS`                         | Get all schema names                                  |
| `ALL_OBJECTS` or `DBA_OBJECTS`                     | Get stored procedures, functions, and packages        |
| `ALL_SOURCE` or `DBA_SOURCE`                       | Get source code for stored procedures and functions   |
| `ALL_ARGUMENTS` or `DBA_ARGUMENTS`                 | Get arguments for stored procedures and functions     |
| `ALL_DEPENDENCIES` or `DBA_DEPENDENCIES`           | Get dependency information for database objects       |
| `ALL_MVIEWS` or `DBA_MVIEWS`                       | Get materialized views and their definitions          |

#### Data Dictionary Views accessible information and required privileges

- **`ALL_` views**: Accessible with standard user privileges — shows only objects the user can access
- **`DBA_` views**: Requires `SYSDBA`, `SELECT ANY DICTIONARY` privilege, or `SELECT_CATALOG_ROLE` role

#### Required Permissions

The following permissions are required based on features used.

**Object Access**

The ingestion user must have SELECT privileges on the tables and views in each schema you want to catalog. Without these grants, `ALL_*` views will not return metadata for those objects.

```sql
-- Option 1: Grant access to all tables (simplest)
GRANT SELECT ANY TABLE TO datahub_user;

-- Option 2: Grant access per schema
BEGIN
  FOR t IN (SELECT table_name FROM dba_tables WHERE owner = '<SCHEMA_NAME>') LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT ON <SCHEMA_NAME>.' || t.table_name || ' TO datahub_user';
  END LOOP;
END;
/
```

If using `data_dictionary_mode: DBA`, object-level grants are not required. Instead, grant one of the following:

```sql
GRANT SELECT_CATALOG_ROLE TO datahub_user;
-- OR
GRANT SELECT ANY DICTIONARY TO datahub_user;
```

**Data Dictionary Views (Tables & Views)**

```sql
-- Using data_dictionary_mode: ALL (default)
GRANT SELECT ON ALL_TABLES TO datahub_user;
GRANT SELECT ON ALL_TAB_COLS TO datahub_user;
GRANT SELECT ON ALL_TAB_COMMENTS TO datahub_user;
GRANT SELECT ON ALL_COL_COMMENTS TO datahub_user;
GRANT SELECT ON ALL_VIEWS TO datahub_user;
GRANT SELECT ON ALL_CONSTRAINTS TO datahub_user;
GRANT SELECT ON ALL_CONS_COLUMNS TO datahub_user;

-- Using data_dictionary_mode: DBA (elevated permissions)
GRANT SELECT ON DBA_TABLES TO datahub_user;
GRANT SELECT ON DBA_TAB_COLS TO datahub_user;
GRANT SELECT ON DBA_TAB_COMMENTS TO datahub_user;
GRANT SELECT ON DBA_COL_COMMENTS TO datahub_user;
GRANT SELECT ON DBA_VIEWS TO datahub_user;
GRANT SELECT ON DBA_CONSTRAINTS TO datahub_user;
GRANT SELECT ON DBA_CONS_COLUMNS TO datahub_user;
```

**Stored Procedures (enabled by default)**

```sql
-- For ALL mode
GRANT SELECT ON ALL_OBJECTS TO datahub_user;
GRANT SELECT ON ALL_SOURCE TO datahub_user;
GRANT SELECT ON ALL_ARGUMENTS TO datahub_user;
GRANT SELECT ON ALL_DEPENDENCIES TO datahub_user;

-- For DBA mode
GRANT SELECT ON DBA_OBJECTS TO datahub_user;
GRANT SELECT ON DBA_SOURCE TO datahub_user;
GRANT SELECT ON DBA_ARGUMENTS TO datahub_user;
GRANT SELECT ON DBA_DEPENDENCIES TO datahub_user;
```

**Materialized Views (enabled by default)**

```sql
-- For ALL mode
GRANT SELECT ON ALL_MVIEWS TO datahub_user;

-- For DBA mode
GRANT SELECT ON DBA_MVIEWS TO datahub_user;
```

**Query Usage from V$SQL (optional)**

For extracting real query usage patterns, enable `include_query_usage: true` and grant:

```sql
GRANT SELECT ON V_$SQL TO datahub_user;
-- OR
GRANT SELECT_CATALOG_ROLE TO datahub_user;
```

**Database Name Resolution**

```sql
GRANT SELECT ON V_$DATABASE TO datahub_user;
```

#### Multitenant (CDB/PDB) Environments

In Oracle multitenant architectures (including Exadata), user data lives inside Pluggable Databases (PDBs). The Container Database root (CDB$ROOT) only contains system schemas (SYS, SYSTEM, XDB).

To ingest from a PDB, use `service_name` in your recipe config to connect directly to the PDB rather than the CDB:

```yaml
source:
  type: oracle
  config:
    host_port: "oracle-host:1521"
    service_name: "<PDB_SERVICE_NAME>"
```

Grants must be applied inside each PDB, not at the CDB level:

```sql
ALTER SESSION SET CONTAINER = <PDB_NAME>;
GRANT SELECT ANY TABLE TO datahub_user;
-- (plus dictionary view grants listed above)
```

If you need to ingest from multiple PDBs, create a separate ingestion source for each PDB service name.
