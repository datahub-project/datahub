### Prerequisites

#### Data Dictionary Mode/Views

The Oracle ingestion source supports two modes for extracting metadata information (see `data_dictionary_mode` option): `ALL` and `DBA`. In the `ALL` mode, the SQLAlchemy backend queries `ALL_` data dictionary views to extract metadata information. In the `DBA` mode, the Oracle ingestion source directly queries `DBA_` data dictionary views to extract metadata information. `ALL_` views only provide information accessible to the user used for ingestion while `DBA_` views provide information for the entire database (that is, all schema objects in the database).

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

- `ALL_` views display all the information accessible to the user used for ingestion, including information from the user's schema as well as information from objects in other schemas, if the user has access to those objects by way of grants of privileges or roles.
- `DBA_` views display all relevant information in the entire database. They can be queried only by users with the `SYSDBA` system privilege or `SELECT ANY DICTIONARY` privilege, or `SELECT_CATALOG_ROLE` role, or by users with direct privileges granted to them.

#### Required Permissions

The following permissions are required based on features used:

**Basic Metadata (Tables & Views)**

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

#### URN Format Configuration

By default, Oracle URNs are formatted as `schema.table` (e.g., `HR.EMPLOYEES`).

**When using `service_name` (recommended):** URNs are always `schema.table`, regardless of configuration.

**When using `database` in config:** You can optionally include the database name in URNs by setting `add_database_name_to_urn: true`. This is useful when ingesting from multiple Oracle databases into the same DataHub instance.

**URN Format Examples:**

- **With `service_name` (any `add_database_name_to_urn` value)**: `urn:li:dataset:(urn:li:dataPlatform:oracle,schema.table,PROD)`
  - Example: `urn:li:dataset:(urn:li:dataPlatform:oracle,hr.employees,PROD)`
- **With `database` in config + `add_database_name_to_urn: true`**: `urn:li:dataset:(urn:li:dataPlatform:oracle,database.schema.table,PROD)`
  - Example: `urn:li:dataset:(urn:li:dataPlatform:oracle,orcl.hr.employees,PROD)`

**Important Notes:**

- All assets (tables, views, stored procedures) will use the same URN format for consistent lineage
- Once you've ingested with a particular setting, changing it will create new URNs and break existing lineage
- The `add_database_name_to_urn` flag only has an effect when you specify `database` in your config (not when using `service_name`)

#### Stored Procedures and Functions

The Oracle connector ingests both stored procedures and functions as DataJob entities with distinct subtypes:

- **Stored Procedures** (object_type = `PROCEDURE`, `PACKAGE`):
  - Appear with subtype "Stored Procedure"
- **Functions** (object_type = `FUNCTION`):
  - Appear with subtype "Function"

**Container Hierarchy:**

Stored procedures and functions are organized together in a hierarchy matching tables and views:

```
Database (e.g., ORCL)
  └── Schema (e.g., HR)
      ├── Tables and Views
      └── stored_procedures (Flow container for all procedures and functions)
          ├── Individual procedures
          └── Individual functions
```

**Container URN Example:**

- All procedures and functions: `urn:li:dataFlow:(oracle,hr.stored_procedures,PROD)`

**Benefits:**

- Filter by subtype in the DataHub UI to distinguish functions from procedures
- Functions and procedures are organized together in the same container (consistent with PostgreSQL, MySQL, and Snowflake)
- Consistent browse path hierarchy with tables and views
- Full lineage extraction from SQL definitions, including cross-type lineage (e.g., a procedure calling a function)

**Note:** Functions return values and are typically used in queries, while procedures perform operations. Both are captured with their distinct subtypes for identification.
