### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

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

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
