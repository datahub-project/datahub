### Prerequisites

To extract metadata from Trino, you'll need a Trino user with the following permissions:

- `SELECT` on `information_schema` tables (for schema metadata)
- `SELECT` on `system.metadata.catalogs` (for connector information)
- Access to any catalogs you want to extract metadata from

A Trino administrator can create a dedicated DataHub user with the required permissions:

```sql
-- Create a dedicated DataHub user
CREATE USER datahub_user;

-- Grant access to query catalogs and schemas
-- Replace 'your_catalog' with your actual catalog names
GRANT USAGE ON CATALOG your_catalog TO datahub_user;
GRANT USAGE ON SCHEMA your_catalog.your_schema TO datahub_user;

-- Grant SELECT on tables to read metadata
GRANT SELECT ON ALL TABLES IN SCHEMA your_catalog.your_schema TO datahub_user;

-- If extracting from multiple catalogs, repeat for each one
GRANT USAGE ON CATALOG hive_catalog TO datahub_user;
GRANT SELECT ON ALL TABLES IN SCHEMA hive_catalog.* TO datahub_user;
```

**Note**: The exact SQL syntax may vary depending on your Trino version and security configuration. Some environments use different access control models (file-based, LDAP, etc.). Consult your Trino administrator for the appropriate setup.

### Connector Lineage

The Trino connector supports extracting lineage from Trino tables and views to their underlying connector sources (e.g., mapping `trino:hive_catalog.schema.table` to `hive:schema.table`).

This feature is enabled by default via the `ingest_lineage_to_connectors` config option.

#### Supported Connectors

The following Trino connectors are supported for lineage extraction:

**Two-Tier Connectors** (use `schema.table` naming):

- Cassandra, ClickHouse, Delta Lake, Druid, Elasticsearch
- Glue, Hive, Hudi, Iceberg
- MariaDB, MongoDB, MySQL, Pinot

**Three-Tier Connectors** (use `database.schema.table` naming - requires `connector_database` config):

- BigQuery, Databricks, DB2, Oracle
- PostgreSQL, Redshift
- Snowflake, SQL Server, Teradata, Vertica

#### Configuration

For each Trino catalog you want to extract lineage from, provide mapping details in your recipe:

```yaml
catalog_to_connector_details:
  # Two-tier example (Hive)
  hive_catalog:
    connector_platform: hive
    platform_instance: prod_hive # Optional
    env: PROD # Optional

  # Three-tier example (PostgreSQL)
  postgres_catalog:
    connector_platform: postgresql
    connector_database: my_database # Required for 3-tier connectors
    platform_instance: prod_postgres # Optional
    env: PROD # Optional
```

### Column-Level Lineage

Column-level lineage between Trino and upstream connectors is enabled by default via `include_column_lineage: true`. This creates 1:1 column mappings between Trino tables/views and their upstream source columns.

**Note**: This feature makes additional metadata queries (2x calls per table/view). For large deployments (1000+ tables), consider disabling it by setting `include_column_lineage: false` to improve ingestion performance.

### View Lineage

The Trino connector supports two types of view lineage:

#### 1. Trino View-to-Table Lineage (SQL Parsing)

**Config**: `include_view_lineage` (enabled by default)  
**Scope**: Within Trino

This parses Trino view definitions to extract lineage from views to their upstream **Trino** tables/views:

- `trino:catalog.schema.my_view` → `trino:catalog.schema.source_table1`
- `trino:catalog.schema.my_view` → `trino:catalog.schema.source_table2`

**What it handles**:

- Complex SQL transformations (JOINs, UNIONs, CTEs, subqueries)
- Column aliases and expressions
- Multi-table dependencies
- Nested views (view referencing other views)

**Column-level lineage**: Enabled via `include_view_column_lineage` - tracks which source columns flow into which view columns through the SQL transformations.

#### 2. Connector View Lineage (1:1 Mapping)

**Config**: `ingest_lineage_to_connectors` + `include_column_lineage` (both enabled by default)  
**Scope**: Trino to upstream connector

This creates lineage from Trino views to their corresponding **upstream connector views**:

- `trino:hive_catalog.schema.my_view` → `hive:schema.my_view`

**What it handles**:

- Direct view exposure from connectors (Hive views, PostgreSQL views, etc.)
- Assumes the view exists in the upstream connector
- Column-level: 1:1 name mapping (no transformation tracking)

#### Example: Combined View Lineage

For a Hive view accessed through Trino:

```
Hive View (source) → Trino View → Downstream Consumer
    hive:db.vw_sales → trino:hive_cat.db.vw_sales → looker:sales_dashboard
```

1. **Connector lineage** links Trino to Hive: `trino:hive_cat.db.vw_sales` → `hive:db.vw_sales`
2. **Trino view lineage** (if view has dependencies within Trino) would parse the view SQL to find upstream Trino tables

**Key Difference**:

- **Connector view lineage**: Simple identity mapping between Trino and upstream connector
- **Trino view lineage**: SQL-based transformation tracking within Trino itself

### Performance Considerations

**Column-level lineage** is enabled by default via `include_column_lineage: true`. This creates fine-grained column mappings between Trino tables/views and their upstream connector sources.

**Impact**: Makes additional database queries for each table/view. For large deployments (1000+ tables), consider:

- Disabling column lineage: `include_column_lineage: false`
- Limiting scope with `schema_pattern` or `table_pattern`
- Estimated overhead: +2-5 minutes per 1,000 tables

### Caveats

- **Connector lineage** works for tables and views that are directly exposed from connectors. It assumes column names match between Trino and the upstream source.
- **Performance**: Column-level lineage makes additional database calls. For very large deployments, consider disabling `include_column_lineage` or using `table_pattern` to limit scope.
- **View transformations**: Column lineage between Trino and connectors uses simple 1:1 name mapping. Complex lineage within Trino views (aliases, computed columns) is handled separately by view definition parsing.
