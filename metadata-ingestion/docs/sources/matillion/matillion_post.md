## Lineage Extraction

The Matillion connector extracts lineage information from OpenLineage events provided by the Matillion Public API's `/v1/lineage/events` endpoint. These events contain dataset-level and column-level lineage in standard OpenLineage format.

### Namespace Mapping

OpenLineage events use namespace URIs to identify data sources (e.g., `snowflake://account.region`, `postgresql://host:5432`). The connector automatically maps these namespaces to DataHub platforms using the platform prefix (the part before `://`).

#### Pre-Mapped Platforms

The following platforms are automatically mapped without additional configuration:

| OpenLineage Namespace Prefix     | DataHub Platform |
| -------------------------------- | ---------------- |
| `postgres://` or `postgresql://` | `postgres`       |
| `sqlserver://`                   | `mssql`          |
| `snowflake://`                   | `snowflake`      |
| `bigquery://`                    | `bigquery`       |
| `redshift://`                    | `redshift`       |
| `mysql://`                       | `mysql`          |
| `oracle://`                      | `oracle`         |
| `s3://`                          | `s3`             |
| `databricks://`                  | `databricks`     |
| `mongodb://`                     | `mongodb`        |

For platforms not in this list, the namespace prefix is used as-is as the DataHub platform name.

**Unmapped Namespaces:** When a namespace is encountered without an explicit `namespace_to_platform_instance` configuration:

- Platform is automatically extracted from the namespace URI (e.g., `postgresql://host:5432` → `postgres` platform)
- Environment defaults to `PROD`
- No `platform_instance` is set (None)
- No database/schema defaults are applied (None)
- URN lowercasing is disabled (False)

This allows the connector to extract lineage from any data source without requiring explicit configuration for every namespace.

#### Custom Platform Mappings

To override default mappings or add new ones, use the `lineage_platform_mapping` configuration:

```yaml
source:
  type: matillion
  config:
    # ... other config ...
    lineage_platform_mapping:
      customdb: postgres # Map "customdb://" to DataHub's postgres platform
      mywarehouse: snowflake # Map "mywarehouse://" to DataHub's snowflake platform
```

#### Platform Instance Mapping

When you have multiple instances of the same platform (e.g., production and staging Snowflake), use `namespace_to_platform_instance` with longest-prefix matching:

```yaml
source:
  type: matillion
  config:
    # ... other config ...
    namespace_to_platform_instance:
      "snowflake://prod-account.us-east-1":
        platform_instance: "snowflake_prod"
        env: "PROD"
        database: "PROD_DB" # Optional: default database for incomplete names
        schema: "PUBLIC" # Optional: default schema for incomplete names
        convert_urns_to_lowercase: true # Lowercase dataset URNs (matches Snowflake connector)
      "postgresql://staging-db.example.com:5432":
        platform_instance: "postgres_staging"
        env: "DEV"
        database: "staging_db"
        schema: "public"
        convert_urns_to_lowercase: false # Preserve case for PostgreSQL
      "mysql://prod-db:3306":
        platform_instance: "mysql_prod"
        env: "PROD"
        database: "prod_db" # MySQL uses 2-part names (database.table)
        # No schema for MySQL
```

**How it works:**

- The connector finds the longest matching prefix from your configuration
- For `snowflake://prod-account.us-east-1/db.schema.table`, it matches `snowflake://prod-account.us-east-1`
- Datasets from this namespace get `platform_instance="snowflake_prod"` and `env="PROD"`

**Configuration Options:**

- **`platform_instance`**: Platform instance identifier for the data source

  - When set, DataHub prepends this to the dataset name in the final URN
  - Example: `platform_instance.schema.table` or `platform_instance.table` depending on platform type
  - **Important**: When `platform_instance` is configured, the `database` is omitted from normalized names because DataHub will prepend the platform_instance automatically

- **`database` and `schema`**: Handle incomplete dataset names from OpenLineage events

  - **3-tier platforms** (Snowflake, Postgres, Redshift): Use `database.schema.table` format
    - Without platform_instance: `table` → `database.schema.table`
    - With platform_instance: `table` → `schema.table` (database omitted, platform_instance prepended by DataHub)
  - **2-tier platforms** (MySQL, Hive, Teradata): Use `database.table` format
    - Without platform_instance: `table` → `database.table`
    - With platform_instance: `table` → `table` (database omitted, platform_instance prepended by DataHub)
  - Already qualified names remain unchanged

- **`convert_urns_to_lowercase`**: Whether to lowercase dataset URNs (default: `false`)
  - Set to `true` for Snowflake to match the Snowflake connector's default behavior
  - Ensures lineage correctly links between Snowflake connector ingestion and Matillion pipelines
  - Note: Schema field names are handled separately (Snowflake fields are always lowercased)

### Lineage Time Range

By default, the connector fetches OpenLineage events from the last 7 days. Configure this using:

```yaml
source:
  type: matillion
  config:
    lineage_start_days_ago: 14 # Fetch events from last 14 days
```

### Column-Level Lineage

Column-level lineage is enabled by default when `include_lineage: true`. The connector extracts column lineage from two sources:

1. **OpenLineage columnLineage facets**: Direct column-to-column mappings from OpenLineage events
2. **SQL Parsing** (when `parse_sql_for_lineage: true`): Enhanced lineage from SQL queries in OpenLineage events using DataHub's SqlParsingAggregator

To customize column-level lineage behavior:

```yaml
source:
  type: matillion
  config:
    include_lineage: true
    include_column_lineage: true # Enable column lineage (default: true)
    parse_sql_for_lineage: true # Use SQL parsing (default: true)
```

To disable column-level lineage and only extract dataset-level lineage:

```yaml
source:
  type: matillion
  config:
    include_lineage: true
    include_column_lineage: false # Disable fine-grained lineage
```

**Platform-Specific Handling:**

The connector automatically normalizes dataset names and field names based on platform conventions:

**Dataset Names:**

- **3-tier platforms** (Snowflake, Postgres, Redshift, BigQuery, Oracle, MS SQL, DB2): Use `database.schema.table` naming
- **2-tier platforms** (MySQL, Hive, Teradata, ClickHouse, Glue, Iceberg): Use `database.table` or `schema.table` naming
- **With platform_instance**: Database is omitted from the normalized name (DataHub prepends platform_instance to create the final URN)

**Field Names:**

- **Snowflake**: Field names are automatically lowercased to match the Snowflake connector's behavior
  - Example: `CUSTOMER_ID` in OpenLineage → `customer_id` in schema field URN
- **Other platforms**: Field names preserve their original casing from OpenLineage events

## Known Limitations

- OpenLineage events are only available for pipeline executions that occurred during the configured time window (`lineage_start_days_ago`)
- Lineage requires Matillion to have generated OpenLineage events for your pipelines
- The connector caches all lineage events for the time range at startup to optimize API calls
