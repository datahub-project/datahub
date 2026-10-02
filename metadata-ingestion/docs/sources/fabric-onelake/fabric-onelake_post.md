### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Basic Recipe

```yaml
source:
  type: fabric-onelake
  config:
    # Authentication (using service principal)
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}

    # Optional: Platform instance (use as tenant identifier)
    # platform_instance: "contoso-tenant"

    # Optional: Environment
    # env: PROD

    # Optional: Filter workspaces by name pattern
    # workspace_pattern:
    #   allow:
    #     - "prod-.*"
    #   deny:
    #     - ".*-test"

    # Optional: Filter lakehouses by name pattern
    # lakehouse_pattern:
    #   allow:
    #     - ".*"
    #   deny: []

    # Optional: Filter warehouses by name pattern
    # warehouse_pattern:
    #   allow:
    #     - ".*"
    #   deny: []

    # Optional: Filter tables by name pattern
    # table_pattern:
    #   allow:
    #     - ".*"
    #   deny: []

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

#### Advanced Configuration

```yaml
source:
  type: fabric-onelake
  config:
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}

    # Platform instance (represents tenant)
    platform_instance: "contoso-tenant"

    # Environment
    env: PROD

    # Filtering
    workspace_pattern:
      allow:
        - "prod-.*"
        - "shared-.*"
      deny:
        - ".*-test"
        - ".*-dev"

    lakehouse_pattern:
      allow:
        - ".*"
      deny:
        - ".*-backup"

    warehouse_pattern:
      allow:
        - ".*"
      deny: []

    table_pattern:
      allow:
        - ".*"
      deny:
        - ".*_temp"
        - ".*_backup"

    view_pattern:
      allow:
        - ".*"
      deny:
        - ".*_internal"

    # Feature flags
    extract_lakehouses: true
    extract_warehouses: true
    extract_schemas: true # Set to false to skip schema containers
    extract_views: true # Requires sql_endpoint.enabled

    # API timeout (seconds)
    api_timeout: 30

    # Stateful ingestion (optional)
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

#### Using Managed Identity

```yaml
source:
  type: fabric-onelake
  config:
    credential:
      authentication_method: managed_identity
      # For user-assigned managed identity, specify client_id
      # client_id: ${MANAGED_IDENTITY_CLIENT_ID}

    platform_instance: "contoso-tenant"
    env: PROD

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

#### Using Azure CLI (Local Development)

```yaml
source:
  type: fabric-onelake
  config:
    credential:
      authentication_method: cli
      # Run 'az login' first

    platform_instance: "contoso-tenant"
    env: DEV

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

#### Schema Extraction

Schema extraction (column metadata) is supported via the SQL Analytics Endpoint. This feature extracts column names, data types, nullability, and ordinal positions from tables in both Lakehouses and Warehouses.

See [SQL Analytics Endpoint Setup](#sql-analytics-endpoint-setup) under Prerequisites for ODBC driver installation.

#### Schema Extraction Configuration

Schema extraction is enabled by default. You can configure it as follows:

```yaml
source:
  type: fabric-onelake
  config:
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}

    # Schema extraction configuration
    extract_schema:
      enabled: true # Enable schema extraction (default: true)
      method: sql_analytics_endpoint # Currently only this method is supported

    # SQL Analytics Endpoint configuration
    sql_endpoint:
      enabled: true # Enable SQL endpoint connection (default: true)
      # Optional: ODBC connection options
      # odbc_driver: "ODBC Driver 18 for SQL Server"  # Default: "ODBC Driver 18 for SQL Server"
      # encrypt: "yes"  # Enable encryption (default: "yes")
      # trust_server_certificate: "no"  # Trust server certificate (default: "no")
      query_timeout: 30 # Timeout for SQL queries in seconds (default: 30)
```

#### How It Works

1. **Endpoint Discovery**: The SQL Analytics Endpoint URL is automatically fetched from the Fabric API for each Lakehouse/Warehouse. The endpoint format is `<unique-identifier>.datawarehouse.fabric.microsoft.com` and cannot be constructed from workspace_id alone.
2. **Authentication**: Uses the same Azure credentials configured for REST API access with Azure AD token injection
3. **Connection**: Connects to the SQL Analytics Endpoint using ODBC with the discovered endpoint URL
4. **Query**: Queries `INFORMATION_SCHEMA.COLUMNS` to extract column metadata (required for schema extraction)
5. **Type Mapping**: SQL Server data types are automatically mapped to DataHub types using the standard type mapping system

**References:**

- [What is the SQL analytics endpoint for a lakehouse?](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)
- [Warehouse connectivity in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/data-warehouse/connectivity)
- [Connect to Fabric Data Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/how-to-connect)

#### Important Notes

- **Endpoint URL Discovery**: The SQL Analytics Endpoint URL is automatically fetched from the Fabric API for each Lakehouse/Warehouse. The endpoint format is `<unique-identifier>.datawarehouse.fabric.microsoft.com` and cannot be constructed from workspace_id alone. If the endpoint URL cannot be retrieved from the API, schema extraction will fail for that item.
- **No Fallback**: Unlike legacy Power BI Premium endpoints, Fabric SQL Analytics Endpoints do not support fallback connection strings. The endpoint must be obtained from the API.

#### Disabling Schema Extraction

To disable schema extraction and ingest tables without column metadata:

```yaml
source:
  type: fabric-onelake
  config:
    extract_schema:
      enabled: false
```

#### View Extraction

Views in Lakehouses and Warehouses are ingested as DataHub `Dataset` entities with the `View` subtype. Each view dataset includes:

- Column-level schema metadata (sourced from `INFORMATION_SCHEMA.COLUMNS` alongside table columns).
- The original view definition (`CREATE VIEW` SQL), captured from `INFORMATION_SCHEMA.VIEWS`.
- Upstream table lineage parsed from the view definition via the SQL parsing aggregator.

See [View Extraction](#view-extraction) under Prerequisites for required ODBC setup and the `VIEW DEFINITION` permission needed to read view definitions.

##### Configuration

```yaml
source:
  type: fabric-onelake
  config:
    # View extraction is enabled by default. Set to false to skip views.
    extract_views: true

    # Filter views by name pattern. Format: 'schema.view' or just 'view' for default schema.
    view_pattern:
      allow:
        - ".*"
      deny:
        - ".*_internal"

    # View extraction requires the SQL Analytics Endpoint (enabled by default).
    sql_endpoint:
      enabled: true
```

##### How It Works

1. **Discovery**: The connector queries `INFORMATION_SCHEMA.VIEWS` on the SQL Analytics Endpoint to list views and capture their definitions.
2. **Filtering**: Each view is matched against `view_pattern` using the `schema.view_name` form.
3. **Schema**: Column metadata is reused from the same `INFORMATION_SCHEMA.COLUMNS` query that powers table schema extraction — no extra queries per view.
4. **Lineage**: View definitions are passed to the SQL parsing aggregator to derive view → upstream table lineage. View URNs and upstream table URNs are resolved within the same workspace and item.

#### Usage Statistics

The connector extracts query usage statistics from each Lakehouse and Warehouse by reading the [`queryinsights.exec_requests_history`](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights) view on the SQL Analytics Endpoint. Usage and lineage are attached only to datasets this run ingested. Tables that appear only inside query text, including the connector's own schema and profiling SQL, are not created as assets. Warehouse tables are listed from the OneLake catalog, not from query history.

Each captured query is parsed by the SQL parsing aggregator and emitted as:

- `datasetUsageStatistics` aspects — query counts, distinct user counts, top users, top fields, and (when enabled) top SQL queries, bucketed by the configured window.
- `operation` aspects — per-query operation events (insert, update, delete, etc.) when `usage.include_operational_stats` is enabled.

See [Query Usage Statistics](#query-usage-statistics) under Prerequisites for the required workspace role (Contributor or higher) and ODBC setup.

##### Configuration

```yaml
source:
  type: fabric-onelake
  config:
    # Usage extraction is enabled by default. Set to false to skip query usage.
    usage:
      include_usage_statistics: true

      # When true, the SQL filter excludes rows where status != 'Succeeded'
      # (canceled / failed queries are skipped at the source).
      skip_failed_queries: true

      # Optional: emit per-query operation aspects in addition to aggregated
      # datasetUsageStatistics. Defaults to true (inherited from BaseUsageConfig).
      include_operational_stats: true

      # Optional: include top SQL queries in the usage payload.
      include_top_n_queries: true
      top_n_queries: 10

      # Optional: window the connector queries from queryinsights. Defaults to
      # the standard BaseUsageConfig "last bucket" window. Fabric retains
      # queryinsights for 30 days.
      bucket_duration: DAY
      # start_time: "2026-04-01T00:00:00Z"
      # end_time:   "2026-05-01T00:00:00Z"

    # Usage extraction depends on the SQL Analytics Endpoint.
    extract_schema:
      enabled: true
    sql_endpoint:
      enabled: true
```

All standard `BaseUsageConfig` fields (`bucket_duration`, `start_time`, `end_time`, `top_n_queries`, `format_sql_queries`, `include_top_n_queries`, `include_operational_stats`, `user_email_pattern`, etc.) are supported under the `usage` block.

When stateful ingestion is enabled, the usage time window is checkpointed only after a successful run, so a partial or failed run won't silently skip the next window.

#### Schemas-Enabled vs Schemas-Disabled Lakehouses

The connector automatically handles both schemas-enabled and schemas-disabled lakehouses:

- **Schemas-Enabled Lakehouses**: The connector uses OneLake Delta Table APIs to list schemas first, then tables within each schema. This requires Storage audience tokens (`https://storage.azure.com/.default`).
- **Schemas-Disabled Lakehouses**: The connector uses the standard Fabric REST API `/tables` endpoint, which lists all tables. Tables without an explicit schema are automatically assigned to the `dbo` schema in DataHub. This uses Power BI API scope tokens.

**Important**: All tables in DataHub will have a schema in their URN, even for schemas-disabled lakehouses. Tables without an explicit schema are normalized to use the `dbo` schema by default. This ensures consistent URN structure across all Fabric entities.

The connector automatically detects the lakehouse type and uses the appropriate API endpoint. No configuration changes are needed.

#### Stateful Ingestion

The connector supports stateful ingestion to track ingested entities and remove stale metadata. Enable it with:

```yaml
stateful_ingestion:
  enabled: true
  remove_stale_metadata: true
```

When enabled, the connector will:

- Track all ingested workspaces, lakehouses, warehouses, schemas, and tables
- Remove entities from DataHub that no longer exist in Fabric
- Maintain state across ingestion runs

### Shortcuts

Shortcut detection is enabled by default (`shortcuts.enabled`). Each lakehouse is checked against the [OneLake Shortcuts API](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/list-shortcuts). Shortcuts whose path is under `Tables` are matched to ingested tables. Those tables are tagged `shortcut` and receive these custom properties:

| Property                         | Value                                                                                   |
| -------------------------------- | --------------------------------------------------------------------------------------- |
| `shortcut_origin_name`           | Original table name at the target                                                       |
| `shortcut_origin_path`           | Target path (`Tables/<schema>/<table>` for OneLake; location plus subpath for external) |
| `shortcut_origin_workspace_id`   | Target workspace GUID (OneLake targets only)                                            |
| `shortcut_origin_workspace_name` | Target workspace display name, resolved via `GET /workspaces/{id}`                      |
| `shortcut_origin_item_id`        | Target lakehouse/item GUID (OneLake targets only)                                       |
| `shortcut_origin_item_name`      | Target item display name, resolved via `GET /workspaces/{id}/items/{id}`                |

Display names are resolved once per workspace and item. If the caller cannot read the target workspace or item, the `_name` properties are omitted and the GUIDs remain.

`shortcuts.include_lineage` (off by default) also emits an upstream dataset when the shortcut target is another OneLake table (`Tables/<schema>/<table>` or `Tables/<table>`). When that origin table is ingested in the same run and both tables have schema, each shortcut column is linked to the origin column of the same name. Shortcuts to ADLS, S3, and other external locations keep the tag and origin properties and do not get a dataset upstream. The caller needs `OneLake.Read.All` or `OneLake.ReadWrite.All`.

### Profiling

When `profiling.enabled` is `true`, the connector profiles each ingested table through that item's SQL Analytics Endpoint. The connection is `mssql+pyodbc` with the Microsoft ODBC Driver for SQL Server, and the statistics are computed by the same SQLAlchemy profiler the `mssql-odbc` source uses. Profiles are attached to the Fabric dataset URN, not a separate SQL Server dataset.

`profile_pattern` filters tables as `schema.table` and columns as `schema.table.column`, the same way `mssql-odbc` does. Column metrics (`include_field_null_count`, `include_field_distinct_count`, min/max/mean/median/stddev, quantiles, histograms, distinct value frequencies, and sample values), `profile_table_level_only`, `query_combiner_enabled`, and `max_workers` all apply. Set `turn_off_expensive_profiling_metrics` to skip quantiles, histograms, frequencies, and sample values.

`profiling.enabled` requires `sql_endpoint.enabled=true`.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

- **Metadata Sync Delays**: The SQL Analytics Endpoint may have delays in reflecting schema changes. New columns or schema modifications may take minutes to hours to appear.
- **Missing Tables**: Some tables may not be visible in the SQL endpoint due to:
  - Unsupported data types
  - Permission issues
  - Table count limits in very large databases
- **Graceful Degradation**: If schema extraction fails for a table, the table will still be ingested without column metadata (no ingestion failure)
- **View Extraction Requires SQL Endpoint**: Views are only discovered through the SQL Analytics Endpoint. If `sql_endpoint.enabled` is `false`, or if the endpoint is unreachable for a given Lakehouse/Warehouse, views in that item will not be ingested.
- **Usage Statistics Retention**: Fabric `queryinsights` retains query history for only **30 days**. Older usage cannot be backfilled, regardless of the configured `usage.start_time`.
- **Usage Statistics Requires SQL Endpoint**: Usage extraction reads `queryinsights.exec_requests_history` over the SQL Analytics Endpoint. If `sql_endpoint.enabled` is `false`, the configuration validator will reject `usage.include_usage_statistics=true`. If the endpoint is unreachable for a specific Lakehouse/Warehouse, usage for that item is skipped without failing the run.
- **Profiling Requires SQL Endpoint**: Table and column profiling queries the SQL Analytics Endpoint. If `sql_endpoint.enabled` is `false`, the configuration validator will reject `profiling.enabled=true`. If the endpoint is unreachable for a specific Lakehouse/Warehouse, profiling for that item is skipped.
- **Shortcuts Are Lakehouse Tables**: Shortcut detection lists `Tables` shortcuts on lakehouses. File shortcuts and warehouse items are not tagged. If the shortcuts API fails for a lakehouse, that lakehouse is ingested without shortcut tags.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
