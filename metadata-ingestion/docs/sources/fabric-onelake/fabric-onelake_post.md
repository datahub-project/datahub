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
   - For **Warehouses**, the tables themselves are also discovered here, from `INFORMATION_SCHEMA.TABLES` (`TABLE_TYPE = 'BASE TABLE'`, excluding the `sys`, `INFORMATION_SCHEMA`, and `queryinsights` schemas). See [Warehouse Table Discovery](#warehouse-table-discovery).
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
4. **Lineage**: View definitions are passed to the SQL parsing aggregator to derive table- and column-level view → upstream lineage. Unqualified and `schema.table` references resolve to the view's own item; `item.schema.table` and `workspace.item.schema.table` references resolve to other items — see [Cross-Item and Cross-Workspace Lineage](#cross-item-and-cross-workspace-lineage).

#### Cross-Item and Cross-Workspace Lineage

Fabric SQL lets a view or query reference tables in other Lakehouses / Warehouses by **display name**, while DataHub dataset URNs are keyed by GUIDs (`<workspaceId>.<itemId>.<schema>.<table>`). The connector translates display names to GUIDs so lineage lands on the real datasets, for both view definitions and observed queries from `queryinsights` (e.g. Warehouse `INSERT ... SELECT` or `CREATE TABLE ... AS SELECT` that read a Lakehouse):

| Reference in SQL                                           | Resolved against                                    |
| ---------------------------------------------------------- | --------------------------------------------------- |
| `customers`, `dbo.customers`                               | The item that owns the view / ran the query         |
| `silver_lh.dbo.customers`, `[Silver_LH].[dbo].[customers]` | An item named `silver_lh` in the **same workspace** |
| `[Shared Data].[ref_lh].[dbo].[regions]` (4-part)          | Item `ref_lh` in the workspace named `Shared Data`  |

- Item and workspace names are matched case-insensitively, with or without `[...]` / `"..."` quoting. Item and workspace GUIDs are accepted in place of names.
- The name index is built from the Lakehouse / Warehouse listing of every workspace allowed by `workspace_pattern`, before any item is processed. Items excluded by `lakehouse_pattern` / `warehouse_pattern` are still indexed, so references to them resolve to their (GUID-based) URNs. Item types turned off with `extract_lakehouses: false` / `extract_warehouses: false` are not listed, so references to them are not resolved.
- Column-level lineage is produced for cross-item references. Column metadata of ingested tables and views is registered with the SQL parser, so `SELECT *` expands and lineage confidence is high when the referenced item's columns were extracted.
- The original SQL text is kept unchanged in view definitions and Query entities.
- A 3-part name is only looked up in the referencing workspace, so identically named items in other workspaces (e.g. dev / test / prod workspaces) do not collide.

A reference that cannot be resolved — the item or workspace is not ingested, does not exist, its name is ambiguous (e.g. two items with the same display name in one workspace), listing the workspace's items failed, or the name has more than four parts — is **skipped** instead of producing a dangling URN: it is removed from lineage, usage, operations, and query subjects, and an `Unresolved Cross-Item SQL Reference` warning with the reference, workspace, and reason is added to the ingestion report. The report also counts `num_cross_item_references_resolved` / `num_cross_item_references_unresolved` (distinct references), `num_lineage_upstreams_dropped_unresolved` (upstream edges removed), and `num_lineage_aspects_dropped_unresolved` (lineage aspects dropped because every upstream was unresolved).

#### Usage Statistics

The connector extracts query usage statistics from each Lakehouse and Warehouse by reading the [`queryinsights.exec_requests_history`](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-insights) view on the SQL Analytics Endpoint. Each captured query is parsed by the SQL parsing aggregator and emitted as:

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

      # When true, queries issued by the identity the connector connects as
      # (the connector's own metadata reads and the ODBC driver's catalog
      # procedures) are skipped. See "Filtered Queries" below.
      skip_ingestion_identity_queries: true

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

##### Filtered Queries and System Objects

`queryinsights` records every statement run on a SQL Analytics Endpoint, including statements that are not user activity. The connector skips them before SQL parsing and counts each skip by reason in `num_usage_queries_skipped`:

| Reason                 | What is skipped                                                                                                                                                                                                                                                                                                                                                                                   |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ingestion_identity`   | Rows whose `login_name` matches the identity the connector connects as (`SUSER_SNAME()`). These are the connector's own `INFORMATION_SCHEMA` / `queryinsights` reads and the catalog procedures the ODBC driver runs for them. Controlled by `usage.skip_ingestion_identity_queries` (default `true`); set it to `false` if the same identity also runs workloads you want usage and lineage for. |
| `procedural_statement` | T-SQL control-of-flow and session statements (`SET`, `IF`, `DECLARE`, `WHILE`, `EXEC`, `BEGIN TRANSACTION`, `COMMIT`, ...), such as the bodies of the ODBC driver's `sp_*` catalog procedures (`set @ODBCVer = 3`). They carry no table lineage and cannot be parsed; DML inside a procedure is recorded as its own row and is kept.                                                              |
| `user_filtered`        | Rows denied by `usage.user_email_pattern`.                                                                                                                                                                                                                                                                                                                                                        |
| `empty_command`        | Rows with no query text.                                                                                                                                                                                                                                                                                                                                                                          |

References to system objects — tables and views in the `sys`, `INFORMATION_SCHEMA`, and `queryinsights` schemas — never produce datasets, lineage, usage, operations, or query subjects, whether they appear in a view definition or an observed query. Queries that also touch user tables keep their lineage and usage for those tables. The report counts `num_system_object_references_filtered` (distinct objects, listed in `filtered_system_objects`) and `num_lineage_upstreams_dropped_system_objects` / `num_lineage_aspects_dropped_system_objects`.

#### Warehouse Table Discovery

The [Fabric REST Tables API](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/list-tables) is available for Lakehouses only; the [Warehouse REST API](https://learn.microsoft.com/en-us/rest/api/fabric/warehouse/items) has no operation that lists tables. The connector therefore discovers Warehouse tables (including tables created with T-SQL `CREATE TABLE` / `CREATE TABLE AS SELECT`) through the Warehouse's SQL Analytics Endpoint, by querying `INFORMATION_SCHEMA.TABLES`. These tables get column-level schema from `INFORMATION_SCHEMA.COLUMNS` and are registered with the SQL parser, so views over them get column-level lineage.

This requires `sql_endpoint.enabled: true` (the default). If the endpoint is disabled or cannot be reached for a Warehouse, the connector falls back to the REST API, which returns 404 for Warehouses: that Warehouse's tables are not ingested, a `Warehouse Tables Not Discovered` warning explaining the fix is added to the report, and `num_warehouses_without_table_discovery` is incremented. The report field `num_warehouse_tables_discovered_via_sql_endpoint` counts the tables found through the endpoint.

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

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

- **Metadata Sync Delays**: The SQL Analytics Endpoint may have delays in reflecting schema changes. New columns or schema modifications may take minutes to hours to appear.
- **Missing Tables**: Some tables may not be visible in the SQL endpoint due to:
  - Unsupported data types
  - Permission issues
  - Table count limits in very large databases
- **Graceful Degradation**: If schema extraction fails for a table, the table will still be ingested without column metadata (no ingestion failure)
- **Warehouse Tables Require SQL Endpoint**: Warehouse tables are only discoverable through the SQL Analytics Endpoint (`INFORMATION_SCHEMA.TABLES`), because the Fabric REST Tables API is Lakehouse-only. With `sql_endpoint.enabled: false`, or if the endpoint is unreachable, a Warehouse's tables are not ingested and a warning is reported; lineage from its views then points at tables without schema metadata.
- **View Extraction Requires SQL Endpoint**: Views are only discovered through the SQL Analytics Endpoint. If `sql_endpoint.enabled` is `false`, or if the endpoint is unreachable for a given Lakehouse/Warehouse, views in that item will not be ingested.
- **Cross-Item References Require the Referenced Workspace**: `item.schema.table` and `workspace.item.schema.table` references resolve only to Lakehouses / Warehouses in workspaces ingested by the same recipe (allowed by `workspace_pattern`). References to other workspaces, to other item types (e.g. SQL databases, mirrored databases), are not resolved and are reported as warnings. A table that is a OneLake shortcut resolves to the shortcut table in the referencing item, not to the shortcut's target. With `platform_instance`, all resolved URNs use the recipe's platform instance.
- **Usage Statistics Retention**: Fabric `queryinsights` retains query history for only **30 days**. Older usage cannot be backfilled, regardless of the configured `usage.start_time`.
- **Usage Statistics Requires SQL Endpoint**: Usage extraction reads `queryinsights.exec_requests_history` over the SQL Analytics Endpoint. If `sql_endpoint.enabled` is `false`, the configuration validator will reject `usage.include_usage_statistics=true`. If the endpoint is unreachable for a specific Lakehouse/Warehouse, usage for that item is skipped without failing the run.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
