### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

##### Virtual Connections

[Tableau Virtual Connections](https://help.tableau.com/current/online/en-us/dm_vconn_overview.htm) are shared data sources that provide a centralised, governed view of underlying tables. DataHub ingests them as datasets and creates lineage back to the physical tables they reference, including column-level lineage where available.

Virtual connection ingestion is enabled by default. To disable it, set `ingest_virtual_connections: false` in your recipe.

##### Lineage

Lineage is emitted as received from Tableau's metadata API for

- Sheets contained within a Dashboard
- Embedded or Published Data Sources depended on by a Sheet
- Published Data Sources upstream to Embedded datasource
- Tables upstream to Embedded or Published Data Source
- Custom SQL datasources upstream to Embedded or Published Data Source
- Tables upstream to Custom SQL Data Source

##### Initial SQL

Some Tableau data sources are configured with an "Initial SQL" statement — SQL that Tableau runs when first opening a connection. DataHub can extract this SQL and use it to enrich the data source's metadata.

This feature is experimental. Enable it by setting `ingest_initial_sql: true` in your recipe. Because it requires **downloading each published data source, and each workbook** that contains embedded data sources, via separate REST API calls, it is disabled by default to avoid performance overhead on large Tableau sites.

When enabled, two output modes are available (both on by default):

- `initial_sql_as_lineage` (default `true`): parses the Initial SQL and emits any referenced tables as upstream lineage of the data source. Statements that do not produce lineage (e.g. session-level `SET` commands) are silently skipped.
- `initial_sql_as_custom_property` (default `true`): stores the raw Initial SQL text as the `initialSql` custom property on the data source dataset.

Both **published** and **embedded** (workbook) data sources are supported. Published data sources are read from their downloaded definition; embedded data sources have no standalone download, so their Initial SQL is read from the parent workbook's downloaded definition and matched to the data source by name. An embedded data source whose name cannot be unambiguously matched in its workbook is skipped and counted in the `num_initial_sql_embedded_datasources_unmatched` report metric.

##### Tables Without Column Metadata

In some cases, the Tableau Metadata API may not return column information for upstream tables (i.e., `columnsConnection.totalCount` is null or 0). This can occur due to:

- Permissions limitations
- Tableau's internal metadata collection issues
- Specific database connector behaviors

DataHub will still create **table-level lineage** for these tables, even though column-level lineage cannot be generated. This ensures that upstream table relationships remain visible in lineage graphs.

**Observability**: The ingestion report tracks these tables using the counter `num_upstream_table_processed_without_columns`.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

- Initial SQL ingestion (`ingest_initial_sql`) supports both published and embedded (workbook) data sources. It is disabled by default because it issues a download request per published data source and per workbook, which can be slow on sites with many data sources or workbooks.

- Tableau metadata API might return incorrect schema name for tables for some databases, leading to incorrect metadata in DataHub. This source attempts to extract correct schema from databaseTable's fully qualified name, wherever possible. Read [Using the databaseTable object in query](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute) for caveats in using schema attribute.

### Troubleshooting

#### Why are only some workbooks/custom SQLs/published datasources ingested from the specified project?

This may happen when the Tableau API returns NODE_LIMIT_EXCEEDED error in response to metadata query and returns partial results with message "Showing partial results. , The request exceeded the ‘n’ node limit. Use pagination, additional filtering, or both in the query to adjust results." To resolve this, consider

- reducing the page size using the `page_size` config param in datahub recipe (Defaults to 10).
- increasing tableau configuration [metadata query node limit](https://help.tableau.com/current/server/en-us/cli_configuration-set_tsm.htm#metadata_nodelimit) to higher value.

#### `PERMISSIONS_MODE_SWITCHED` error in ingestion report

This error occurs if the Tableau site is using external assets. For more detail, refer to the Tableau documentation [Manage Permissions for External Assets](https://help.tableau.com/current/online/en-us/dm_perms_assets.htm).

Follow the below steps to enable the derived permissions:

1.  Sign in to Tableau Cloud or Tableau Server as an admin.
2.  From the left navigation pane, click Settings.
3.  On the General tab, under Automatic Access to Metadata about Databases and Tables, select the `Automatically grant authorized users access to metadata about databases and tables` check box.

#### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
