### Overview

The `powerbi` module ingests metadata from Powerbi into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Power BI dashboards, tiles and datasets
- Names, descriptions and URLs of dashboard and tile
- Owners of dashboards

### Prerequisites

In order to execute this source, you will need to have a Microsoft Entra Application service principal and grant permissions to it inside Power BI.

[Power BI's APIs](https://learn.microsoft.com/en-us/rest/api/power-bi/) can be categorized into two sets of API methods, with different permission structures:

- Public APIs are designed for developers to interact with specific resources within a tenant, and require the Entra application to be explicitly granted access to individual Workspaces.
- The Admin APIs are designed for administrators to interact with the entire Power BI tenant at a high level, and return metadata on all Power BI resources.

The recommended way to execute Power BI ingestion is to do both: add your Entra application to the workspaces you want to ingest, andgrant it access to the public _and_ Admin APIs. That way ingestion can extract the most metadata.

#### Public APIs ingestion

To grant public API access to your Entra application:

1. **Grant permissions to access Fabric public APIs:** Add your Entra Application's parent Entra Group under your Power BI/Fabric tenant settings in order to grant API access.

   a. In Power BI or Fabric, go to `Settings` -> `Admin portal`

   b. In the `Admin portal`, navigate to `Tenant settings`

   d. Under `Developer Settings`, enable the option `Service principals can call Fabric Public APIs` (or `Allow service principals to use Power BI APIs` in older versions of Power BI), and add your application's Entra group under `Specific security groups`.

2. **Add your Entra application as a member of your Power BI workspaces:** For workspaces which you want to ingest into DataHub, add the Entra application as a member. For most cases `Viewer` role is enough, but for profiling the `Contributor` role is required.

If you have granted your Entra application permissions to the public APIs and added it as a member in a workspace, then the Power BI Source will be able to ingest the below metadata of that particular workspace:

- Dashboards
- Dashboard Tiles
- Reports
- Report Pages

If you don't want to add an Entra application as a member in your workspace, then you can enable `admin_apis_only: true` in your recipe to use the Power BI Admin API only. Caveats of setting `admin_apis_only` to `true`:

- Report Pages will not get ingested as the page API is not available in the Power BI Admin API
- [Power BI Parameters](https://learn.microsoft.com/en-us/power-query/power-query-query-parameters) will not get resolved to actual values while processing M-Query for table lineage
- Dataset profiling is unavailable, as it requires access to the non-admin workspace API

#### Admin APIs ingestion

To grant admin API access to the Entra application:

1. **Grant permissions to access Admin APIs:** Add your Entra Application's parent Entra Group under your Power BI/Fabric tenant settings in order to grant API access.

   a. In Power BI or Fabric, go to `Settings` -> `Admin portal`

   b. In the `Admin portal`, navigate to `Tenant settings`

   d. For each of the following options, enable the option and add your Entra application's Group under `Specific security groups`:

   - `Service principals can access read-only admin APIs`
   - `Enhance admin APIs responses with detailed metadata`
   - `Enhance admin APIs responses with DAX and mashup expressions`

If you have granted your Entra application permissions to the Admin APIs, then the Power BI Source will be able to ingest the below listed metadata of that particular workspace:

- Lineage
- Datasets
- Endorsement as tag
- Dashboards
- Dashboard Tiles
- Reports
- Report Pages
- App

#### Workspace APIs ingestion (no admin API access)

If granting tenant-level admin API access is not an option, set `metadata_api_mode: workspace_apis`. In this mode the connector never calls the Admin Scanner API; semantic-model schemas, M expressions and lineage come from the Fabric item-definition APIs (TMDL for semantic models, PBIR for reports) and ownership comes from workspace role assignments.

Requirements, all scoped to the workspaces you ingest:

| Requirement                                                                                                    | Why                                                                                                                              |
| -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Tenant setting `Allow service principals to use Power BI APIs` (Developer settings)                            | Every Power BI REST call (workspaces, datasets, reports, dashboards, refresh history and schedule, workspace users).             |
| Tenant setting `Service principals can use Fabric APIs` (Developer settings)                                   | Fabric item listing and `getDefinition` for semantic models, reports and paginated reports.                                      |
| **Contributor** role on each workspace (a security group containing the application is fine)                   | `getDefinition` and the full dataset listing need read _and_ write item permission; `Viewer` only returns dataset ids and names. |
| The same role on workspaces that host shared semantic models used by your reports                              | Report-to-dataset lineage is resolved across workspaces only for datasets the principal can read.                                |
| No encrypted (protected) sensitivity labels on the semantic models and reports, or usage rights to remove them | Fabric refuses `getDefinition` for items with an encrypted label.                                                                |
| `environment: COMMERCIAL`                                                                                      | The connector only knows the commercial Fabric API endpoint.                                                                     |

If the principal only has the `Viewer` role on a workspace, the dataset listing still returns each dataset's id and name, so the workspace, its reports and dashboards are ingested and linked to their datasets, but `getDefinition` is refused: those datasets have no tables, columns, measures or upstream lineage, and the run reports a failure for each refused semantic-model definition (a refused report definition only loses visual field usage and is a warning). Grant `Contributor` on every workspace you expect lineage from.

No Microsoft Entra application permissions need to change: both API families are authorized through the tenant settings and workspace roles above. The application requests a second token audience (`https://api.fabric.microsoft.com/.default`) in addition to the Power BI one.

What `workspace_apis` mode ingests: workspaces, dashboards, tiles, reports, report pages, datasets with tables, columns, measures and their descriptions, M expressions, table and column lineage (including DirectLake lineage to Fabric Lakehouses and Warehouses), paginated-report dataset bindings, and owners derived from workspace roles. With `extract_report_field_usage` enabled, each page chart also records the model fields its visuals read, and report-level and page-level filters are included as `filters` / `filterCount` properties on the report and its pages, with the filtered fields added to the page's `inputFields`. A filter's rendered condition includes the literal values it compares against (for example the selected categories or thresholds), so treat these properties as potentially containing business data.

In both metadata API modes a calculated column (one defined by a DAX expression) gets no column-level edge to the upstream table. In `workspace_apis` mode column lineage additionally follows each column's TMDL `sourceColumn`, so a field renamed in the model still points at the right column of the upstream table; a calculated table always gets table-level lineage from the model tables its DAX reads, and a measure that reads columns of other tables always gets column-level edges from them. Set `extract_dax_reference_lineage: true` to also derive lineage from the rest of the DAX: a calculated column gets column-level edges from the columns and measures its expression reads, and a measure that references another measure gets an edge from it. Edges are only emitted to tables and fields that exist in the parsed model. A DAX reference that does not resolve is counted in the report and surfaced as a `DAX Reference Not Resolved` warning. DAX is only read from TMDL definitions, so this applies to `workspace_apis` mode; without the flag only the table-qualified references of measures and the tables read by calculated tables are checked. Enabling the flag makes the connector write `upstreamLineage` on tables that previously had none (for example a measures-only table whose measures read each other), which replaces any upstreams authored for those tables in the UI. The flag requires `extract_column_level_lineage`.

Set `extract_model_properties: true` to also ingest the model properties the TMDL definition carries. Each schema field gets `isPartOfKey` from `isKey`, `nullable` from an explicit `isNullable` (an explicit `isNullable: false` is the same as the aspect's default), and a JSON object in its `jsonProps` with the rest: `isHidden` (only written when true), `formatString`, `displayFolder`, `dataCategory`, `sortByColumn` and `summarizeBy` (omitted when it is the Desktop default `none`) for columns, and `isHidden`, `formatString`, `displayFolder` and `hasKpi` for measures. Each table's custom properties gain `storageMode` (`Import`, `DirectQuery`, `Dual`, `DirectLake` or `Push`), `partitionCount`, `incrementalRefresh: true` with the `refreshPolicy.rollingWindowGranularity`, `refreshPolicy.rollingWindowPeriods`, `refreshPolicy.incrementalGranularity` and `refreshPolicy.incrementalPeriods` settings when the table has a refresh policy, the row-level-security roles that filter the table as `rlsRoles` (a JSON list of `{role, filter}`) with `rlsRoleCount`, and every dataset parameter as `parameter.<name>` with its current value cut to 1000 characters. The RLS `filter` is the role's DAX exactly as authored, so it can contain hard-coded user names or emails if the model's author wrote them into the filter; a role filtering a table that is not in the model is reported as an `RLS Filter Table Not Found` warning. A parameter whose name contains `secret`, `token`, `password`, `pwd`, `pass`, `credential`, `key`, `auth`, `connection`, `sas`, `signature`, `cert` or `private` (substring match, so over-masking is the safe direction) is emitted with the value `***`. Role members are identities and are never ingested; a role file that cannot be parsed is reported as a `TMDL Role Not Parsed` warning and does not fail the model. The flag requires `extract_dataset_schema`.

Composite models (`DirectQuery to Power BI semantic model`) are ingested like any other model; with `extract_composite_model_lineage: true` their remote tables are also linked to the tables of the semantic model they read: table-level lineage to the source table and, with `extract_column_level_lineage`, column-level lineage for every column the remote table mirrors, matched by the column's source name so a column renamed on the composite model still resolves (a column calculated on the composite model is covered by the DAX lineage instead). The source model is matched by dataset id when the connection carries one, otherwise by workspace and model name, and only against the semantic models ingested in the same run: the source model's workspace must be in the workspace pattern and readable by the principal with the roles above. A source model that was not ingested (or whose definition was refused) is reported as `Composite Model Source Not Ingested`, a name that matches more than one ingested model as `Composite Model Source Ambiguous` (rename one of them, or republish the composite model so its connection carries the dataset id), and a table or column missing from the source model as `Composite Model Source Table Not Found` / `Composite Model Column Not Resolved`; each case gets no edge rather than a guessed one, with matching `composite_model_*` counters in the report. Tables that read any other Analysis Services server are ingested without lineage. The flag requires `extract_lineage`.

With `extract_report_visuals` additionally enabled (it requires `extract_report_field_usage`), every visual on a report page becomes its own Chart entity with subtype `PowerBI Visual`, listed on the report next to its page charts. Each visual chart carries `inputFields` for exactly the model columns and measures that visual reads, its title (or `<visualType> on <page>` when the visual has none), a chart type where the Power BI visual type maps to one (bar, line, area, pie, scatter, table, text), the raw `visualType`, and its visual-level filters as `filters` / `filterCount` properties. Visual charts are keyed by the visual's stable name in the report definition, so renaming a visual's title keeps its URN, and visuals removed from a report are soft-deleted by stateful ingestion like any other entity. Page charts are unchanged. Expect one chart per visual: a report with many visuals produces correspondingly more entities. Visual groups (containers that only arrange other visuals) are skipped. Because the report's chart list is patched add-only when `patch_metadata` is `true`, a visual removed from the report, or all visuals when you turn the flag off again, stays listed on the report dashboard although its chart is soft-deleted; run the connector once with `patch_metadata: false` after such a change so the list is rewritten.

**Dataflow Gen2.** With `extract_dataflows: true` the connector also reads each workspace's Dataflow Gen2 items through `getDefinition` (one call per dataflow; the same `Contributor` role applies). Each dataflow becomes a container in its workspace and each of its load-enabled queries a dataset with subtype `Dataflow Entity`, carrying the query's M expression. Upstream lineage is parsed from that M expression exactly as for semantic-model tables (Snowflake, SQL Server, Databricks, Fabric and the other supported sources), including through helper queries that are not loaded themselves. Semantic-model tables that read a dataflow through `PowerPlatform.Dataflows` or `PowerBI.Dataflows` get an upstream edge to the matching dataflow dataset, but only when that dataflow was ingested in the same run: a reference to a dataflow in a workspace outside `workspace_id_pattern` is reported as a `Dataflow Not Ingested` warning instead of an edge. Dataflow datasets have no schema, so there is no column-level lineage for them, and dataflow mashup parameters are not resolved: a source such as `Sql.Database(#"Server", ...)` whose server or database is a parameter yields no upstream edge.

What stays admin-only (the connector rejects these options together with `workspace_apis`): `extract_endorsements_to_tags`, `filter_dataset_endorsements`, `extract_app`, `modified_since`, `admin_apis_only`, and per-report or per-dashboard owners (owners are the workspace's members instead, filtered by `ownership.owner_criteria` against `Admin`, `Member`, `Contributor` and `Viewer`).

#### Enriching the admin scan with item definitions

If you keep the default `metadata_api_mode: admin_scan` (for endorsements, apps, `modified_since` or per-item owners) but also want what only the item definitions carry, set `enrich_from_item_definitions: true`. After the admin scan has built each workspace's datasets, the connector reads every semantic model's TMDL definition (and, with `extract_report_field_usage`, every report's definition) through the Fabric item APIs and overlays onto the scanned tables: each column's `sourceColumn` for column lineage, calculated-column and measure DAX references, table, column and measure descriptions, relationships, model properties, composite-model connections (for `extract_composite_model_lineage`) and report field usage.

The scan stays the source of truth for which tables and columns exist. A table that is only in the definition is not added and is reported as a `Definition Table Not In Scan` warning; a scanned table that is not in the definition keeps its scan metadata and is reported as `Scan Table Not In Definition`. A definition that cannot be fetched or parsed is an `Item Definition Not Fetched` (or `TMDL Table Not Parsed`) warning, never a failure: the scan already provided the entities, so only the enrichment is lost.

The requirements are the same as for `workspace_apis` mode: the tenant setting `Service principals can use Fabric APIs` and the **Contributor** role on each ingested workspace (the admin API access the scan needs stays as it is). The flag cannot be combined with `admin_apis_only` (getDefinition needs workspace membership) or with `metadata_api_mode: workspace_apis` (which always reads the definitions), and needs `environment: COMMERCIAL`. It unlocks `extract_model_relationships`, `extract_report_field_usage`, `extract_dax_reference_lineage` and `extract_model_properties` in `admin_scan` mode, each with the behavior described above.
