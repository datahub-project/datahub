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
| Tenant setting `Allow service principals to use Power BI APIs` (Developer settings)                            | Every Power BI REST call (workspaces, datasets, reports, dashboards, refresh history, workspace users).                          |
| Tenant setting `Service principals can use Fabric APIs` (Developer settings)                                   | Fabric item listing and `getDefinition` for semantic models, reports and paginated reports.                                      |
| **Contributor** role on each workspace (a security group containing the application is fine)                   | `getDefinition` and the full dataset listing need read _and_ write item permission; `Viewer` only returns dataset ids and names. |
| The same role on workspaces that host shared semantic models used by your reports                              | Report-to-dataset lineage is resolved across workspaces only for datasets the principal can read.                                |
| No encrypted (protected) sensitivity labels on the semantic models and reports, or usage rights to remove them | Fabric refuses `getDefinition` for items with an encrypted label.                                                                |
| `environment: COMMERCIAL`                                                                                      | The connector only knows the commercial Fabric API endpoint.                                                                     |

If the principal only has the `Viewer` role on a workspace, the dataset listing still returns each dataset's id and name, so the workspace, its reports and dashboards are ingested and linked to their datasets, but `getDefinition` is refused: those datasets have no tables, columns, measures or upstream lineage, and the run reports a failure for each refused semantic-model definition (a refused report definition only loses visual field usage and is a warning). Grant `Contributor` on every workspace you expect lineage from.

No Microsoft Entra application permissions need to change: both API families are authorized through the tenant settings and workspace roles above. The application requests a second token audience (`https://api.fabric.microsoft.com/.default`) in addition to the Power BI one.

What `workspace_apis` mode ingests: workspaces, dashboards, tiles, reports, report pages, datasets with tables, columns, measures and their descriptions, M expressions, table and column lineage (including DirectLake lineage to Fabric Lakehouses and Warehouses), paginated-report dataset bindings, and owners derived from workspace roles.

What stays admin-only (the connector rejects these options together with `workspace_apis`): `extract_endorsements_to_tags`, `filter_dataset_endorsements`, `extract_app`, `modified_since`, `admin_apis_only`, and per-report or per-dashboard owners (owners are the workspace's members instead, filtered by `ownership.owner_criteria` against `Admin`, `Member`, `Contributor` and `Viewer`).
