### Overview

The `powerbi` module ingests metadata from Powerbi into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Power BI dashboards, tiles and datasets
- Names, descriptions and URLs of dashboard and tile
- Owners of dashboards

### Prerequisites

In order to execute this source, you will need to have a Microsoft Entra Application service principal with permissions granted to it for reading metadata from Power BI's APIs.

[Power BI's APIs](https://learn.microsoft.com/en-us/rest/api/power-bi/) can be categorized into two sets of API methods, with different permissions:

- Basic APIs only return metadata of Power BI resources where the Entra application has been explicitly granted access.
- The Admin APIs return metadata of all Power BI resources irrespective of whether the application was granted explicit access.

The recommended way to execute Power BI ingestion is to do both: add your Entra application to the workspaces you want to ingest, as well as granting it access to the Admin APIs (explained below). That way ingestion can extract the most metadata.

#### Basic Ingestion

To grant basic API access to your Entra application:

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

If you don't want to add an Entra application as a member in your workspace, then you can enable the `admin_apis_only: true` in recipe to use the Power BI Admin API only.

Caveats of setting `admin_apis_only` to `true`:

- Reports' pages will not get ingested as the page API is not available in the Power BI Admin API
- [Power BI Parameters](https://learn.microsoft.com/en-us/power-query/power-query-query-parameters) will not get resolved to actual values while processing M-Query for table lineage
- Dataset profiling is unavailable, as it requires access to the non-admin workspace API

#### Admin Ingestion

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
