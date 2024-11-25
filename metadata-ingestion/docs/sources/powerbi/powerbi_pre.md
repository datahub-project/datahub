## Configuration Notes
1. Refer [Microsoft AD App Creation doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) to create a Microsoft AD Application. Once Microsoft AD Application is created you can configure client-credential i.e. client_id and client_secret in recipe for ingestion.
2. Enable admin access only if you want to ingest dataset, lineage and endorsement tags. Refer section [Admin Ingestion vs. Basic Ingestion](#admin-ingestion-vs-basic-ingestion) for more detail. 

    Login to PowerBI as Admin and from `Admin API settings` allow below permissions

    - Allow service principals to use read-only admin APIs
    - Enhance admin APIs responses with detailed metadata
    - Enhance admin APIs responses with DAX and mashup expressions

## Concept mapping 

| PowerBI           | Datahub             |                                                                                               
|-------------------|---------------------|
| `Dashboard`       | `Dashboard`         |
| `Dataset's Table` | `Dataset`           |
| `Tile`            | `Chart`             |
| `Report.webUrl`   | `Chart.externalUrl` |
| `Workspace`       | `Container`         |
| `Report`          | `Dashboard`         |
| `PaginatedReport` | `Dashboard`         |
| `Page`            | `Chart`             |
| `App`             | `Dashboard`         |

- If `Tile` is created from report then `Chart.externalUrl` is set to Report.webUrl.
- The `Page` is unavailable for PowerBI PaginatedReport.

## Lineage

This source extract table lineage for tables present in PowerBI Datasets. Lets consider a PowerBI Dataset `SALES_REPORT` and a PostgreSQL database is configured as data-source in `SALES_REPORT` dataset. 

Consider `SALES_REPORT` PowerBI Dataset has a table `SALES_ANALYSIS` which is backed by `SALES_ANALYSIS_VIEW` of PostgreSQL Database then in this case `SALES_ANALYSIS_VIEW` will appear as upstream dataset for `SALES_ANALYSIS` table.

You can control table lineage ingestion using `extract_lineage` configuration parameter, by default it is set to `true`. 

PowerBI Source extracts the lineage information by parsing PowerBI M-Query expression.

PowerBI Source supports M-Query expression for below listed PowerBI Data Sources 

1.  Snowflake 
2.  Oracle 
3.  PostgreSQL
4.  Microsoft SQL Server
5.  Google BigQuery
6.  Databricks

Native SQL query parsing is supported for `Snowflake` and `Amazon Redshift` data-sources.

For example refer below native SQL query. The table `OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_UNIT_TARGET` will be ingested as upstream table.

```shell
let
  Source = Value.NativeQuery(
    Snowflake.Databases(
      "sdfsd788.ws-east-2.fakecomputing.com", 
      "operations_analytics_prod", 
      [Role = "OPERATIONS_ANALYTICS_MEMBER"]
    ){[Name = "OPERATIONS_ANALYTICS"]}[Data], 
    "select #(lf)UPPER(REPLACE(AGENT_NAME,\'-\',\'\')) AS Agent,#(lf)TIER,#(lf)UPPER(MANAGER),#(lf)TEAM_TYPE,#(lf)DATE_TARGET,#(lf)MONTHID,#(lf)TARGET_TEAM,#(lf)SELLER_EMAIL,#(lf)concat((UPPER(REPLACE(AGENT_NAME,\'-\',\'\'))), MONTHID) as AGENT_KEY,#(lf)UNIT_TARGET AS SME_Quota,#(lf)AMV_TARGET AS Revenue_Quota,#(lf)SERVICE_QUOTA,#(lf)BL_TARGET,#(lf)SOFTWARE_QUOTA as Software_Quota#(lf)#(lf)from OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_UNIT_TARGETS#(lf)#(lf)where YEAR_TARGET >= 2020#(lf)and TEAM_TYPE = \'foo\'#(lf)and TARGET_TEAM = \'bar\'", 
    null, 
    [EnableFolding = true]
  ), 
  #"Added Conditional Column" = Table.AddColumn(
    Source, 
    "Has PS Software Quota?", 
    each 
      if [TIER] = "Expansion (Medium)" then
        "Yes"
      else if [TIER] = "Acquisition" then
        "Yes"
      else
        "No"
  )
in
  #"Added Conditional Column"
```
Use full-table-name in `from` clause. For example dev.public.category 

## M-Query Pattern Supported For Lineage Extraction
Lets consider a M-Query which combine two PostgreSQL tables. Such M-Query can be written as per below patterns.

**Pattern-1**

```shell
let
Source = PostgreSQL.Database("localhost", "book_store"),
book_date = Source{[Schema="public",Item="book"]}[Data],
issue_history = Source{[Schema="public",Item="issue_history"]}[Data],
combine_result  = Table.Combine({book_date, issue_history})
in
combine_result
```

**Pattern-2**

```shell
let
Source = PostgreSQL.Database("localhost", "book_store"),
combine_result  = Table.Combine({Source{[Schema="public",Item="book"]}[Data], Source{[Schema="public",Item="issue_history"]}[Data]})
in
combine_result
```

`Pattern-2` is *not* supported for upstream table lineage extraction as it uses nested item-selector i.e. {Source{[Schema="public",Item="book"]}[Data], Source{[Schema="public",Item="issue_history"]}[Data]} as argument to M-QUery table function i.e. Table.Combine

`Pattern-1` is supported as it first assigns the table from schema to variable and then variable is used in M-Query Table function i.e. Table.Combine

## Extract endorsements to tags

By default, extracting endorsement information to tags is disabled. The feature may be useful if organization uses [endorsements](https://learn.microsoft.com/en-us/power-bi/collaborate-share/service-endorse-content) to identify content quality.

Please note that the default implementation overwrites tags for the ingested entities, if you need to preserve existing tags, consider using a [transformer](../../../../metadata-ingestion/docs/transformer/dataset_transformer.md#simple-add-dataset-globaltags) with `semantics: PATCH` tags instead of `OVERWRITE`.

## Profiling

The profiling implementation is done through querying [DAX query endpoint](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries). Therefore, the principal needs to have permission to query the datasets to be profiled. Usually this means that the service principal should have `Contributor` role for the workspace to be ingested. Profiling is done with column-based queries to be able to handle wide datasets without timeouts.

Take into account that the profiling implementation executes a fairly big number of DAX queries, and for big datasets this is a significant load to the PowerBI system.

The `profiling_pattern` setting may be used to limit profiling actions to only a certain set of resources in PowerBI. Both allowed and deny rules are matched against the following pattern for every table in a PowerBI Dataset: `workspace_name.dataset_name.table_name`. Users may limit profiling with these settings at table level, dataset level or workspace level.

## Admin Ingestion vs. Basic Ingestion
PowerBI provides two sets of API i.e. [Basic API and Admin API](https://learn.microsoft.com/en-us/rest/api/power-bi/). 

The Basic API returns metadata of PowerBI resources where service principal has granted access explicitly on resources,
whereas Admin API returns metadata of all PowerBI resources irrespective of whether service principal has granted
or doesn't grant access explicitly on resources.

The Admin Ingestion (explained below) is the recommended way to execute PowerBI ingestion as this ingestion can extract most of the metadata.


### Admin Ingestion: Service Principal As Admin in Tenant Setting and Added as Member In Workspace
To grant admin access to the service principal, visit your PowerBI tenant Settings.

If you have added service principal as `member` in workspace and also allowed below permissions from PowerBI tenant Settings

  - Allow service principal to use read-only PowerBI Admin APIs
  - Enhance admin APIs responses with detailed metadata
  - Enhance admin APIs responses with DAX and mashup expressions

PowerBI Source would be able to ingest below listed metadata of that particular workspace 

  - Lineage 
  - PowerBI Dataset 
  - Endorsement as tag
  - Dashboards 
  - Reports 
  - Dashboard Tiles
  - Report Pages
  - App

If you don't want to add a service principal as a member in your workspace, then you can enable the `admin_apis_only: true` in recipe to use PowerBI Admin API only. 

Caveats of setting `admin_apis_only` to `true`:
  - Report's pages would not get ingested as page API is not available in PowerBI Admin API
  - [PowerBI Parameters](https://learn.microsoft.com/en-us/power-query/power-query-query-parameters) would not get resolved to actual values while processing M-Query for table lineage
  - Dataset profiling is unavailable, as it requires access to the workspace API


### Basic Ingestion: Service Principal As Member In Workspace 
If you have added service principal as `member` in workspace then PowerBI Source would be able to ingest below metadata of that particular workspace 

  - Dashboards 
  - Reports 
  - Dashboard's Tiles
  - Report's Pages
