## Configuration Notes
1. Refer [Microsoft AD App Creation doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) to create a Microsoft AD Application and allow service principal to use Power BI APIs
2. Enable admin access only if you want to ingest dataset, lineage and endorsement tags. Refer section [Admin Access Vs Data Ingestion](#admin-access-vs-data-ingestion) for more detail. 

    Login to Power BI as Admin and from `Admin API settings` allow below permissions

    - Allow service principals to use read-only admin APIs
    - Enhance admin APIs responses with detailed metadata
    - Enhance admin APIs responses with DAX and mashup expressions

## Concept mapping 

| Power BI              | Datahub                 |                                                                                               
|-----------------------|---------------------|
| `Dashboard`           | `Dashboard`         |
| `Dataset's Table`     | `Dataset`           |
| `Tile`                | `Chart`             |
| `Report.webUrl`       | `Chart.externalUrl` |
| `Workspace`           | `N/A`               |
| `Report`              | `Dashboard`         |
| `Page`                | `Chart`             |

If Tile is created from report then Chart.externalUrl is set to Report.webUrl.

## Lineage

This source extract table lineage for tables present in Power BI Datasets. Lets consider a PowerBI Dataset `SALES_REPORT` and a PostgreSQL database is configured as data-source in `SALES_REPORT` dataset. 

Consider `SALES_REPORT` PowerBI Dataset has a table `SALES_ANALYSIS` which is backed by `SALES_ANALYSIS_VIEW` of PostgreSQL Database then in this case `SALES_ANALYSIS_VIEW` will appear as upstream dataset for `SALES_ANALYSIS` table.

You can control table lineage ingestion using `extract_lineage` configuration parameter, by default it is set to `true`. 

PowerBI Source extracts the lineage information by parsing PowerBI M-Query expression.

PowerBI Source supports M-Query expression for below listed PowerBI Data Sources 

1.  Snowflake 
2.  Oracle 
3.  PostgreSQL
4.  Microsoft SQL Server

Native SQL query parsing is only supported for `Snowflake` data-source and only first table from `FROM` clause will be ingested as upstream table. Advance SQL construct like JOIN and SUB-QUERIES in `FROM` clause are not supported.

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

`Pattern-1` is supported as it first assign the table from schema to variable and then variable is used in M-Query Table function i.e. Table.Combine

## Extract endorsements to tags

By default, extracting endorsement information to tags is disabled. The feature may be useful if organization uses [endorsements](https://learn.microsoft.com/en-us/power-bi/collaborate-share/service-endorse-content) to identify content quality.

Please note that the default implementation overwrites tags for the ingested entities, if you need to preserve existing tags, consider using a [transformer](../../../../metadata-ingestion/docs/transformer/dataset_transformer.md#simple-add-dataset-globaltags) with `semantics: PATCH` tags instead of `OVERWRITE`.

## Admin Access vs Data Ingestion
### Service Principal As Member In Workspace 
If you have added Service Principal as `member` in workspace then PowerBI Source would be able ingest below metadata of that particular workspace 

  - Dashboards 
  - Reports 
  - Dashboard's Tiles
  - Report's Pages

### Service Principal As Admin in Tenant Setting and Added as Member In Workspace
If you have added Service Principal as `member` in workspace and also allowed below permissions  

  - Allow service principal to use read-only PowerBI Admin APIs
  - Enhance admin APIs responses with detailed metadata

PowerBI Source would be able to ingest below listed metadata of that particular workspace 

  - Lineage 
  - PowerBI Dataset 
  - Endorsement as tag
  - Dashboards 
  - Reports 
  - Dashboard's Tiles
  - Report's Pages

### Service Principal As Admin in Tenant Setting
In this scenario Service Principal is only added in tenant setting and not added in individual workspaces.

Service Principal is allowed below permissions

  - Allow service principal to use read-only PowerBI Admin APIs
  - Enhance admin APIs responses with detailed metadata

In PowerBI Source recipe add flag `admin_apis_only: true` to configure PowerBI Source to use PowerBI Admin APIs only to retrieve the meta-data. PowerBI Source would be able to ingest below listed metadata of all workspaces.

  - Lineage 
  - PowerBI Dataset 
  - Endorsement as tag
  - Dashboards 
  - Reports 
  - Dashboard's Tiles

Report's Pages are not available in PowerBI Admin API.
