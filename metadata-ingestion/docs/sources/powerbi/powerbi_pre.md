## Configuration Notes
See the 
1. [Microsoft AD App Creation doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) for the steps to create an app client ID and secret
2. Login to Power BI as Admin and from Tenant settings allow below permissions
- Allow service principles to use Power BI APIs
- Allow service principals to use read-only Power BI admin APIs
- Enhance admin APIs responses with detailed metadata
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

