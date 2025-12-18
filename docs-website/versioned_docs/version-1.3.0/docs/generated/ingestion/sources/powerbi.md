---
sidebar_position: 52
title: PowerBI
slug: /generated/ingestion/sources/powerbi
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/powerbi.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PowerBI
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Workspace, Semantic Model. |
| Column-level Lineage | ✅ | Disabled by default, configured using `extract_column_level_lineage`. . |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration profiling.enabled. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default. |
| Extract Tags | ✅ | Enabled by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default, configured using `extract_lineage`. |
| Test Connection | ✅ | Enabled by default. |


This plugin extracts the following:
- Power BI dashboards, tiles and datasets
- Names, descriptions and URLs of dashboard and tile
- Owners of dashboards

## Configuration Notes

1. Refer [Microsoft AD App Creation doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) to create a Microsoft AD Application. Once Microsoft AD Application is created you can configure client-credential i.e. client_id and client_secret in recipe for ingestion.
2. Enable admin access if you want to ingest data source and dataset information, including lineage, and endorsement tags. Refer section [Admin Ingestion vs. Basic Ingestion](#admin-ingestion-vs-basic-ingestion) for more detail.

   Login to PowerBI as Admin and from `Admin API settings` allow below permissions

   - Allow service principals to use read-only admin APIs
   - Enhance admin APIs responses with detailed metadata
   - Enhance admin APIs responses with DAX and mashup expressions

## Concept mapping

| PowerBI           | Datahub             |
| ----------------- | ------------------- |
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

This source extracts table lineage for tables present in PowerBI Datasets. Lets consider a PowerBI Dataset `SALES_REPORT` and a PostgreSQL database is configured as data-source in `SALES_REPORT` dataset.

Consider `SALES_REPORT` PowerBI Dataset has a table `SALES_ANALYSIS` which is backed by `SALES_ANALYSIS_VIEW` of PostgreSQL Database then in this case `SALES_ANALYSIS_VIEW` will appear as upstream dataset for `SALES_ANALYSIS` table.

You can control table lineage ingestion using `extract_lineage` configuration parameter, by default it is set to `true`.

PowerBI Source extracts the lineage information by parsing PowerBI M-Query expressions and from dataset data returned by the PowerBI API.

The source will attempt to extract information from ODBC connection strings in M-Query expressions to determine the database type. If the database type matches a supported platform and the source is able to extract enough information to construct a valid Dataset URN, it will extract lineage for that data source.

PowerBI Source will extract lineage for the below listed PowerBI Data Sources:

1.  Snowflake
2.  Oracle
3.  PostgreSQL
4.  Microsoft SQL Server
5.  Google BigQuery
6.  Databricks
7.  MySQL

Native SQL query parsing is supported for `Snowflake`, `Amazon Redshift`, and ODBC data sources.

For example, consider the SQL query shown below. The table `OPERATIONS_ANALYTICS.TRANSFORMED_PROD.V_UNIT_TARGET` will be ingested as an upstream table.

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

`Pattern-2` is _not_ supported for upstream table lineage extraction as it uses nested item-selector i.e. {Source{[Schema="public",Item="book"]}[Data], Source{[Schema="public",Item="issue_history"]}[Data]} as argument to M-QUery table function i.e. Table.Combine

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

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "powerbi"
  config:
    # Your Power BI tenant identifier
    tenant_id: a949d688-67c0-4bf1-a344-e939411c6c0a

    # Azure AD Application identifier
    client_id: foo
    # Azure AD App client secret
    client_secret: bar
    
    # Ingest elements of below PowerBi Workspace into Datahub
    workspace_name_pattern:
      allow:
        - MyWorkspace
      deny:
        - PrivateWorkspace

    # Enable / Disable ingestion of ownership information for dashboards
    extract_ownership: true
    
    # Enable/Disable extracting workspace information to DataHub containers
    extract_workspaces_to_containers: true
    
    # Enable / Disable ingestion of endorsements.
    # Please notice that this may overwrite any existing tags defined to ingested entities!
    extract_endorsements_to_tags: false
    
    # Optional -- This mapping is optional and only required to configure platform-instance for upstream tables
    # A mapping of PowerBI datasource's server i.e host[:port] to data platform instance.
    # :port is optional and only needed if your datasource server is running on non-standard port.
    # For Google BigQuery the datasource's server is google bigquery project name
    server_to_platform_instance:
        ap-south-1.snowflakecomputing.com:
          platform_instance: operational_instance
          env: DEV
        oracle-server:1920:
          platform_instance: high_performance_production_unit
          env: PROD
        big-query-sales-project:
          platform_instance: sn-2
          env: QA

    # Need admin_api, only ingest workspace that are modified since...
    modified_since: "2023-02-10T00:00:00.0000000Z"

    ownership:
        # create powerbi user as datahub corpuser, false will still extract ownership of workspace/ dashboards
        create_corp_user: false
        # use email to build user urn instead of powerbi user identifier
        use_powerbi_email: true
        # remove email suffix like @acryl.io
        remove_email_suffix: true
        # only ingest user with certain authority
        owner_criteria: ["ReadWriteReshareExplore","Owner","Admin"]
    # wrap powerbi tables (datahub dataset) under 1 powerbi dataset (datahub container)
    extract_datasets_to_containers: true
    # only ingest dataset that are endorsed, like "Certified"
    filter_dataset_endorsements: 
      allow:
        - Certified
      
    # extract powerbi dashboards and tiles
    extract_dashboards: false
    # extract powerbi dataset table schema
    extract_dataset_schema: true

    # Enable PowerBI dataset profiling
    profiling:
      enabled: false
    # Pattern to limit which resources to profile
    # Matched resource format is following:
    # workspace_name.dataset_name.table_name
    profile_pattern:
      deny:
        - .*


sink:
  # sink configs

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">client_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Azure app client identifier  |
| <div className="path-line"><span className="path-main">client_secret</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Azure app client secret  |
| <div className="path-line"><span className="path-main">tenant_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | PowerBI tenant identifier  |
| <div className="path-line"><span className="path-main">admin_apis_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Retrieve metadata using PowerBI Admin API only. If this is enabled, then Report Pages will not be extracted. Admin API access is required if this setting is enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">convert_lineage_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert the urns of ingested lineage dataset to lowercase <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert the PowerBI assets urns to lowercase <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">dsn_to_database_schema</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">dsn_to_platform_name</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">enable_advance_lineage_sql_construct</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to enable advance native sql construct for parsing like join, sub-queries. along this flag , the native_query_parsing should be enabled. By default convert_lineage_urns_to_lowercase is enabled, in-case if you have disabled it in previous ingestion execution then it may break lineageas this option generates the upstream datasets URN in lowercase. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_app</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest workspace app. Requires DataHub server 0.14.2+. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract column level lineage. Works only if configs `native_query_parsing`, `enable_advance_lineage_sql_construct` & `extract_lineage` are enabled.Works for M-Query where native SQL is used for transformation. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_dashboards</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest PBI Dashboard and Tiles as Datahub Dashboard and Chart <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_dataset_schema</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest PBI Dataset Table columns and measures. Note: this setting must be `true` for schema extraction and column lineage to be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_datasets_to_containers</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | PBI tables will be grouped under a Datahub Container, the container reflect a PBI Dataset <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_endorsements_to_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract endorsements to tags, note that this may overwrite existing tags. Admin API access is required if this setting is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_independent_datasets</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract datasets not used in any PowerBI visualization <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether lineage should be ingested between X and Y. Admin API access is required if this setting is enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether ownership should be ingested. Admin API access is required if this setting is enabled. Note that enabling this may overwrite owners that you've added inside DataHub's web application. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_reports</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether reports should be ingested <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_workspaces_to_containers</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract workspaces to DataHub containers <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_workspace_name_in_dataset_urn</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | It is recommended to set this to true, as it helps prevent the overwriting of datasets.Read section #11560 at https://docs.datahub.com/docs/how/updating-datahub/ before enabling this option.To maintain backward compatibility, this is set to False. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">m_query_parse_timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Timeout for PowerBI M-query parsing in seconds. Table-level lineage is determined by analyzing the M-query expression. Increase this value if you encounter the 'M-Query Parsing Timeout' message in the connector report. <div className="default-line default-line-with-docs">Default: <span className="default-value">70</span></div> |
| <div className="path-line"><span className="path-main">modified_since</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Get only recently modified workspaces based on modified_since datetime '2023-02-10T00:00:00.0000000Z', excludeInActiveWorkspaces limit to last 30 days <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">native_query_parsing</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether PowerBI native query should be parsed to extract lineage <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">patch_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Patch dashboard metadata <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">scan_batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | batch size for sending workspace_ids to PBI, 100 is the limit <div className="default-line default-line-with-docs">Default: <span className="default-value">1</span></div> |
| <div className="path-line"><span className="path-main">scan_timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | timeout for PowerBI metadata scanning <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-main">workspace_id_as_urn_part</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | It is recommended to set this to True only if you have legacy workspaces based on Office 365 groups, as those workspaces can have identical names. To maintain backward compatibility, this is set to False which uses workspace name <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">filter_dataset_endorsements</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">ownership</span></div> <div className="type-name-line"><span className="type-name">OwnershipMapping</span></div> |   |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">create_corp_user</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether ingest PowerBI user as Datahub Corpuser <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">dataset_configured_by_as_owner</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Take PBI dataset configuredBy as dataset owner if exist <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">remove_email_suffix</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Remove PowerBI User email suffix for example, @acryl.io <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">use_powerbi_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use PowerBI User email to ingest as corpuser, default is powerbi user identifier <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">owner_criteria</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Need to have certain authority to qualify as owner for example ['ReadWriteReshareExplore','Owner','Admin'] <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.owner_criteria.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">server_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of map(str,union), map(str,union)</span></div> |   |
| <div className="path-line"><span className="path-prefix">server_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null, union(anyOf), string, null</span></div> | DataHub platform instance name. To generate correct urn for upstream dataset, this should match with platform instance name used in ingestion recipe of other datahub sources. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">server_to_platform_instance.`key`.</span><span className="path-main">metastore</span>&nbsp;<abbr title="Required if server_to_platform_instance is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Databricks Unity Catalog metastore name.  |
| <div className="path-line"><span className="path-prefix">server_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by DataHub platform ingestion source belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">workspace_id_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workspace_id_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">workspace_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workspace_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">workspace_type_filter</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Ingest the metadata of the workspace where the workspace type corresponds to the specified workspace_type_filter. Note: This field works in conjunction with 'workspace_id_pattern'. Both must be matched for a workspace to be processed. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;Workspace&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">workspace_type_filter.</span><span className="path-main">enum</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "Workspace", "PersonalGroup", "Personal", "AdminWorkspace", "AdminInsights"  |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">PowerBiProfilingConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling of PowerBI datasets should be done <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | PowerBI Stateful Ingestion Config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "DataBricksPlatformDetail": {
      "additionalProperties": false,
      "description": "metastore is an additional field used in Databricks connector to generate the dataset urn",
      "properties": {
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "DataHub platform instance name. To generate correct urn for upstream dataset, this should match with platform instance name used in ingestion recipe of other datahub sources.",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "The environment that all assets produced by DataHub platform ingestion source belong to",
          "title": "Env",
          "type": "string"
        },
        "metastore": {
          "description": "Databricks Unity Catalog metastore name.",
          "title": "Metastore",
          "type": "string"
        }
      },
      "required": [
        "metastore"
      ],
      "title": "DataBricksPlatformDetail",
      "type": "object"
    },
    "OwnershipMapping": {
      "additionalProperties": false,
      "properties": {
        "create_corp_user": {
          "default": true,
          "description": "Whether ingest PowerBI user as Datahub Corpuser",
          "title": "Create Corp User",
          "type": "boolean"
        },
        "use_powerbi_email": {
          "default": true,
          "description": "Use PowerBI User email to ingest as corpuser, default is powerbi user identifier",
          "title": "Use Powerbi Email",
          "type": "boolean"
        },
        "remove_email_suffix": {
          "default": false,
          "description": "Remove PowerBI User email suffix for example, @acryl.io",
          "title": "Remove Email Suffix",
          "type": "boolean"
        },
        "dataset_configured_by_as_owner": {
          "default": false,
          "description": "Take PBI dataset configuredBy as dataset owner if exist",
          "title": "Dataset Configured By As Owner",
          "type": "boolean"
        },
        "owner_criteria": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Need to have certain authority to qualify as owner for example ['ReadWriteReshareExplore','Owner','Admin']",
          "title": "Owner Criteria"
        }
      },
      "title": "OwnershipMapping",
      "type": "object"
    },
    "PlatformDetail": {
      "additionalProperties": false,
      "properties": {
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "DataHub platform instance name. To generate correct urn for upstream dataset, this should match with platform instance name used in ingestion recipe of other datahub sources.",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "The environment that all assets produced by DataHub platform ingestion source belong to",
          "title": "Env",
          "type": "string"
        }
      },
      "title": "PlatformDetail",
      "type": "object"
    },
    "PowerBiProfilingConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether profiling of PowerBI datasets should be done",
          "title": "Enabled",
          "type": "boolean"
        }
      },
      "title": "PowerBiProfilingConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "PowerBI Stateful Ingestion Config."
    },
    "tenant_id": {
      "description": "PowerBI tenant identifier",
      "title": "Tenant Id",
      "type": "string"
    },
    "workspace_id_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter PowerBI workspaces in ingestion by ID. By default all IDs are allowed unless they are filtered by name using 'workspace_name_pattern'. Note: This field works in conjunction with 'workspace_type_filter' and both must be considered when filtering workspaces."
    },
    "workspace_name_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter PowerBI workspaces in ingestion by name. By default all names are allowed unless they are filtered by ID using 'workspace_id_pattern'. Note: This field works in conjunction with 'workspace_type_filter' and both must be considered when filtering workspaces."
    },
    "server_to_platform_instance": {
      "additionalProperties": {
        "anyOf": [
          {
            "$ref": "#/$defs/PlatformDetail"
          },
          {
            "$ref": "#/$defs/DataBricksPlatformDetail"
          }
        ]
      },
      "default": {},
      "description": "A mapping of PowerBI datasource's server i.e host[:port] to Data platform instance. :port is optional and only needed if your datasource server is running on non-standard port. For Google BigQuery the datasource's server is google bigquery project name. For Databricks Unity Catalog the datasource's server is workspace FQDN.",
      "title": "Server To Platform Instance",
      "type": "object"
    },
    "dsn_to_platform_name": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "A mapping of ODBC DSN to DataHub data platform name. For example with an ODBC connection string 'DSN=database' where the database type is 'PostgreSQL' you would configure the mapping as 'database: postgres'.",
      "title": "Dsn To Platform Name",
      "type": "object"
    },
    "dsn_to_database_schema": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "A mapping of ODBC DSN to database names with optional schema names (some database platforms such a MySQL use the table name pattern 'database.table', while others use the pattern 'database.schema.table'). This mapping is used in conjunction with ODBC SQL query parsing. If SQL queries used with ODBC do not reference fully qualified tables names, then you should configure mappings for your DSNs. For example with an ODBC connection string 'DSN=database' where the database is 'prod' you would configure the mapping as 'database: prod'. If the database is 'prod' and the schema is 'data' then mapping would be 'database: prod.data'.",
      "title": "Dsn To Database Schema",
      "type": "object"
    },
    "client_id": {
      "description": "Azure app client identifier",
      "title": "Client Id",
      "type": "string"
    },
    "client_secret": {
      "description": "Azure app client secret",
      "title": "Client Secret",
      "type": "string"
    },
    "scan_timeout": {
      "default": 60,
      "description": "timeout for PowerBI metadata scanning",
      "title": "Scan Timeout",
      "type": "integer"
    },
    "scan_batch_size": {
      "default": 1,
      "description": "batch size for sending workspace_ids to PBI, 100 is the limit",
      "exclusiveMinimum": 0,
      "maximum": 100,
      "title": "Scan Batch Size",
      "type": "integer"
    },
    "workspace_id_as_urn_part": {
      "default": false,
      "description": "It is recommended to set this to True only if you have legacy workspaces based on Office 365 groups, as those workspaces can have identical names. To maintain backward compatibility, this is set to False which uses workspace name",
      "title": "Workspace Id As Urn Part",
      "type": "boolean"
    },
    "extract_ownership": {
      "default": false,
      "description": "Whether ownership should be ingested. Admin API access is required if this setting is enabled. Note that enabling this may overwrite owners that you've added inside DataHub's web application.",
      "title": "Extract Ownership",
      "type": "boolean"
    },
    "extract_reports": {
      "default": true,
      "description": "Whether reports should be ingested",
      "title": "Extract Reports",
      "type": "boolean"
    },
    "ownership": {
      "$ref": "#/$defs/OwnershipMapping",
      "default": {
        "create_corp_user": true,
        "use_powerbi_email": true,
        "remove_email_suffix": false,
        "dataset_configured_by_as_owner": false,
        "owner_criteria": null
      },
      "description": "Configure how is ownership ingested"
    },
    "modified_since": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Get only recently modified workspaces based on modified_since datetime '2023-02-10T00:00:00.0000000Z', excludeInActiveWorkspaces limit to last 30 days",
      "title": "Modified Since"
    },
    "extract_dashboards": {
      "default": true,
      "description": "Whether to ingest PBI Dashboard and Tiles as Datahub Dashboard and Chart",
      "title": "Extract Dashboards",
      "type": "boolean"
    },
    "extract_dataset_schema": {
      "default": true,
      "description": "Whether to ingest PBI Dataset Table columns and measures. Note: this setting must be `true` for schema extraction and column lineage to be enabled.",
      "title": "Extract Dataset Schema",
      "type": "boolean"
    },
    "extract_lineage": {
      "default": true,
      "description": "Whether lineage should be ingested between X and Y. Admin API access is required if this setting is enabled",
      "title": "Extract Lineage",
      "type": "boolean"
    },
    "extract_endorsements_to_tags": {
      "default": false,
      "description": "Whether to extract endorsements to tags, note that this may overwrite existing tags. Admin API access is required if this setting is enabled.",
      "title": "Extract Endorsements To Tags",
      "type": "boolean"
    },
    "filter_dataset_endorsements": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Filter and ingest datasets which are 'Certified' or 'Promoted' endorsement. If both are added, dataset which are 'Certified' or 'Promoted' will be ingested . Default setting allows all dataset to be ingested"
    },
    "extract_workspaces_to_containers": {
      "default": true,
      "description": "Extract workspaces to DataHub containers",
      "title": "Extract Workspaces To Containers",
      "type": "boolean"
    },
    "extract_datasets_to_containers": {
      "default": false,
      "description": "PBI tables will be grouped under a Datahub Container, the container reflect a PBI Dataset",
      "title": "Extract Datasets To Containers",
      "type": "boolean"
    },
    "native_query_parsing": {
      "default": true,
      "description": "Whether PowerBI native query should be parsed to extract lineage",
      "title": "Native Query Parsing",
      "type": "boolean"
    },
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert the PowerBI assets urns to lowercase",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
    "convert_lineage_urns_to_lowercase": {
      "default": true,
      "description": "Whether to convert the urns of ingested lineage dataset to lowercase",
      "title": "Convert Lineage Urns To Lowercase",
      "type": "boolean"
    },
    "admin_apis_only": {
      "default": false,
      "description": "Retrieve metadata using PowerBI Admin API only. If this is enabled, then Report Pages will not be extracted. Admin API access is required if this setting is enabled",
      "title": "Admin Apis Only",
      "type": "boolean"
    },
    "extract_independent_datasets": {
      "default": false,
      "description": "Whether to extract datasets not used in any PowerBI visualization",
      "title": "Extract Independent Datasets",
      "type": "boolean"
    },
    "enable_advance_lineage_sql_construct": {
      "default": true,
      "description": "Whether to enable advance native sql construct for parsing like join, sub-queries. along this flag , the native_query_parsing should be enabled. By default convert_lineage_urns_to_lowercase is enabled, in-case if you have disabled it in previous ingestion execution then it may break lineageas this option generates the upstream datasets URN in lowercase.",
      "title": "Enable Advance Lineage Sql Construct",
      "type": "boolean"
    },
    "extract_column_level_lineage": {
      "default": false,
      "description": "Whether to extract column level lineage. Works only if configs `native_query_parsing`, `enable_advance_lineage_sql_construct` & `extract_lineage` are enabled.Works for M-Query where native SQL is used for transformation.",
      "title": "Extract Column Level Lineage",
      "type": "boolean"
    },
    "profile_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter tables for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered. Matched format is 'workspacename.datasetname.tablename'"
    },
    "profiling": {
      "$ref": "#/$defs/PowerBiProfilingConfig",
      "default": {
        "enabled": false
      }
    },
    "patch_metadata": {
      "default": true,
      "description": "Patch dashboard metadata",
      "title": "Patch Metadata",
      "type": "boolean"
    },
    "workspace_type_filter": {
      "default": [
        "Workspace"
      ],
      "description": "Ingest the metadata of the workspace where the workspace type corresponds to the specified workspace_type_filter. Note: This field works in conjunction with 'workspace_id_pattern'. Both must be matched for a workspace to be processed.",
      "items": {
        "enum": [
          "Workspace",
          "PersonalGroup",
          "Personal",
          "AdminWorkspace",
          "AdminInsights"
        ],
        "type": "string"
      },
      "title": "Workspace Type Filter",
      "type": "array"
    },
    "include_workspace_name_in_dataset_urn": {
      "default": false,
      "description": "It is recommended to set this to true, as it helps prevent the overwriting of datasets.Read section #11560 at https://docs.datahub.com/docs/how/updating-datahub/ before enabling this option.To maintain backward compatibility, this is set to False.",
      "title": "Include Workspace Name In Dataset Urn",
      "type": "boolean"
    },
    "extract_app": {
      "default": false,
      "description": "Whether to ingest workspace app. Requires DataHub server 0.14.2+.",
      "title": "Extract App",
      "type": "boolean"
    },
    "m_query_parse_timeout": {
      "default": 70,
      "description": "Timeout for PowerBI M-query parsing in seconds. Table-level lineage is determined by analyzing the M-query expression. Increase this value if you encounter the 'M-Query Parsing Timeout' message in the connector report.",
      "title": "M Query Parse Timeout",
      "type": "integer"
    }
  },
  "required": [
    "tenant_id",
    "client_id",
    "client_secret"
  ],
  "title": "PowerBiDashboardSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.powerbi.powerbi.PowerBiDashboardSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for PowerBI, feel free to ping us on [our Slack](https://datahub.com/slack).
