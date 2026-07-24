


# PowerBI

## Overview

Microsoft Power BI is a business intelligence and analytics platform. Learn more in the [official Microsoft Power BI documentation](https://powerbi.microsoft.com/).

The DataHub integration for Microsoft Power BI covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table- and column-level lineage, data profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

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


## Module `powerbi`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Workspace, Semantic Model. |
| Column-level Lineage | ✅ | Enabled by default, configured using `extract_column_level_lineage`. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration profiling.enabled. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default. |
| Extract Tags | ✅ | Enabled by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default, configured using `extract_lineage`. |
| Test Connection | ✅ | Enabled by default. |

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


### Install the Plugin
```shell
pip install 'acryl-datahub[powerbi]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "powerbi"
  config:
    # Your Power BI tenant identifier
    tenant_id: a949d688-67c0-4bf1-a344-e939411c6c0a

    # Microsoft Entra Application identifier
    client_id: 12345678-abcd-abcd-abcd-123456789012
    # Microsoft Entra Application client secret value
    client_secret: Abc12d~efg3hijkl_45Abcdefg67aBcdef89
    
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
    # For Google BigQuery the datasource's server is google bigquery project name.
    # For Fabric OneLake (DirectLake lineage), use PowerBI workspace ID as the key.
    # The platform_instance typically represents the Fabric tenant identifier, matching how
    # the OneLake connector uses platform_instance to group workspaces by tenant.
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
        # Fabric OneLake: workspace ID -> (platform_instance, env)
        # platform_instance is typically the Fabric tenant identifier
        ff23fbe3-7418-42f8-a675-9f10eb2b78cb:  # PowerBI workspace ID
          platform_instance: contoso-tenant  # Fabric tenant/platform instance
          env: PROD

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

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">client_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Azure app client identifier  |
| <div className="path-line"><span className="path-main">client_secret</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Azure app client secret  |
| <div className="path-line"><span className="path-main">tenant_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | PowerBI tenant identifier  |
| <div className="path-line"><span className="path-main">admin_apis_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Retrieve metadata using PowerBI Admin API only. If this is enabled, then Report Pages will not be extracted. Admin API access is required if this setting is enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">app_url_pattern</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "WORKSPACE_BASED", "REDIRECT_BASED"  |
| <div className="path-line"><span className="path-main">convert_lineage_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert the urns of ingested lineage dataset to lowercase <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert the PowerBI assets urns to lowercase <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">dsn_to_database_schema</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">dsn_to_platform_name</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">enable_advance_lineage_sql_construct</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to enable advance native sql construct for parsing like join, sub-queries. along this flag , the native_query_parsing should be enabled. By default convert_lineage_urns_to_lowercase is enabled, in-case if you have disabled it in previous ingestion execution then it may break lineageas this option generates the upstream datasets URN in lowercase. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">environment</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "COMMERCIAL", "GOVERNMENT"  |
| <div className="path-line"><span className="path-main">extract_app</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest workspace app. Requires DataHub server 0.14.2+. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract column level lineage. Works only if configs `native_query_parsing`, `enable_advance_lineage_sql_construct` & `extract_lineage` are enabled. Works for M-Query where native SQL is used for transformation. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
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
| <div className="path-line"><span className="path-main">athena_table_platform_override</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of platform overrides for Athena federated queries. Use this to override the platform when Athena queries data from federated sources (e.g., MySQL, PostgreSQL) via ODBC. The lineage will point to the actual source platform instead of Athena. This override is applied AFTER catalog stripping, so use 2-part names (database.table), not 3-part names (catalog.database.table). Overrides with a DSN specified take precedence over those without. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">athena_table_platform_override.</span><span className="path-main">AthenaPlatformOverride</span></div> <div className="type-name-line"><span className="type-name">AthenaPlatformOverride</span></div> | Configuration for overriding the platform of Athena federated tables. <br />  <br /> Use this when Athena queries data from federated sources (e.g., MySQL, PostgreSQL) <br /> and you want the lineage to point to the actual source platform instead of Athena.  |
| <div className="path-line"><span className="path-prefix">athena_table_platform_override.AthenaPlatformOverride.</span><span className="path-main">database</span>&nbsp;<abbr title="Required if AthenaPlatformOverride is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The database name in the Athena query (after catalog stripping).  |
| <div className="path-line"><span className="path-prefix">athena_table_platform_override.AthenaPlatformOverride.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if AthenaPlatformOverride is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The target DataHub platform name (e.g., 'mysql', 'postgres').  |
| <div className="path-line"><span className="path-prefix">athena_table_platform_override.AthenaPlatformOverride.</span><span className="path-main">table</span>&nbsp;<abbr title="Required if AthenaPlatformOverride is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The table name in the Athena query.  |
| <div className="path-line"><span className="path-prefix">athena_table_platform_override.AthenaPlatformOverride.</span><span className="path-main">dsn</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional DSN to scope this override to a specific data source. If specified, this override only applies when the query comes from this DSN. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">filter_dataset_endorsements</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">filter_dataset_endorsements.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">ownership</span></div> <div className="type-name-line"><span className="type-name">OwnershipMapping</span></div> |   |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">create_corp_user</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to create user entities from PowerBI data. When False (RECOMMENDED): PowerBI emits ownership URNs only (soft references). User profiles must come from LDAP/SCIM/Okta. When True (OPT-IN): PowerBI creates users with displayName and email from PowerBI. WARNING: May overwrite existing user profiles from other sources. Use only if PowerBI is your authoritative user source. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">dataset_configured_by_as_owner</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Take PBI dataset configuredBy as dataset owner if exist <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">remove_email_suffix</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Remove PowerBI User email suffix for example, @acryl.io <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">use_powerbi_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use PowerBI User email to ingest as corpuser, default is powerbi user identifier <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.</span><span className="path-main">owner_criteria</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Need to have certain authority to qualify as owner for example ['ReadWriteReshareExplore','Owner','Admin'] <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">ownership.owner_criteria.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">server_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of map(str,union), map(str,union), map(str,union)</span></div> |   |
| <div className="path-line"><span className="path-prefix">server_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null, union(anyOf), string, null, union(anyOf), string, null</span></div> | DataHub platform instance name. To generate correct urn for upstream dataset, this should match with platform instance name used in ingestion recipe of other datahub sources. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">server_to_platform_instance.`key`.</span><span className="path-main">metastore</span>&nbsp;<abbr title="Required if server_to_platform_instance is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Databricks Unity Catalog metastore name.  |
| <div className="path-line"><span className="path-prefix">server_to_platform_instance.`key`.</span><span className="path-main">default_database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Database segment prepended to the table name when the ``Oracle.Database`` connection is a bare TNS alias or descriptor (which carries no database). Set this to match the database segment your Oracle ingestion uses, only when that ingestion emits 3-part ``database.schema.table`` URNs (``add_database_name_to_urn: true``); leave unset for the default 2-part URNs and for EZ-Connect ``host:port/service`` connections. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">server_to_platform_instance.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Owner/schema applied to unqualified table references inside ``Oracle.Database(…, Query="…")`` inline native SQL, so they resolve to your ingested Oracle datasets. Not used by hierarchical navigation. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">server_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, string</span></div> | The environment that all assets produced by DataHub platform ingestion source belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
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




#### Schema


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
    "AthenaPlatformOverride": {
      "additionalProperties": false,
      "description": "Configuration for overriding the platform of Athena federated tables.\n\nUse this when Athena queries data from federated sources (e.g., MySQL, PostgreSQL)\nand you want the lineage to point to the actual source platform instead of Athena.",
      "properties": {
        "database": {
          "description": "The database name in the Athena query (after catalog stripping).",
          "minLength": 1,
          "title": "Database",
          "type": "string"
        },
        "table": {
          "description": "The table name in the Athena query.",
          "minLength": 1,
          "title": "Table",
          "type": "string"
        },
        "platform": {
          "description": "The target DataHub platform name (e.g., 'mysql', 'postgres').",
          "minLength": 1,
          "title": "Platform",
          "type": "string"
        },
        "dsn": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Optional DSN to scope this override to a specific data source. If specified, this override only applies when the query comes from this DSN.",
          "title": "Dsn"
        }
      },
      "required": [
        "database",
        "table",
        "platform"
      ],
      "title": "AthenaPlatformOverride",
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
    "OraclePlatformDetail": {
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
        },
        "default_schema": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Owner/schema applied to unqualified table references inside ``Oracle.Database(\u2026, Query=\"\u2026\")`` inline native SQL, so they resolve to your ingested Oracle datasets. Not used by hierarchical navigation.",
          "title": "Default Schema"
        },
        "default_database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Database segment prepended to the table name when the ``Oracle.Database`` connection is a bare TNS alias or descriptor (which carries no database). Set this to match the database segment your Oracle ingestion uses, only when that ingestion emits 3-part ``database.schema.table`` URNs (``add_database_name_to_urn: true``); leave unset for the default 2-part URNs and for EZ-Connect ``host:port/service`` connections.",
          "title": "Default Database"
        }
      },
      "title": "OraclePlatformDetail",
      "type": "object"
    },
    "OwnershipMapping": {
      "additionalProperties": false,
      "properties": {
        "create_corp_user": {
          "default": true,
          "description": "Whether to create user entities from PowerBI data. When False (RECOMMENDED): PowerBI emits ownership URNs only (soft references). User profiles must come from LDAP/SCIM/Okta. When True (OPT-IN): PowerBI creates users with displayName and email from PowerBI. WARNING: May overwrite existing user profiles from other sources. Use only if PowerBI is your authoritative user source.",
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
    "PowerBiAppUrlPattern": {
      "enum": [
        "WORKSPACE_BASED",
        "REDIRECT_BASED"
      ],
      "title": "PowerBiAppUrlPattern",
      "type": "string"
    },
    "PowerBiEnvironment": {
      "enum": [
        "COMMERCIAL",
        "GOVERNMENT"
      ],
      "title": "PowerBiEnvironment",
      "type": "string"
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
    "environment": {
      "$ref": "#/$defs/PowerBiEnvironment",
      "default": "COMMERCIAL",
      "description": "PowerBI environment to connect to. Options: 'commercial' (default) for commercial PowerBI, 'government' for PowerBI Government Community Cloud (GCC)"
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
            "$ref": "#/$defs/OraclePlatformDetail"
          },
          {
            "$ref": "#/$defs/DataBricksPlatformDetail"
          },
          {
            "$ref": "#/$defs/PlatformDetail"
          }
        ]
      },
      "default": {},
      "description": "Mapping from a PowerBI datasource server to the DataHub platform instance (and env) of its upstream tables, so lineage URNs match your other DataHub sources. The key is the server as it appears in the M-query, i.e. `host[:port]` (`:port` only for non-standard ports); for Google BigQuery it is the project name, for Databricks Unity Catalog the workspace FQDN, and for Oracle the EZ-Connect host, bare TNS alias, or descriptor SERVICE_NAME (case-insensitive). The value is a platform-detail object; Oracle servers may add `default_schema`/`default_database` and Databricks servers `metastore`.",
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
    "athena_table_platform_override": {
      "default": [],
      "description": "List of platform overrides for Athena federated queries. Use this to override the platform when Athena queries data from federated sources (e.g., MySQL, PostgreSQL) via ODBC. The lineage will point to the actual source platform instead of Athena. This override is applied AFTER catalog stripping, so use 2-part names (database.table), not 3-part names (catalog.database.table). Overrides with a DSN specified take precedence over those without.",
      "items": {
        "$ref": "#/$defs/AthenaPlatformOverride"
      },
      "title": "Athena Table Platform Override",
      "type": "array"
    },
    "client_id": {
      "description": "Azure app client identifier",
      "title": "Client Id",
      "type": "string"
    },
    "client_secret": {
      "description": "Azure app client secret",
      "format": "password",
      "title": "Client Secret",
      "type": "string",
      "writeOnly": true
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
      "default": true,
      "description": "Whether to extract column level lineage. Works only if configs `native_query_parsing`, `enable_advance_lineage_sql_construct` & `extract_lineage` are enabled. Works for M-Query where native SQL is used for transformation.",
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
    "app_url_pattern": {
      "$ref": "#/$defs/PowerBiAppUrlPattern",
      "default": "WORKSPACE_BASED",
      "description": "URL pattern for Power BI App external links. 'workspace_based' uses /groups/{workspace-id}/apps/{app-id} (default). 'redirect_based' uses /Redirect?action=OpenApp&appId={app-id}."
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





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### User and Ownership Handling

PowerBI Source supports two modes for handling user ownership:

##### Soft References Mode (Recommended - Default)

When `ownership.create_corp_user: false` (default), PowerBI will:

- Extract ownership information as URN references only
- NOT create user entities in DataHub
- User profiles must come from your identity provider (LDAP/SCIM/Okta)

This is the recommended approach as it prevents PowerBI from overwriting user profiles from your identity provider.

```yaml
ownership:
  create_corp_user: false # Default - soft references only
```

##### Full User Creation Mode (Opt-in)

When `ownership.create_corp_user: true`, PowerBI will:

- Create user entities with `displayName` and `email` from PowerBI
- This may overwrite existing user profiles from LDAP/Okta/SCIM

**Warning**: Only use this if PowerBI is your authoritative source for user information.

```yaml
ownership:
  create_corp_user: true # Opt-in - creates user entities
```

##### Filtering Owners by Access Rights

You can limit which users become owners using `owner_criteria`. Only users with at least one of the specified access rights will be assigned as owners:

```yaml
ownership:
  owner_criteria:
    - ReadWriteReshareExplore
    - Owner
    - Admin
```

Valid values depend on the PowerBI access right types for your resources (e.g., dataset, report, dashboard). If `owner_criteria` is not set or is an empty list, all users with `principalType: User` qualify as owners.

#### Admin scan vs. workspace listing

PowerBI ingestion uses two metadata paths per workspace:

| Path                       | When                                                                               | What it fetches                                                                                                                            | API volume                         |
| -------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------- |
| Admin scan                 | The configured principal has admin-API access and the workspace scan returns data. | Reports, users, datasets, lineage, endorsements — all from a single workspace scan response.                                               | 1 scan request per workspace batch |
| Workspace listing fallback | The admin scan returns no data for a workspace (permissions, throttling, etc.).    | Reports via the per-workspace `/reports` endpoint; users via `/admin/reports/{id}/users` per report (only when `extract_ownership: true`). | O(workspaces + reports) requests   |

When the fallback engages, the ingestion report includes a **Report Scan Fallback Active** entry per workspace so you can correlate that workspace's slower ingestion with the missing scan output.

#### Lineage

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
8.  Amazon Redshift
9.  Amazon Athena

Native SQL query parsing is supported for `Snowflake`, `Amazon Redshift`, `Oracle`, and ODBC data sources.

#### Oracle TNS Aliases and Inline Native Queries

PowerBI users can connect to Oracle via a TNS alias (any `tnsnames.ora`-based deployment) and embed SQL inline using a `Query=` argument:

```text
let
  Source = Oracle.Database(
    "EDWPSFN",
    [HierarchicalNavigation = true,
     Query = "SELECT … FROM PS_VENDOR, PS_COR_CNTRCT_PROJ …"])
in
  Source
```

Three Oracle.Database first-argument forms are recognized:

1. EZ-Connect: `host:port/service[.domain]`
2. Bare TNS alias: e.g. `EDWPSFN`, `MYDB.WORLD`
3. Full TNS descriptor: `(DESCRIPTION=(ADDRESS=…)(CONNECT_DATA=(SERVICE_NAME=foo)))`

For the bare-alias and descriptor forms, the alias / SERVICE_NAME becomes the `server` key for `server_to_platform_instance` lookup. The lookup is case-insensitive, so `EDWPSFN` in the M-Query matches an `EDWPSFN` (or `edwpsfn`) entry in the recipe.

##### Matching the Oracle URN shape (database segment)

The generated upstream URN must match the URN that your Oracle ingestion produces for the same table, otherwise the lineage edge points at a dataset that does not exist. Oracle ingestion emits **2-part `schema.table`** URNs by default, and **3-part `database.schema.table`** URNs only when it runs with `add_database_name_to_urn: true`.

PowerBI derives the database segment from the connection form:

| Oracle.Database form           | Database segment              |
| ------------------------------ | ----------------------------- |
| EZ-Connect `host:port/service` | the `service` (always 3-part) |
| Bare TNS alias / descriptor    | none — 2-part by default      |

So a bare TNS alias produces a 2-part URN, which is correct for a default Oracle ingestion. If your Oracle ingestion uses `add_database_name_to_urn: true`, set `default_database` on the alias entry to supply the missing segment (use the same value as the Oracle ingestion's `database` / `urn_db_name`):

```yaml
source:
  type: powerbi
  config:
    server_to_platform_instance:
      EDWPSFN:
        default_database: edwprd # only if Oracle ingestion uses 3-part URNs
        default_schema: sysadm # owner schema for unqualified inline-SQL tables
        # platform_instance: EDWPSFN  # only set this if your Oracle ingestion uses one
```

##### Qualifying unqualified inline SQL (`default_schema`)

Because inline `Query=` SQL often references unqualified tables, declare a `default_schema` on the alias entry so those references resolve to the ingested Oracle datasets. `default_schema` only applies to inline native SQL — hierarchical navigation takes the schema from the M-Query itself.

If `default_schema` is missing and the inline SQL references unqualified tables, lineage will still be drawn for any qualified tables in the SQL, and a structured warning will appear in the ingestion report telling you exactly which alias needs the knob set.

At least one of `default_schema` / `default_database` must be set for an Oracle entry; a mapping that needs neither should be a plain `platform_instance` / `env` entry.

#### Athena Federated Query Platform Override

When using Amazon Athena via ODBC that queries federated data sources (e.g., Athena querying MySQL or PostgreSQL via federated connectors), the lineage URNs will default to the Athena platform. Use `athena_table_platform_override` to point lineage to the actual source platform instead of Athena.

**Configuration:**

```yaml
source:
  type: powerbi
  config:
    # ... other config ...
    dsn_to_platform_name:
      MyAthenaDSN: athena
    athena_table_platform_override:
      # DSN-scoped key (takes precedence)
      "MyAthenaDSN:analytics.users": mysql
      # Global key (fallback for any DSN)
      "reporting.orders": postgres
```

**Key format:**

- **DSN-scoped**: `"DSN_NAME:database.table"` - applies only to specific DSN
- **Global**: `"database.table"` - applies to all DSNs

DSN-scoped keys take precedence over global keys, allowing different overrides for the same table name across different Athena data sources.

**Note:** This override only applies to Athena ODBC connections. For other ODBC platforms, lineage will use the platform determined from the DSN configuration.

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

#### M-Query Pattern Supported For Lineage Extraction

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

#### Extract endorsements to tags

By default, extracting endorsement information to tags is disabled. The feature may be useful if organization uses [endorsements](https://learn.microsoft.com/en-us/power-bi/collaborate-share/service-endorse-content) to identify content quality.

Please note that the default implementation overwrites tags for the ingested entities, if you need to preserve existing tags, consider using a [transformer](../../../../metadata-ingestion/docs/transformer/dataset_transformer.md#simple-add-dataset-globaltags) with `semantics: PATCH` tags instead of `OVERWRITE`.

#### Profiling

The profiling implementation is done through querying [DAX query endpoint](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries). Therefore, the principal needs to have permission to query the datasets to be profiled. Usually this means that the service principal should have `Contributor` role for the workspace to be ingested. Profiling is done with column-based queries to be able to handle wide datasets without timeouts.

Take into account that the profiling implementation executes a fairly big number of DAX queries, and for big datasets this is a significant load to the PowerBI system.

The `profiling_pattern` setting may be used to limit profiling actions to only a certain set of resources in PowerBI. Both allowed and deny rules are matched against the following pattern for every table in a PowerBI Dataset: `workspace_name.dataset_name.table_name`. Users may limit profiling with these settings at table level, dataset level or workspace level.

### Limitations

- Some metadata and lineage fields are only available through admin APIs or specific tenant settings.
- Lineage quality depends on available model metadata and supported query/source patterns.
- Large tenants with many workspaces can require longer extraction windows.

### Troubleshooting

- **Authentication failures**: verify `tenant_id`, `client_id`, and `client_secret`, and confirm the app has the required Power BI API permissions.
- **Missing workspaces/assets**: check service principal access to target workspaces or enable the required admin API mode/settings.
- **Lineage gaps**: confirm lineage-related config is enabled and that semantic models expose supported upstream source details.


### Code Coordinates
- Class Name: `datahub.ingestion.source.powerbi.powerbi.PowerBiDashboardSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/powerbi/powerbi.py)


:::tip Questions?

If you've got any questions on configuring ingestion for PowerBI, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
