


# Tableau

## Overview

Tableau is a business intelligence and analytics platform. Learn more in the [official Tableau documentation](https://www.tableau.com/).

The DataHub integration for Tableau covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table- and column-level lineage, usage statistics, ownership, tags, and stateful deletion detection.

:::info Ingestion through UI

The following video shows you how to get started with ingesting Tableau metadata through the UI.

[▶ Watch video](https://www.loom.com/share/ef521c4e66564614a6ddde35dc3840f8)
:::

## Concept Mapping

| Source Concept              | DataHub Concept                                               | Notes                             |
| --------------------------- | ------------------------------------------------------------- | --------------------------------- |
| `"Tableau"`                 | [Data Platform](../../metamodel/entities/dataPlatform.md)     |
| Project                     | [Container](../../metamodel/entities/container.md)            | SubType `"Project"`               |
| Embedded DataSource         | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Embedded Data Source"`  |
| Published DataSource        | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Published Data Source"` |
| Custom SQL Table            | [Dataset](../../metamodel/entities/dataset.md)                | SubTypes `"View"`, `"Custom SQL"` |
| Embedded or External Tables | [Dataset](../../metamodel/entities/dataset.md)                |                                   |
| Sheet                       | [Chart](../../metamodel/entities/chart.md)                    |                                   |
| Dashboard                   | [Dashboard](../../metamodel/entities/dashboard.md)            |                                   |
| User                        | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted              |
| Workbook                    | [Container](../../metamodel/entities/container.md)            | SubType `"Workbook"`              |
| Tag                         | [Tag](../../metamodel/entities/tag.md)                        | Optionally Extracted              |


## Module `tableau`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Project, Site, Workbook. |
| Column-level Lineage | ✅ | Enabled by default, configure using `extract_column_level_lineage`. |
| Dataset Usage | ✅ | Dashboard/Chart view counts, enabled using extract_usage_stats config. Supported for types - Dashboard, Chart. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ❌ | Requires transformer. |
| Extract Ownership | ✅ | Requires recipe configuration. |
| Extract Tags | ✅ | Requires recipe configuration. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `tableau` module ingests metadata from Tableau into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

Extracts metadata for Sheets, Dashboards, Workbooks, and Data Sources (Embedded and Published) from Tableau projects via the GraphQL Metadata API.

### Prerequisites

- **Tableau Server** 2021.1.10 or later (may work with older versions)
- **Metadata API enabled** ([enable for Server](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_start.html#enable-the-tableau-metadata-api-for-tableau-server); always enabled for Tableau Cloud)

#### Authentication

DataHub supports two authentication methods:

1. Username/Password
2. [Personal Access Token](https://help.tableau.com/current/pro/desktop/en-us/useracct.htm#create-and-revoke-personal-access-tokens)

Either way, the user/token must have at least the **Site Administrator Explorer** site role.

:::info

We need at least the **Site Administrator Explorer** site role in order to get complete metadata from Tableau. Roles with higher privileges, like **Site Administrator Creator** or **Server Admin** also work.

With any lower role, the Tableau Metadata API returns missing/partial metadata.
This particularly affects data source fields and definitions, which impacts our ability to extract most columns and generate column-level lineage. However, table-level lineage will still be extracted for tables even when column metadata is not available (see [Tables Without Column Metadata](#tables-without-column-metadata) section below).
Other site roles, like Viewer or Explorer, are insufficient due to these limitations in the current Tableau Metadata API.

:::


### Install the Plugin
```shell
pip install 'acryl-datahub[tableau]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: tableau
  config:
    # Coordinates
    connect_uri: https://prod-ca-a.online.tableau.com
    site: acryl
    platform_instance: acryl_instance
    project_pattern: ["^default$", "^Project 2$", "^/Project A/Nested Project B$"]

    # Credentials
    username: "${TABLEAU_USER}"
    password: "${TABLEAU_PASSWORD}"

    # Options
    ingest_tags: True
    ingest_owner: True
    default_schema_map:
      mydatabase: public
      anotherdatabase: anotherschema

sink:
  # sink configs
```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">connect_uri</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Tableau host URL.  |
| <div className="path-line"><span className="path-main">add_site_container</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, sites are added as containers and therefore visible in the folder structure within Datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">custom_sql_table_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of custom sql datasources to query at a time using the Tableau API; fallbacks to `page_size` if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">dashboard_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of dashboards to query at a time using the Tableau API; fallbacks to `page_size` if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">database_hostname_to_platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Mappings to change platform instance in generated dataset urns based on database. Use only if you really know what you are doing. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">database_server_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of database servers to query at a time using the Tableau API; fallbacks to `page_size` if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">database_table_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of database tables to query at a time using the Tableau API; fallbacks to `page_size` if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">default_schema_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">embedded_datasource_field_upstream_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of upstream fields to query at a time for embedded datasources using the Tableau API; fallbacks to `page_size` * 10 if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">embedded_datasource_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of embedded datasources to query at a time using the Tableau API; fallbacks to `page_size` if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">emit_all_embedded_datasources</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest all embedded data sources. When False (default), only ingest embedded data sources that belong to an ingested workbook. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">emit_all_published_datasources</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest all published data sources. When False (default), only ingest published data sources that belong to an ingested workbook. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, extracts column-level lineage from Tableau Datasources <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_lineage_from_unsupported_custom_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | [Experimental] Extract lineage from Custom SQL queries using DataHub's SQL parser in cases where the Tableau Catalog API fails to return lineage for the query. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_project_hierarchy</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract entire project hierarchy for nested projects. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_usage_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | [experimental] Extract usage statistics for dashboards and charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">force_extraction_of_lineage_from_custom_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | [Experimental] Force extraction of lineage from Custom SQL queries using DataHub's SQL parser, even when the Tableau Catalog API returns lineage already. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_embed_url</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Ingest a URL to render an embedded Preview of assets within Tableau. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_external_links_for_charts</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Ingest a URL to link out to from charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_external_links_for_dashboards</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Ingest a URL to link out to from dashboards. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_initial_sql</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | [Experimental] When enabled, downloads published data sources and the workbooks that contain embedded data sources to extract their Initial SQL and emit lineage and/or a custom property from it. Disabled by default because the extra downloads can be slow on large sites. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_multiple_sites</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, ingests multiple sites the user has access to. If the user doesn't have access to the default site, specify an initial site to query in the site property. By default all sites the user has access to will be ingested. You can filter sites with the site_name_pattern property. This flag is currently only supported for Tableau Server. Tableau Cloud is not supported. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_owner</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Ingest Owner from source. This will override Owner info entered from UI <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_tables_external</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest details for tables external to (not embedded in) tableau as entities. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_tags</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Ingest Tags from source. This will override Tags entered from UI <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_virtual_connections</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest details for virtual connections as entities. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">initial_sql_as_custom_property</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When ingest_initial_sql is enabled, store the raw Initial SQL text as the `initialSql` custom property on the data source dataset. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">initial_sql_as_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When ingest_initial_sql is enabled, parse the Initial SQL with DataHub's SQL parser and emit the referenced tables as upstream lineage of the data source. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of retries for failed requests. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | [advanced] Number of metadata objects (e.g. CustomSQLTable, PublishedDatasource, etc) to query at a time using the Tableau API. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Tableau password, must be set if authenticating using username/password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A holder for platform -> platform_instance mappings to generate correct dataset urns <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">project_path_separator</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The separator used for the project_path_pattern field between project names. By default, we use a slash. You can change this if your Tableau projects contain slashes in their names, and you'd like to filter by project. <div className="default-line default-line-with-docs">Default: <span className="default-value">/</span></div> |
| <div className="path-line"><span className="path-main">published_datasource_field_upstream_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of upstream fields to query at a time for published datasources using the Tableau API; fallbacks to `page_size` * 10 if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">published_datasource_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of published datasources to query at a time using the Tableau API; fallbacks to `page_size` if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">session_trust_env</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Configures the trust_env property in the requests session. If set to false (default value) it will bypass proxy settings. See https://requests.readthedocs.io/en/latest/api/#requests.Session.trust_env for more information. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">sheet_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of sheets to query at a time using the Tableau API; fallbacks to `page_size` if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">site</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Tableau Site. Always required for Tableau Online. Use emptystring to connect with Default site on Tableau Server. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">sql_parsing_disable_schema_awareness</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | [Experimental] Ignore pre ingested tables schemas during parsing of SQL queries (allows to workaround ingestion errors when pre ingested schema and queries are out of sync) <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ssl_verify</span></div> <div className="type-name-line"><span className="type-name">One of boolean, string</span></div> | Whether to verify SSL certificates. If using self-signed certificates, set to false or provide the path to the .pem certificate bundle. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">token_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Tableau token name, must be set if authenticating using a personal access token. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">token_value</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Tableau token value, must be set if authenticating using a personal access token. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">use_email_as_username</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use email address instead of username for entity owners. Requires ingest_owner to be True. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Tableau username, must be set if authenticating using username/password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">virtual_connection_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of virtual connections to query at a time using the Tableau API; fallbacks to `page_size` if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">workbook_page_size</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | [advanced] Number of workbooks to query at a time using the Tableau API; defaults to `1` and fallbacks to `page_size` if not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">1</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Environment to use in namespace when constructing URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">ingest_hidden_assets</span></div> <div className="type-name-line"><span className="type-name">One of array, boolean</span></div> | When enabled, hidden worksheets and dashboards are ingested into Datahub. If a dashboard or worksheet is hidden in Tableau the luid is blank. A list of asset types can also be specified, to only ingest those hidden assets. Current options supported are 'worksheet' and 'dashboard'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;worksheet&#x27;, &#x27;dashboard&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">ingest_hidden_assets.</span><span className="path-main">enum</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "worksheet", "dashboard"  |
| <div className="path-line"><span className="path-main">lineage_overrides</span></div> <div className="type-name-line"><span className="type-name">One of TableauLineageOverrides, null</span></div> | Mappings to change generated dataset urns. Use only if you really know what you are doing. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">lineage_overrides.</span><span className="path-main">database_override_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A holder for database -> database mappings to generate correct dataset urns <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">lineage_overrides.</span><span className="path-main">platform_override_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A holder for platform -> platform mappings to generate correct dataset urns <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">permission_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of PermissionIngestionConfig, null</span></div> | Configuration settings for ingesting Tableau groups and their capabilities as custom properties. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">permission_ingestion.</span><span className="path-main">enable_workbooks</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable group permission ingestion for workbooks. Default: True <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">permission_ingestion.</span><span className="path-main">group_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">permission_ingestion.group_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_path_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_path_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">projects</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | [deprecated] Use project_pattern instead. List of tableau projects  <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;default&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">projects.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">site_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">site_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">tags_for_hidden_assets</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Tags to be added to hidden dashboards and views. If a dashboard or view is hidden in Tableau the luid is blank. This can only be used with ingest_tags enabled as it will overwrite tags entered from the UI. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">tags_for_hidden_assets.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
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
    "PermissionIngestionConfig": {
      "additionalProperties": false,
      "properties": {
        "enable_workbooks": {
          "default": true,
          "description": "Whether or not to enable group permission ingestion for workbooks. Default: True",
          "title": "Enable Workbooks",
          "type": "boolean"
        },
        "group_name_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Filter for Tableau group names when ingesting group permissions. For example, you could filter for groups that include the term 'Consumer' in their name by adding '^.*Consumer$' to the allow list.By default, all groups will be ingested. You can both allow and deny groups based on their name using their name, or a Regex pattern. Deny patterns always take precedence over allow patterns. "
        }
      },
      "title": "PermissionIngestionConfig",
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
    },
    "TableauLineageOverrides": {
      "additionalProperties": false,
      "properties": {
        "platform_override_map": {
          "anyOf": [
            {
              "additionalProperties": {
                "type": "string"
              },
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A holder for platform -> platform mappings to generate correct dataset urns",
          "title": "Platform Override Map"
        },
        "database_override_map": {
          "anyOf": [
            {
              "additionalProperties": {
                "type": "string"
              },
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A holder for database -> database mappings to generate correct dataset urns",
          "title": "Database Override Map"
        }
      },
      "title": "TableauLineageOverrides",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "page_size": {
      "default": 10,
      "description": "[advanced] Number of metadata objects (e.g. CustomSQLTable, PublishedDatasource, etc) to query at a time using the Tableau API.",
      "title": "Page Size",
      "type": "integer"
    },
    "database_server_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of database servers to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
      "title": "Database Server Page Size"
    },
    "workbook_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 1,
      "description": "[advanced] Number of workbooks to query at a time using the Tableau API; defaults to `1` and fallbacks to `page_size` if not set.",
      "title": "Workbook Page Size"
    },
    "sheet_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of sheets to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
      "title": "Sheet Page Size"
    },
    "dashboard_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of dashboards to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
      "title": "Dashboard Page Size"
    },
    "embedded_datasource_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of embedded datasources to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
      "title": "Embedded Datasource Page Size"
    },
    "embedded_datasource_field_upstream_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of upstream fields to query at a time for embedded datasources using the Tableau API; fallbacks to `page_size` * 10 if not set.",
      "title": "Embedded Datasource Field Upstream Page Size"
    },
    "published_datasource_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of published datasources to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
      "title": "Published Datasource Page Size"
    },
    "published_datasource_field_upstream_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of upstream fields to query at a time for published datasources using the Tableau API; fallbacks to `page_size` * 10 if not set.",
      "title": "Published Datasource Field Upstream Page Size"
    },
    "virtual_connection_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of virtual connections to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
      "title": "Virtual Connection Page Size"
    },
    "custom_sql_table_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of custom sql datasources to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
      "title": "Custom Sql Table Page Size"
    },
    "database_table_page_size": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[advanced] Number of database tables to query at a time using the Tableau API; fallbacks to `page_size` if not set.",
      "title": "Database Table Page Size"
    },
    "connect_uri": {
      "description": "Tableau host URL.",
      "title": "Connect Uri",
      "type": "string"
    },
    "username": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Tableau username, must be set if authenticating using username/password.",
      "title": "Username"
    },
    "password": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Tableau password, must be set if authenticating using username/password.",
      "title": "Password"
    },
    "token_name": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Tableau token name, must be set if authenticating using a personal access token.",
      "title": "Token Name"
    },
    "token_value": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Tableau token value, must be set if authenticating using a personal access token.",
      "title": "Token Value"
    },
    "site": {
      "default": "",
      "description": "Tableau Site. Always required for Tableau Online. Use emptystring to connect with Default site on Tableau Server.",
      "title": "Site",
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
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "max_retries": {
      "default": 3,
      "description": "Number of retries for failed requests.",
      "title": "Max Retries",
      "type": "integer"
    },
    "ssl_verify": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "string"
        }
      ],
      "default": true,
      "description": "Whether to verify SSL certificates. If using self-signed certificates, set to false or provide the path to the .pem certificate bundle.",
      "title": "Ssl Verify"
    },
    "session_trust_env": {
      "default": false,
      "description": "Configures the trust_env property in the requests session. If set to false (default value) it will bypass proxy settings. See https://requests.readthedocs.io/en/latest/api/#requests.Session.trust_env for more information.",
      "title": "Session Trust Env",
      "type": "boolean"
    },
    "extract_column_level_lineage": {
      "default": true,
      "description": "When enabled, extracts column-level lineage from Tableau Datasources",
      "title": "Extract Column Level Lineage",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "Environment to use in namespace when constructing URNs.",
      "title": "Env",
      "type": "string"
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
      "description": ""
    },
    "platform_instance_map": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "A holder for platform -> platform_instance mappings to generate correct dataset urns",
      "title": "Platform Instance Map"
    },
    "projects": {
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
      "default": [
        "default"
      ],
      "description": "[deprecated] Use project_pattern instead. List of tableau projects ",
      "title": "Projects"
    },
    "project_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "[deprecated] Use project_path_pattern instead. Filter for specific Tableau projects. For example, use 'My Project' to ingest a root-level Project with name 'My Project', or 'My Project/Nested Project' to ingest a nested Project with name 'Nested Project'. By default, all Projects nested inside a matching Project will be included in ingestion. You can both allow and deny projects based on their name using their name, or a Regex pattern. Deny patterns always take precedence over allow patterns. By default, all projects will be ingested."
    },
    "project_path_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Filters Tableau projects by their full path. For instance, 'My Project/Nested Project' targets a specific nested project named 'Nested Project'. This is also useful when you need to exclude all nested projects under a particular project. You can allow or deny projects by specifying their path or a regular expression pattern. Deny patterns always override allow patterns. By default, all projects are ingested."
    },
    "project_path_separator": {
      "default": "/",
      "description": "The separator used for the project_path_pattern field between project names. By default, we use a slash. You can change this if your Tableau projects contain slashes in their names, and you'd like to filter by project.",
      "title": "Project Path Separator",
      "type": "string"
    },
    "default_schema_map": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "Default schema to use when schema is not found.",
      "title": "Default Schema Map",
      "type": "object"
    },
    "ingest_tags": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Ingest Tags from source. This will override Tags entered from UI",
      "title": "Ingest Tags"
    },
    "ingest_owner": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Ingest Owner from source. This will override Owner info entered from UI",
      "title": "Ingest Owner"
    },
    "use_email_as_username": {
      "default": false,
      "description": "Use email address instead of username for entity owners. Requires ingest_owner to be True.",
      "title": "Use Email As Username",
      "type": "boolean"
    },
    "ingest_tables_external": {
      "default": false,
      "description": "Ingest details for tables external to (not embedded in) tableau as entities.",
      "title": "Ingest Tables External",
      "type": "boolean"
    },
    "ingest_virtual_connections": {
      "default": true,
      "description": "Ingest details for virtual connections as entities.",
      "title": "Ingest Virtual Connections",
      "type": "boolean"
    },
    "emit_all_published_datasources": {
      "default": false,
      "description": "Ingest all published data sources. When False (default), only ingest published data sources that belong to an ingested workbook.",
      "title": "Emit All Published Datasources",
      "type": "boolean"
    },
    "emit_all_embedded_datasources": {
      "default": false,
      "description": "Ingest all embedded data sources. When False (default), only ingest embedded data sources that belong to an ingested workbook.",
      "title": "Emit All Embedded Datasources",
      "type": "boolean"
    },
    "lineage_overrides": {
      "anyOf": [
        {
          "$ref": "#/$defs/TableauLineageOverrides"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Mappings to change generated dataset urns. Use only if you really know what you are doing."
    },
    "database_hostname_to_platform_instance_map": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Mappings to change platform instance in generated dataset urns based on database. Use only if you really know what you are doing.",
      "title": "Database Hostname To Platform Instance Map"
    },
    "extract_usage_stats": {
      "default": false,
      "description": "[experimental] Extract usage statistics for dashboards and charts.",
      "title": "Extract Usage Stats",
      "type": "boolean"
    },
    "ingest_embed_url": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Ingest a URL to render an embedded Preview of assets within Tableau.",
      "title": "Ingest Embed Url"
    },
    "ingest_external_links_for_dashboards": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Ingest a URL to link out to from dashboards.",
      "title": "Ingest External Links For Dashboards"
    },
    "ingest_external_links_for_charts": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Ingest a URL to link out to from charts.",
      "title": "Ingest External Links For Charts"
    },
    "extract_project_hierarchy": {
      "default": true,
      "description": "Whether to extract entire project hierarchy for nested projects.",
      "title": "Extract Project Hierarchy",
      "type": "boolean"
    },
    "extract_lineage_from_unsupported_custom_sql_queries": {
      "default": true,
      "description": "[Experimental] Extract lineage from Custom SQL queries using DataHub's SQL parser in cases where the Tableau Catalog API fails to return lineage for the query.",
      "title": "Extract Lineage From Unsupported Custom Sql Queries",
      "type": "boolean"
    },
    "force_extraction_of_lineage_from_custom_sql_queries": {
      "default": false,
      "description": "[Experimental] Force extraction of lineage from Custom SQL queries using DataHub's SQL parser, even when the Tableau Catalog API returns lineage already.",
      "title": "Force Extraction Of Lineage From Custom Sql Queries",
      "type": "boolean"
    },
    "ingest_initial_sql": {
      "default": false,
      "description": "[Experimental] When enabled, downloads published data sources and the workbooks that contain embedded data sources to extract their Initial SQL and emit lineage and/or a custom property from it. Disabled by default because the extra downloads can be slow on large sites.",
      "title": "Ingest Initial Sql",
      "type": "boolean"
    },
    "initial_sql_as_lineage": {
      "default": true,
      "description": "When ingest_initial_sql is enabled, parse the Initial SQL with DataHub's SQL parser and emit the referenced tables as upstream lineage of the data source.",
      "title": "Initial Sql As Lineage",
      "type": "boolean"
    },
    "initial_sql_as_custom_property": {
      "default": true,
      "description": "When ingest_initial_sql is enabled, store the raw Initial SQL text as the `initialSql` custom property on the data source dataset.",
      "title": "Initial Sql As Custom Property",
      "type": "boolean"
    },
    "sql_parsing_disable_schema_awareness": {
      "default": false,
      "description": "[Experimental] Ignore pre ingested tables schemas during parsing of SQL queries (allows to workaround ingestion errors when pre ingested schema and queries are out of sync)",
      "title": "Sql Parsing Disable Schema Awareness",
      "type": "boolean"
    },
    "ingest_multiple_sites": {
      "default": false,
      "description": "When enabled, ingests multiple sites the user has access to. If the user doesn't have access to the default site, specify an initial site to query in the site property. By default all sites the user has access to will be ingested. You can filter sites with the site_name_pattern property. This flag is currently only supported for Tableau Server. Tableau Cloud is not supported.",
      "title": "Ingest Multiple Sites",
      "type": "boolean"
    },
    "site_name_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Filter for specific Tableau sites. By default, all sites will be included in the ingestion. You can both allow and deny sites based on their name using their name, or a Regex pattern. Deny patterns always take precedence over allow patterns. This property is currently only supported for Tableau Server. Tableau Cloud is not supported. "
    },
    "add_site_container": {
      "default": false,
      "description": "When enabled, sites are added as containers and therefore visible in the folder structure within Datahub.",
      "title": "Add Site Container",
      "type": "boolean"
    },
    "permission_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/PermissionIngestionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Configuration settings for ingesting Tableau groups and their capabilities as custom properties."
    },
    "ingest_hidden_assets": {
      "anyOf": [
        {
          "items": {
            "enum": [
              "worksheet",
              "dashboard"
            ],
            "type": "string"
          },
          "type": "array"
        },
        {
          "type": "boolean"
        }
      ],
      "default": [
        "worksheet",
        "dashboard"
      ],
      "description": "When enabled, hidden worksheets and dashboards are ingested into Datahub. If a dashboard or worksheet is hidden in Tableau the luid is blank. A list of asset types can also be specified, to only ingest those hidden assets. Current options supported are 'worksheet' and 'dashboard'.",
      "title": "Ingest Hidden Assets"
    },
    "tags_for_hidden_assets": {
      "default": [],
      "description": "Tags to be added to hidden dashboards and views. If a dashboard or view is hidden in Tableau the luid is blank. This can only be used with ingest_tags enabled as it will overwrite tags entered from the UI.",
      "items": {
        "type": "string"
      },
      "title": "Tags For Hidden Assets",
      "type": "array"
    }
  },
  "required": [
    "connect_uri"
  ],
  "title": "TableauConfig",
  "type": "object"
}
```





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


### Code Coordinates
- Class Name: `datahub.ingestion.source.tableau.tableau.TableauSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/tableau/tableau.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Tableau, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
