---
sidebar_position: 7
title: Databricks
slug: /generated/ingestion/sources/databricks
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/databricks.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Databricks

DataHub supports integration with Databricks ecosystem using a multitude of connectors, depending on your exact setup.

## Databricks Hive

The simplest way to integrate is usually via the Hive connector. The [Hive starter recipe](http://datahubproject.io/docs/generated/ingestion/sources/hive#starter-recipe) has a section describing how to connect to your Databricks workspace.

## Databricks Unity Catalog (new)

The recently introduced [Unity Catalog](https://www.databricks.com/product/unity-catalog) provides a new way to govern your assets within the Databricks lakehouse. If you have enabled Unity Catalog, you can use the `unity-catalog` source (see below) to integrate your metadata into DataHub as an alternate to the Hive pathway.

## Databricks Spark

To complete the picture, we recommend adding push-based ingestion from your Spark jobs to see real-time activity and lineage between your Databricks tables and your Spark jobs. Use the Spark agent to push metadata to DataHub using the instructions [here](../../../../metadata-integration/java/spark-lineage/README.md#configuration-instructions-databricks).

## Watch the DataHub Talk at the Data and AI Summit 2022

For a deeper look at how to think about DataHub within and across your Databricks ecosystem, watch the recording of our talk at the Data and AI Summit 2022.

<p align="center">
  <a href="https://www.youtube.com/watch?v=SCP0PR3t7dc">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/metadata-ingestion/databricks/data_and_ai_summit_2022.png"/>
  </a>
</p>

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                                                             |
| ---------------------------------------------------------------------------------------------------------- | ------ | ----------------------------------------------------------------- |
| Asset Containers                                                                                           | ✅     | Enabled by default                                                |
| Column-level Lineage                                                                                       | ✅     | Enabled by default                                                |
| Dataset Usage                                                                                              | ✅     | Enabled by default                                                |
| Descriptions                                                                                               | ✅     | Enabled by default                                                |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Optionally enabled via `stateful_ingestion.remove_stale_metadata` |
| [Domains](../../../domains.md)                                                                             | ✅     | Supported via the `domain` config field                           |
| Extract Ownership                                                                                          | ✅     | Supported via the `include_ownership` config                      |
| [Platform Instance](../../../platform-instances.md)                                                        | ✅     | Enabled by default                                                |
| Schema Metadata                                                                                            | ✅     | Enabled by default                                                |
| Table-Level Lineage                                                                                        | ✅     | Enabled by default                                                |

This plugin extracts the following metadata from Databricks Unity Catalog:

- metastores
- schemas
- tables and column lineage

### Prerequisities

- Get your Databricks instance's [workspace url](https://docs.databricks.com/workspace/workspace-details.html#workspace-instance-names-urls-and-ids)
- Create a [Databricks Service Principal](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#what-is-a-service-principal)
  - You can skip this step and use your own account to get things running quickly,
    but we strongly recommend creating a dedicated service principal for production use.
- Generate a Databricks Personal Access token following the following guides:
  - [Service Principals](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#personal-access-tokens)
  - [Personal Access Tokens](https://docs.databricks.com/dev-tools/auth.html#databricks-personal-access-tokens)
- Provision your service account:
  - To ingest your workspace's metadata and lineage, your service principal must have all of the following:
    - One of: metastore admin role, ownership of, or `USE CATALOG` privilege on any catalogs you want to ingest
    - One of: metastore admin role, ownership of, or `USE SCHEMA` privilege on any schemas you want to ingest
    - Ownership of or `SELECT` privilege on any tables and views you want to ingest
    - [Ownership documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/ownership.html)
    - [Privileges documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
  - To `include_usage_statistics` (enabled by default), your service principal must have `CAN_MANAGE` permissions on any SQL Warehouses you want to ingest: [guide](https://docs.databricks.com/security/auth-authz/access-control/sql-endpoint-acl.html).
  - To ingest `profiling` information with `call_analyze` (enabled by default), your service principal must have ownership or `MODIFY` privilege on any tables you want to profile.
    - Alternatively, you can run [ANALYZE TABLE](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-analyze-table.html) yourself on any tables you want to profile, then set `call_analyze` to `false`.
      You will still need `SELECT` privilege on those tables to fetch the results.
- Check the starter recipe below and replace `workspace_url` and `token` with your information from the previous steps.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[unity-catalog]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: unity-catalog
  config:
    workspace_url: https://my-workspace.cloud.databricks.com
    token: "mygenerated_databricks_token"
    #metastore_id_pattern:
    #  deny:
    #    - 11111-2222-33333-44-555555
    #catalog_pattern:
    #  allow:
    #    - my-catalog
    #schema_pattern:
    #  deny:
    #    - information_schema
    #table_pattern:
    #  allow:
    #    - test.lineagedemo.dinner
    # First you have to create domains on Datahub by following this guide -> https://datahubproject.io/docs/domains/#domains-setup-prerequisites-and-permissions
    #domain:
    #  urn:li:domain:1111-222-333-444-555:
    #    allow:
    #      - main.*

    stateful_ingestion:
      enabled: true

pipeline_name: acme-corp-unity
# sink configs if needed
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                    | Databricks personal access token                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">workspace_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                            | Databricks workspace url. e.g. https://my-workspace.cloud.databricks.com                                                                                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                                  | Size of the time window to aggregate usage stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">DAY</span></div>                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div>                                                            | Latest date of usage to consider. Default: Current time in UTC                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                            | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                        | Option to enable/disable lineage generation. Currently we have to call a rest call per column to get column level lineage due to the Databrick api which can slow down ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                     | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">include_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                             | Option to enable/disable ownership generation for metastores, catalogs, schemas, and tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                | Whether to report read operational stats. Experimental. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">include_table_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                         | Option to enable/disable lineage generation. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">include_top_n_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                         | Whether to ingest the top_n_queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">include_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                      | Generate usage statistics. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                              | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div>                                                          | Earliest date of usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`)                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">store_last_profiling_timestamps</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                               | Enable storing last profile timestamp in store. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">top_n_queries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                 | Number of top queries to save to each table. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div>                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">workspace_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                 | Name of the workspace. Default to deployment name present in workspace_url                                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                            | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">catalog_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                      | Regex patterns for catalogs to filter in ingestion. Specify regex to match the full `metastore.catalog` name. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">catalog_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">catalog_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">catalog_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div>                                                      | A class to store allow deny regexes                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                 |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                  | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                       | Regex patterns for schemas to filter in ingestion. Specify regex to the full `metastore.catalog.schema` name. e.g. to match all tables in schema analytics, use the regex `^mymetastore\.mycatalog\.analytics$`. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                      |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                        | Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in `catalog.schema.table` format. e.g. to match all tables starting with customer in Customer catalog and public schema, use the regex `Customer\.public\.customer.*`. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                 |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                 | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                   | regex patterns for user emails to filter in usage. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>            | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">UnityCatalogProfilerConfig</span></div>                                                  | Data profiling configuration <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;enabled&#x27;: False, &#x27;warehouse_id&#x27;: None, &#x27;profile\_...</span></div>                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">call_analyze</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                   | Whether to call ANALYZE TABLE as part of profile ingestion.If false, will ingest the results of the most recent ANALYZE TABLE call, if any. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                        | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_wait_secs</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                  | Maximum time to wait for an ANALYZE TABLE query to complete. <div className="default-line default-line-with-docs">Default: <span className="default-value">3600</span></div>                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                    | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">80</span></div>                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>       | Whether to perform profiling at table-level only or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">warehouse_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                    | SQL Warehouse id, for running profiling queries.                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>               | Regex patterns to filter tables for profiling during ingestion. Specify regex to match the `catalog.schema.table` format. Note that only tables allowed by the `table_pattern` will be considered. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                    |
| <div className="path-line"><span className="path-prefix">profiling.pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">profiling.pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>             |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">profiling.pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>             | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                 | Unity Catalog Stateful Ingestion Config.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                          |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "UnityCatalogSourceConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
  "type": "object",
  "properties": {
    "store_last_profiling_timestamps": {
      "title": "Store Last Profiling Timestamps",
      "description": "Enable storing last profile timestamp in store.",
      "default": false,
      "type": "boolean"
    },
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "bucket_duration": {
      "description": "Size of the time window to aggregate usage stats.",
      "default": "DAY",
      "allOf": [
        {
          "$ref": "#/definitions/BucketDuration"
        }
      ]
    },
    "end_time": {
      "title": "End Time",
      "description": "Latest date of usage to consider. Default: Current time in UTC",
      "type": "string",
      "format": "date-time"
    },
    "start_time": {
      "title": "Start Time",
      "description": "Earliest date of usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`)",
      "type": "string",
      "format": "date-time"
    },
    "top_n_queries": {
      "title": "Top N Queries",
      "description": "Number of top queries to save to each table.",
      "default": 10,
      "exclusiveMinimum": 0,
      "type": "integer"
    },
    "user_email_pattern": {
      "title": "User Email Pattern",
      "description": "regex patterns for user emails to filter in usage.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "include_operational_stats": {
      "title": "Include Operational Stats",
      "description": "Whether to display operational stats.",
      "default": true,
      "type": "boolean"
    },
    "include_read_operational_stats": {
      "title": "Include Read Operational Stats",
      "description": "Whether to report read operational stats. Experimental.",
      "default": false,
      "type": "boolean"
    },
    "format_sql_queries": {
      "title": "Format Sql Queries",
      "description": "Whether to format sql queries",
      "default": false,
      "type": "boolean"
    },
    "include_top_n_queries": {
      "title": "Include Top N Queries",
      "description": "Whether to ingest the top_n_queries.",
      "default": true,
      "type": "boolean"
    },
    "stateful_ingestion": {
      "title": "Stateful Ingestion",
      "description": "Unity Catalog Stateful Ingestion Config.",
      "allOf": [
        {
          "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
        }
      ]
    },
    "token": {
      "title": "Token",
      "description": "Databricks personal access token",
      "type": "string"
    },
    "workspace_url": {
      "title": "Workspace Url",
      "description": "Databricks workspace url. e.g. https://my-workspace.cloud.databricks.com",
      "type": "string"
    },
    "workspace_name": {
      "title": "Workspace Name",
      "description": "Name of the workspace. Default to deployment name present in workspace_url",
      "type": "string"
    },
    "catalog_pattern": {
      "title": "Catalog Pattern",
      "description": "Regex patterns for catalogs to filter in ingestion. Specify regex to match the full `metastore.catalog` name.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "schema_pattern": {
      "title": "Schema Pattern",
      "description": "Regex patterns for schemas to filter in ingestion. Specify regex to the full `metastore.catalog.schema` name. e.g. to match all tables in schema analytics, use the regex `^mymetastore\\.mycatalog\\.analytics$`.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "table_pattern": {
      "title": "Table Pattern",
      "description": "Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in `catalog.schema.table` format. e.g. to match all tables starting with customer in Customer catalog and public schema, use the regex `Customer\\.public\\.customer.*`.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "domain": {
      "title": "Domain",
      "description": "Attach domains to catalogs, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like \"Marketing\".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.",
      "default": {},
      "type": "object",
      "additionalProperties": {
        "$ref": "#/definitions/AllowDenyPattern"
      }
    },
    "include_table_lineage": {
      "title": "Include Table Lineage",
      "description": "Option to enable/disable lineage generation.",
      "default": true,
      "type": "boolean"
    },
    "include_ownership": {
      "title": "Include Ownership",
      "description": "Option to enable/disable ownership generation for metastores, catalogs, schemas, and tables.",
      "default": false,
      "type": "boolean"
    },
    "include_column_lineage": {
      "title": "Include Column Lineage",
      "description": "Option to enable/disable lineage generation. Currently we have to call a rest call per column to get column level lineage due to the Databrick api which can slow down ingestion. ",
      "default": true,
      "type": "boolean"
    },
    "include_usage_statistics": {
      "title": "Include Usage Statistics",
      "description": "Generate usage statistics.",
      "default": true,
      "type": "boolean"
    },
    "profiling": {
      "title": "Profiling",
      "description": "Data profiling configuration",
      "default": {
        "enabled": false,
        "warehouse_id": null,
        "profile_table_level_only": false,
        "pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "call_analyze": true,
        "max_wait_secs": 3600,
        "max_workers": 80
      },
      "allOf": [
        {
          "$ref": "#/definitions/UnityCatalogProfilerConfig"
        }
      ]
    }
  },
  "required": [
    "token",
    "workspace_url"
  ],
  "additionalProperties": false,
  "definitions": {
    "BucketDuration": {
      "title": "BucketDuration",
      "description": "An enumeration.",
      "enum": [
        "DAY",
        "HOUR"
      ],
      "type": "string"
    },
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "DynamicTypedStateProviderConfig": {
      "title": "DynamicTypedStateProviderConfig",
      "type": "object",
      "properties": {
        "type": {
          "title": "Type",
          "description": "The type of the state provider to use. For DataHub use `datahub`",
          "type": "string"
        },
        "config": {
          "title": "Config",
          "description": "The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19)."
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": false
    },
    "StatefulStaleMetadataRemovalConfig": {
      "title": "StatefulStaleMetadataRemovalConfig",
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "The type of the ingestion state provider registered with datahub.",
          "default": false,
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "title": "Remove Stale Metadata",
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "UnityCatalogProfilerConfig": {
      "title": "UnityCatalogProfilerConfig",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "Whether profiling should be done.",
          "default": false,
          "type": "boolean"
        },
        "warehouse_id": {
          "title": "Warehouse Id",
          "description": "SQL Warehouse id, for running profiling queries.",
          "type": "string"
        },
        "profile_table_level_only": {
          "title": "Profile Table Level Only",
          "description": "Whether to perform profiling at table-level only or include column-level profiling as well.",
          "default": false,
          "type": "boolean"
        },
        "pattern": {
          "title": "Pattern",
          "description": "Regex patterns to filter tables for profiling during ingestion. Specify regex to match the `catalog.schema.table` format. Note that only tables allowed by the `table_pattern` will be considered.",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "allOf": [
            {
              "$ref": "#/definitions/AllowDenyPattern"
            }
          ]
        },
        "call_analyze": {
          "title": "Call Analyze",
          "description": "Whether to call ANALYZE TABLE as part of profile ingestion.If false, will ingest the results of the most recent ANALYZE TABLE call, if any.",
          "default": true,
          "type": "boolean"
        },
        "max_wait_secs": {
          "title": "Max Wait Secs",
          "description": "Maximum time to wait for an ANALYZE TABLE query to complete.",
          "default": 3600,
          "type": "integer"
        },
        "max_workers": {
          "title": "Max Workers",
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "default": 80,
          "type": "integer"
        }
      },
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

#### Troubleshooting

##### No data lineage captured or missing lineage

Check that you meet the [Unity Catalog lineage requirements](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements).

Also check the [Unity Catalog limitations](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#limitations) to make sure that lineage would be expected to exist in this case.

##### Lineage extraction is too slow

Currently, there is no way to get table or column lineage in bulk from the Databricks Unity Catalog REST api. Table lineage calls require one API call per table, and column lineage calls require one API call per column. If you find metadata extraction taking too long, you can turn off column level lineage extraction via the `include_column_lineage` config flag.

### Code Coordinates

- Class Name: `datahub.ingestion.source.unity.source.UnityCatalogSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/unity/source.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Databricks, feel free to ping us on [our Slack](https://slack.datahubproject.io).
