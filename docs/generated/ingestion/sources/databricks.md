


# Databricks

## Overview

Databricks is a data platform used to store and query analytical or operational data. Learn more in the [official Databricks documentation](https://www.databricks.com/).

The DataHub integration for Databricks covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, usage statistics, data profiling, ownership, and stateful deletion detection.

DataHub supports integration with Databricks ecosystem using a multitude of connectors, depending on your exact setup.

### Databricks Unity Catalog

[Unity Catalog](https://www.databricks.com/product/unity-catalog) is Databricks' governance layer for assets within the lakehouse. If you have a Unity Catalog-enabled workspace, use the `databricks` source (aka `unity-catalog` source — see below) to integrate your metadata into DataHub. This also ingests the Hive metastore catalog in Databricks, and is the recommended approach for ingesting the Databricks ecosystem into DataHub.

### Databricks Hive (legacy)

The alternative way to integrate is via the Hive connector. The [Hive starter recipe](/docs/generated/ingestion/sources/hive#starter-recipe) has a section describing how to connect to your Databricks workspace.

### Databricks Spark

To complete the picture, we recommend adding push-based ingestion from your Spark jobs to see real-time activity and lineage between your Databricks tables and your Spark jobs. Use the Spark agent to push metadata to DataHub using the instructions [here](../../../../metadata-integration/java/acryl-spark-lineage/README.md#configuration-instructions-databricks).

## Concept Mapping

| Databricks Concept                        | DataHub Entity (Subtype)          | Notes                                                                                                                                                           |
| ----------------------------------------- | --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Workspace / Account                       | Platform Instance                 | Top-level scope; all URNs include the configured platform instance.                                                                                             |
| Metastore                                 | Container (METASTORE)             | Top-level Unity Catalog container.                                                                                                                              |
| Catalog                                   | Container (CATALOG)               | Namespace within a metastore. Hive Metastore is ingested as a special catalog type.                                                                             |
| Hive Metastore catalog                    | Container (CATALOG)               | Ingested as a special catalog type when `include_hive_metastore: true` (default). Represents the legacy workspace-level Hive Metastore alongside Unity Catalog. |
| Schema                                    | Container (SCHEMA)                | Nested under its Catalog container.                                                                                                                             |
| Table (managed, external, Delta, Iceberg) | Dataset (TABLE)                   | All non-view table types including streaming tables. Schema, descriptions, and tags are extracted.                                                              |
| View / Materialized View                  | Dataset (VIEW)                    | View definition is captured.                                                                                                                                    |
| Metric View                               | Dataset (METRIC_VIEW)             | Opt-in via `include_metric_views: true`. YAML body is preserved; dimensions and measures are tagged as schema fields.                                           |
| Notebook                                  | Dataset (NOTEBOOK)                | Ingested when `include_notebooks` is enabled. Lineage to and from tables is extracted.                                                                          |
| ML Model Group                            | MLModelGroup                      | Represents an MLflow Registered Model.                                                                                                                          |
| ML Model Version                          | MLModel                           | Each registered version with run metrics, parameters, tags, and aliases.                                                                                        |
| Column / field                            | SchemaField                       | Column type, nullability, and descriptions are extracted.                                                                                                       |
| User / Service Principal                  | CorpUser                          | Ownership; service principals mapped via display name to user URN.                                                                                              |
| Group                                     | CorpGroup                         | Ownership mapped as `urn:li:corpGroup:{group_name}`.                                                                                                            |
| Unity Catalog Tag                         | Tag                               | Extracted at catalog, schema, table, and column levels.                                                                                                         |
| Table / column lineage                    | Lineage edges                     | From view definitions and SQL query history. Notebook-to-table lineage also extracted.                                                                          |
| Query operations and usage                | DatasetUsageStatistics, Operation | Per-dataset query counts and DML operation metrics. System-table query history when `warehouse_id` is configured.                                               |


## Module `unity-catalog`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Catalog, Schema. |
| Column-level Lineage | ✅ | Enabled by default. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Supported via the `profiling.enabled` config. |
| Dataset Usage | ✅ | Enabled by default. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| Extract Ownership | ✅ | Supported via the `include_ownership` config. |
| [Operation Capture](../../../api/tutorials/operations.md) | ✅ | Enabled by default via usage extraction, can be disabled via `include_operational_stats`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `unity-catalog` module ingests metadata from Databricks into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

- Get your Databricks instance's [workspace url](https://docs.databricks.com/workspace/workspace-details.html#workspace-instance-names-urls-and-ids)
- Create a [Databricks Service Principal](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#what-is-a-service-principal)
  - You can skip this step and use your own account to get things running quickly,
    but we strongly recommend creating a dedicated service principal for production use.

#### Authentication Options

You can authenticate with Databricks using OAuth, Azure authentication, a Personal Access Token (legacy), or Databricks unified authentication:

**Option 1: OAuth**

- Generate a Databricks OAuth secret using the following guide:
  - [Authorize service principal access to Databricks with OAuth](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m#oauth-m2m-manual)

**Option 2: Azure Authentication (for Azure Databricks)**

- Create a Microsoft Entra ID application:
  - Follow the [Microsoft Entra ID app registration guide](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app)
  - Note down the `client_id` (Application ID), `tenant_id` (Directory ID), and create a `client_secret`
- Grant the Microsoft Entra ID application access to your Databricks workspace:
  - Add the service principal to your Databricks workspace following [this guide](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#add-a-service-principal-to-your-azure-databricks-account-using-the-account-console)

**Option 3: Personal Access Token (PAT) (legacy)**

- Generate a Databricks Personal Access token following the following guides:
  - [Service Principals](https://docs.databricks.com/administration-guide/users-groups/service-principals.html#personal-access-tokens)
  - [Personal Access Tokens](https://docs.databricks.com/dev-tools/auth.html#databricks-personal-access-tokens)

**Option 4: Unified authentication**

- Set authentication configuration via environment variables or Databricks configuration profiles, as specified in the [Databricks unified authentication guide](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth).

#### Provision your service account:

- To ingest your workspace's metadata and lineage, your service principal must have all of the following:
  - One of: metastore admin role, ownership of, or `USE CATALOG` privilege on any catalogs you want to ingest
  - One of: metastore admin role, ownership of, or `USE SCHEMA` privilege on any schemas you want to ingest
  - Ownership of or `SELECT` privilege on any tables and views you want to ingest
  - [Ownership documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/ownership.html)
  - [Privileges documentation](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
- To ingest legacy hive_metastore catalog (`include_hive_metastore` - enabled by default), your service principal must have all of the following:
  - `READ_METADATA` and `USAGE` privilege on `hive_metastore` catalog
  - `READ_METADATA` and `USAGE` privilege on schemas you want to ingest
  - `READ_METADATA` and `USAGE` privilege on tables and views you want to ingest
  - [Hive Metastore Privileges documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-privileges-hms.html)
- To ingest your workspace's notebooks and respective lineage, your service principal must have `CAN_READ` privileges on the folders containing the notebooks you want to ingest: [guide](https://docs.databricks.com/en/security/auth-authz/access-control/workspace-acl.html#folder-permissions).
- To `include_usage_statistics` (enabled by default), your service principal must have one of the following:
  - When `usage_data_source` is `SYSTEM_TABLES`, or `AUTO` (default) with `warehouse_id` configured: `CAN_USE` on the SQL warehouse and `SELECT` on `system.query.history` and `system.access.table_lineage`.
  - Otherwise (REST API path): `CAN_MANAGE` on the SQL warehouse: [guide](https://docs.databricks.com/security/auth-authz/access-control/sql-endpoint-acl.html).
- To `include_table_constraints` (disabled by default), no additional permissions are required beyond the `SELECT` privilege already listed above. The connector uses the same `tables.get()` API endpoint.
- To ingest `profiling` information with the default SQLAlchemy profiler (`method: sqlalchemy`), you need `SELECT` privilege on tables and views.
- To ingest `profiling` information with `method: ge` (requires `pip install 'acryl-datahub[profiling-ge]'`), you need `SELECT` privileges on all profiled tables.
- To ingest `profiling` information with `method: analyze` and `call_analyze: true` (enabled by default), your service principal must have ownership or `MODIFY` privilege on any tables you want to profile.
  - Alternatively, you can run [ANALYZE TABLE](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-analyze-table.html) yourself on any tables you want to profile, then set `call_analyze` to `false`.
    You will still need `SELECT` privilege on those tables to fetch the results.
- Check the starter recipe below and replace `workspace_url` and either `token` (for PAT authentication) or `azure_auth` credentials (for Azure authentication) with your information from the previous steps.

#### Permissions for DataHub Cloud Assertions (Observe)

If you plan to use DataHub Cloud's [Freshness](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/managed-datahub/observe/freshness-assertions), [Volume](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/managed-datahub/observe/volume-assertions), or [Column](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/managed-datahub/observe/column-assertions) Assertions on Databricks, the required Unity Catalog privileges depend on which **Source** you select in the assertion builder:

| Source Type                                                                        | Required Privilege(s)                                                                                              | Notes                                                                                                                                                                                                                      |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Table Statistics**                                                               | `MODIFY` (or ownership) on the target table                                                                        | Runs `ANALYZE TABLE ... COMPUTE STATISTICS` followed by `DESCRIBE TABLE EXTENDED`. On Delta tables this is metadata-only (reads file-level stats from the transaction log). Tables only, not Views. Default Volume Source. |
| **Information Schema**                                                             | `USE CATALOG` + `USE SCHEMA` on the containing catalog/schema, plus `SELECT` on `system.information_schema.tables` | Queries the Unity Catalog `information_schema.tables` view. Tables only, not Views.                                                                                                                                        |
| **Audit Log**                                                                      | `SELECT` on `system.access.audit` (requires Unity Catalog system schemas to be enabled)                            | Reads workspace audit events. Tables only.                                                                                                                                                                                 |
| **File Metadata**                                                                  | `SELECT` on the target table                                                                                       | Reads file-level modification time via Delta transaction log metadata. Delta tables only.                                                                                                                                  |
| **Query** / **Last Modified Column** / **High Watermark Column** / **Field Value** | `SELECT` on the target table                                                                                       | Runs SQL queries against the table. Works for Tables and Views.                                                                                                                                                            |
| **DataHub Operation** / **DataHub Dataset Profile**                                | _(none)_                                                                                                           | Uses DataHub metadata only, no Databricks access needed.                                                                                                                                                                   |

In addition, the service principal used for assertion evaluation needs `USE CATALOG` and `USE SCHEMA` on the catalog and schema containing the target tables, and must be granted access to a SQL Warehouse (`CAN_USE` permission) to run statements.


### Install the Plugin
```shell
pip install 'acryl-datahub[unity-catalog]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: databricks
  config:
    workspace_url: https://my-workspace.cloud.databricks.com
    
    # Authentication Option 1: OAuth
    client_id: "<client_id>"
    client_secret: "<client_secret>"
    
    # Authentication Option 2: Azure Authentication (for Azure Databricks)
    # Uncomment the following section and comment out the token above to use Azure auth
    # azure_auth:
    #   client_id: "<azure_client_id>"
    #   tenant_id: "<azure_tenant_id>" 
    #   client_secret: "<azure_client_secret>"

    # Authentication Option 3: Personal Access Token
    # Uncomment the following section and comment out client_id/client_secret above
    # token: "<token>"

    # Authentication Option 4: Databricks unified auth
    # Comment out client_id/client_secret above to use Databricks unified auth (reads environment variables
    # and local Databricks configuration profiles)

    include_metastore: false
    include_ownership: true
    include_ml_model_aliases: false
    ml_model_max_results: 1000
    profiling:
      method: "sqlalchemy"
      enabled: true
      warehouse_id: "<warehouse_id>"
      profile_table_level_only: false
      max_wait_secs: 60
      pattern:
        deny:
          - ".*\\.unwanted_schema"

#    emit_siblings: true
#    delta_lake_options:
#      platform_instance_name: null
#      env: 'PROD'

#    profiling:
#      method: "analyze"
#      enabled: true
#      warehouse_id: "<warehouse_id>"
#      profile_table_level_only: true
#      call_analyze: true

#    catalogs: ["my_catalog"]
#    schema_pattern:
#      deny:
#        - information_schema
#    table_pattern:
#      allow:
#        - my_catalog.my_schema.my_table
#     First you have to create domains on Datahub by following this guide -> https://docs.datahub.com/docs/domains/#domains-setup-prerequisites-and-permissions
#    domain:
#      urn:li:domain:1111-222-333-444-555:
#        allow:
#          - main.*

    stateful_ingestion:
      enabled: true

pipeline_name: acme-corp-unity


# sink configs if needed

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">workspace_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Databricks workspace url. e.g. https://my-workspace.cloud.databricks.com  |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-main">client_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Databricks service principal client ID <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Databricks service principal client secret <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">column_lineage_column_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Limit the number of columns to get column level lineage.  <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">databricks_api_page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Page size for Databricks API calls when listing resources (catalogs, schemas, tables, etc.). When set to 0 (default), uses server-side configured page length (recommended). When set to a positive value, the page length is the minimum of this value and the server configured value. Must be a non-negative integer. <div className="default-line default-line-with-docs">Default: <span className="default-value">0</span></div> |
| <div className="path-line"><span className="path-main">emit_federation_structured_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Define and assign structured properties marking foreign catalogs as federated (facetable in the UI). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">emit_siblings</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit siblings relation with corresponding delta-lake platform's table. If enabled, this will also ingest the corresponding delta-lake table. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_profiling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful profiling. This will store profiling timestamps per dataset after successful profiling. and will not run profiling again in subsequent run if table has not been updated.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-main">extra_client_options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Additional options to pass to Databricks SQLAlchemy client. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">federation_structured_property_namespace</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Qualified-name prefix for the federation structured properties; each property is this prefix plus its suffix (e.g. 'databricks.federation.platform'). <div className="default-line default-line-with-docs">Default: <span className="default-value">databricks.federation</span></div> |
| <div className="path-line"><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ignore_start_time_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Option to ignore the start_time and retrieve all available lineage. When enabled, the start_time filter will be set to zero to extract all lineage events regardless of the configured time window. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Option to enable/disable lineage generation. Currently we have to call a rest call per column to get column level lineage due to the Databrick api which can slow down ingestion.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_column_usage_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, force full sqlglot parsing of usage queries so column-level usage statistics (fieldCounts) are produced. This bypasses the faster system-table preparsed lineage path, so usage extraction is slower. Only changes behavior on the system-tables usage path (the REST API path already parses every query). Takes precedence over push_down_database_pattern_access_history and skip_sqlglot_when_system_table_lineage_missing, which are ignored when set. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_external_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Option to enable/disable lineage generation for external tables. Only external S3 tables are supported at the moment. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_federation_column_backfill</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | For foreign (Lakehouse Federation) catalog tables whose columns Unity Catalog has not synced yet, resolve the schema from the already-ingested external source dataset via the DataHub graph. Requires a graph connection (e.g. a datahub-rest sink) and the external source to be ingested; otherwise it is a no-op. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_federation_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit an upstream COPY lineage edge from each Lakehouse Federation foreign catalog table to its external source dataset (with column-level lineage when include_column_lineage is set). Disable to skip the cross-platform link. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_hive_metastore</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest legacy `hive_metastore` catalog. This requires executing queries on SQL warehouse. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_metastore</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest the workspace's metastore as a container and include it in all urns. Changing this will affect the urns of all entities in the workspace. This config is deprecated and will be removed in the future, so it is recommended to not set this to `True` for new ingestions. If you have an existing unity catalog ingestion, you'll want to avoid duplicates by soft deleting existing data. If stateful ingestion is enabled, running with `include_metastore: false` should be sufficient. Otherwise, we recommend deleting via the cli: `datahub delete --platform databricks` and re-ingesting with `include_metastore: false`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_metric_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable enriched ingestion of Unity Catalog Metric Views: YAML body as ViewProperties, upstream and column-level lineage from `source` / `joins` / `dimensions.expr` / `measures.expr`, `Dimension` / `Measure` tags on matching columns, `materialization` → `ViewProperties.materialized`, and `filter` as a custom property. Metric views are always assigned the 'Metric View' subtype regardless of this flag (they are never plain Tables); this flag only controls the additional enrichment above. Requires a `databricks-sdk` recent enough to expose `TableType.METRIC_VIEW`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_ml_model_aliases</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to include ML model aliases in the ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_ml_models</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest ML models (MLModelGroups and MLModels) registered in Unity Catalog. Set to False to skip ML model discovery entirely — no calls are made to the registered-models API. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_notebooks</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest notebooks, represented as DataHub datasets. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Option to enable/disable ownership generation for metastores, catalogs, schemas, and tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_partition_keys</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, the `isPartitioningKey` field is populated on schema fields for columns that are part of the table's partition key. Partition key information is already present in the tables.list() response so no additional API calls are made. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, emit DataHub Query entities for the SQL statements seen in query history (the statement text and the datasets it reads/writes). Only effective on the system-tables usage path; identical statements are de-duplicated by fingerprint. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_query_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, emit per-query usage/popularity statistics (queryUsageStatistics) for the emitted Query entities. Only effective when include_queries is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report read operational stats. Experimental. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_table_constraints</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, fetches primary key and foreign key constraints for each table via an additional tables.get() API call per table (one call per table). Disabled by default to avoid unexpected API load on large catalogs. Enables PK/FK visualisation in the DataHub schema view when set to true. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_table_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Option to enable/disable lineage generation. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_location_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If the source supports it, include table lineage to the underlying storage location. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether tables should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Option to enable/disable column/table tag extraction. Requires warehouse_id to be set since tag extraction needs to query system.information_schema.tags. If warehouse_id is not provided, this will be automatically disabled to allow ingestion to continue. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_top_n_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest the top_n_queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Generate usage statistics. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser. Requires `include_view_lineage` to be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates view->view and table->view lineage using DataHub's sql parser. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether views should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">incremental_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits ownership as incremental to existing ownership in DataHub. When disabled, re-states ownership on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">incremental_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits dataset properties as incremental to existing dataset properties in DataHub. When disabled, re-states dataset properties on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_data_platform_instance_aspect</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Option to enable/disable ingestion of the data platform instance aspect. The default data platform instance id for a dataset is workspace_name <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">lineage_data_source</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "AUTO", "SYSTEM_TABLES", "API"  |
| <div className="path-line"><span className="path-main">ml_model_max_results</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum total number of ML models to ingest per schema. Set to 0 to ingest none. To disable ML model ingestion entirely (including API calls), use `include_ml_models: false` instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.  |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">push_down_database_pattern_access_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, pushes down catalog pattern filtering to system.access.table_lineage for improved performance during usage extraction. This filters on source and target catalogs in table_lineage. Maps to Snowflake's database_pattern semantics via catalog_pattern (Unity catalog = Snowflake database). Only applies when usage is fetched via system tables (usage_data_source AUTO with warehouse or SYSTEM_TABLES). Also adds a statement_id semi-join against table_lineage, so only queries that have at least one lineage row in the configured time window are fetched; queries without system.access.table_lineage rows are omitted entirely (not sqlglot-fallbacked). <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">scheme</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">databricks</span></div> |
| <div className="path-line"><span className="path-main">skip_sqlglot_when_system_table_lineage_missing</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled on the system-tables usage path, queries with no matching rows in system.access.table_lineage (for their statement_id within the configured time window) are skipped instead of parsed with sqlglot. Only applies when usage is fetched via system.query.history joined with system.access.table_lineage. Queries that have lineage but unresolvable dataset URNs still fall back to sqlglot. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Databricks personal access token <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">top_n_queries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of top queries to save to each table. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">usage_data_source</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "AUTO", "SYSTEM_TABLES", "API"  |
| <div className="path-line"><span className="path-main">use_file_backed_cache</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to use a file backed cache for the view definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">warehouse_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | SQL Warehouse id, for running queries. Must be explicitly provided to enable SQL-based features. Required for the following features that need SQL access: 1) Tag extraction (include_tags=True) - queries system.information_schema.tags 2) Hive Metastore catalog (include_hive_metastore=True) - queries legacy hive_metastore catalog 3) System table lineage (lineage_data_source=SYSTEM_TABLES) - queries system.access.table_lineage/column_lineage 4) Data profiling (profiling.enabled=True) - runs SELECT/ANALYZE queries on tables. When warehouse_id is missing, these features will be automatically disabled (with warnings) to allow ingestion to continue. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">workspace_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Name of the workspace. Default to deployment name present in workspace_url <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">azure_auth</span></div> <div className="type-name-line"><span className="type-name">One of AzureAuthConfig, null</span></div> | Azure configuration <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure_auth.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if azure_auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Azure application (client) ID. This is the unique identifier for the registered Azure AD application.  |
| <div className="path-line"><span className="path-prefix">azure_auth.</span><span className="path-main">client_secret</span>&nbsp;<abbr title="Required if azure_auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Azure application client secret used for authentication. This is a confidential credential that should be kept secure.  |
| <div className="path-line"><span className="path-prefix">azure_auth.</span><span className="path-main">tenant_id</span>&nbsp;<abbr title="Required if azure_auth is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Azure tenant (directory) ID. This identifies the Azure AD tenant where the application is registered.  |
| <div className="path-line"><span className="path-main">catalog_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">catalog_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">catalogs</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Fixed list of catalogs to ingest. If not specified, catalogs will be ingested based on `catalog_pattern`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">catalogs.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">delta_lake_options</span></div> <div className="type-name-line"><span className="type-name">DeltaLakeDetails</span></div> |   |
| <div className="path-line"><span className="path-prefix">delta_lake_options.</span><span className="path-main">platform_instance_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Delta-lake platform instance name <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">delta_lake_options.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Delta-lake environment <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">federation_connection_details</span></div> <div className="type-name-line"><span className="type-name">map(str,FederationConnectionDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">federation_connection_details.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Override the DataHub platform auto-detected from the Unity Catalog connection type (e.g. 'mssql', 'postgres'). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">federation_connection_details.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Lower-case the external URN's name. Enabled by default because DataHub SQL connectors lower-case identifiers; set to false for a connection whose external source was ingested case-sensitively so the URN still matches. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">federation_connection_details.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Override the remote database name (falls back to the foreign catalog's connection options). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">federation_connection_details.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | platform_instance the external source was ingested under. Must match for the lineage link to resolve. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">federation_connection_details.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | env of the external dataset (defaults to the source env). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">metric_view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">metric_view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">notebook_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">notebook_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">classification</span></div> <div className="type-name-line"><span className="type-name">ClassificationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether classification should be used to auto-detect glossary terms <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">info_type_to_term</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker processes to use for classification. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">4</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of sample values used for classification. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">classifiers</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#123;&#x27;type&#x27;: &#x27;datahub&#x27;, &#x27;config&#x27;: None&#125;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">classification.classifiers.</span><span className="path-main">DynamicTypedClassifierConfig</span></div> <div className="type-name-line"><span className="type-name">DynamicTypedClassifierConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.classifiers.DynamicTypedClassifierConfig.</span><span className="path-main">type</span>&nbsp;<abbr title="Required if DynamicTypedClassifierConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The type of the classifier to use. The built-in `datahub` classifier has been removed; register a custom classifier and reference its type here.  |
| <div className="path-line"><span className="path-prefix">classification.classifiers.DynamicTypedClassifierConfig.</span><span className="path-main">config</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | The configuration required for initializing the classifier. If not specified, uses defaults for classifer type. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">column_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">classification.column_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">classification.table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">One of UnityCatalogGEProfilerConfig, UnityCatalogAnalyzeProfilerConfig, UnityCatalogSQLAlchemyProfilerConfig</span></div> | Data profiling configuration <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;method&#x27;: &#x27;sqlalchemy&#x27;, &#x27;enabled&#x27;: False, &#x27;operat...</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">call_analyze</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to call ANALYZE TABLE as part of profile ingestion.If false, will ingest the results of the most recent ANALYZE TABLE call, if any. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">catch_exceptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">One of boolean, boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">field_sample_values_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Upper limit for number of sample values to collect for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of distinct values for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_value_frequencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for distinct value frequencies. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_histogram</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the histogram for numeric fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_mean_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the mean value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_median_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the median value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_quantiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the quantiles of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_sample_values</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the sample values for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_stddev_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the standard deviation of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null, union(anyOf), integer, null</span></div> | Max number of documents to profile. By default, profiles all documents. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_number_of_fields_to_profile</span></div> <div className="type-name-line"><span className="type-name">One of integer, null, union(anyOf), integer, null</span></div> | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_wait_secs</span></div> <div className="type-name-line"><span className="type-name">One of integer, null, integer, union(anyOf), integer, null</span></div> | Maximum time to wait for a table to be profiled. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">One of integer, integer</span></div> | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">method</span></div> <div className="type-name-line"><span className="type-name">One of string, string</span></div> | Const value: ge <div className="default-line default-line-with-docs">Default: <span className="default-value">ge</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">nested_field_max_depth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch). <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">One of integer, null, union(anyOf), integer, null</span></div> | Offset in documents to profile. By default, uses no offset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_datetime</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), null, union(anyOf), string(date-time), null</span></div> | If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_external_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile external tables. Only Snowflake and Redshift supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_if_updated_since_days</span></div> <div className="type-name-line"><span className="type-name">One of number, null, union(anyOf), number, null</span></div> | Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_nested_fields</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile complex types like structs, arrays and maps.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">One of boolean, boolean</span></div> | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_count_estimate_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null, union(anyOf), integer, null</span></div> | Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">5000000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_size_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null, union(anyOf), integer, null</span></div> | Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_combiner_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | *This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">report_dropped_profiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">turn_off_expensive_profiling_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">use_sampling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">warehouse_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null, union(anyOf), string, null, union(anyOf), string, null</span></div> | SQL Warehouse id, for running profiling queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">One of OperationConfig, OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">One of boolean, boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null, union(anyOf), integer, null, union(anyOf), integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null, union(anyOf), integer, null, union(anyOf), integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">pattern</span></div> <div className="type-name-line"><span className="type-name">One of AllowDenyPattern, AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profiling.pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null, union(anyOf), boolean, null, union(anyOf), boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">tags_to_ignore_sampling</span></div> <div className="type-name-line"><span className="type-name">One of array, null, union(anyOf), array, null</span></div> | Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.tags_to_ignore_sampling.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Unity Catalog Stateful Ingestion Config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "AzureAuthConfig": {
      "additionalProperties": false,
      "properties": {
        "client_secret": {
          "description": "Azure application client secret used for authentication. This is a confidential credential that should be kept secure.",
          "format": "password",
          "title": "Client Secret",
          "type": "string",
          "writeOnly": true
        },
        "client_id": {
          "description": "Azure application (client) ID. This is the unique identifier for the registered Azure AD application.",
          "title": "Client Id",
          "type": "string"
        },
        "tenant_id": {
          "description": "Azure tenant (directory) ID. This identifies the Azure AD tenant where the application is registered.",
          "title": "Tenant Id",
          "type": "string"
        }
      },
      "required": [
        "client_secret",
        "client_id",
        "tenant_id"
      ],
      "title": "AzureAuthConfig",
      "type": "object"
    },
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
    },
    "ClassificationConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether classification should be used to auto-detect glossary terms",
          "title": "Enabled",
          "type": "boolean"
        },
        "sample_size": {
          "default": 100,
          "description": "Number of sample values used for classification.",
          "title": "Sample Size",
          "type": "integer"
        },
        "max_workers": {
          "default": 4,
          "description": "Number of worker processes to use for classification. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "table_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter tables for classification. This is used in combination with other patterns in parent config. Specify regex to match the entire table name in `database.schema.table` format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
        },
        "column_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter columns for classification. This is used in combination with other patterns in parent config. Specify regex to match the column name in `database.schema.table.column` format."
        },
        "info_type_to_term": {
          "additionalProperties": {
            "type": "string"
          },
          "default": {},
          "description": "Optional mapping to provide glossary term identifier for info type",
          "title": "Info Type To Term",
          "type": "object"
        },
        "classifiers": {
          "default": [
            {
              "type": "datahub",
              "config": null
            }
          ],
          "description": "Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance.",
          "items": {
            "$ref": "#/$defs/DynamicTypedClassifierConfig"
          },
          "title": "Classifiers",
          "type": "array"
        }
      },
      "title": "ClassificationConfig",
      "type": "object"
    },
    "DeltaLakeDetails": {
      "additionalProperties": false,
      "properties": {
        "platform_instance_name": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Delta-lake platform instance name",
          "title": "Platform Instance Name"
        },
        "env": {
          "default": "PROD",
          "description": "Delta-lake environment",
          "title": "Env",
          "type": "string"
        }
      },
      "title": "DeltaLakeDetails",
      "type": "object"
    },
    "DynamicTypedClassifierConfig": {
      "additionalProperties": false,
      "properties": {
        "type": {
          "description": "The type of the classifier to use. The built-in `datahub` classifier has been removed; register a custom classifier and reference its type here.",
          "title": "Type",
          "type": "string"
        },
        "config": {
          "anyOf": [
            {},
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The configuration required for initializing the classifier. If not specified, uses defaults for classifer type.",
          "title": "Config"
        }
      },
      "required": [
        "type"
      ],
      "title": "DynamicTypedClassifierConfig",
      "type": "object"
    },
    "FederationConnectionDetail": {
      "additionalProperties": false,
      "properties": {
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Override the DataHub platform auto-detected from the Unity Catalog connection type (e.g. 'mssql', 'postgres').",
          "title": "Platform"
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
          "description": "platform_instance the external source was ingested under. Must match for the lineage link to resolve.",
          "title": "Platform Instance"
        },
        "env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "env of the external dataset (defaults to the source env).",
          "title": "Env"
        },
        "database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Override the remote database name (falls back to the foreign catalog's connection options).",
          "title": "Database"
        },
        "convert_urns_to_lowercase": {
          "default": true,
          "description": "Lower-case the external URN's name. Enabled by default because DataHub SQL connectors lower-case identifiers; set to false for a connection whose external source was ingested case-sensitively so the URN still matches.",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "FederationConnectionDetail",
      "type": "object"
    },
    "LineageDataSource": {
      "enum": [
        "AUTO",
        "SYSTEM_TABLES",
        "API"
      ],
      "title": "LineageDataSource",
      "type": "string"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
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
    "UnityCatalogAnalyzeProfilerConfig": {
      "additionalProperties": false,
      "properties": {
        "method": {
          "const": "analyze",
          "default": "analyze",
          "title": "Method",
          "type": "string"
        },
        "warehouse_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "SQL Warehouse id, for running profiling queries.",
          "title": "Warehouse Id"
        },
        "pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter tables for profiling during ingestion. Specify regex to match the `catalog.schema.table` format. Note that only tables allowed by the `table_pattern` will be considered."
        },
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "call_analyze": {
          "default": true,
          "description": "Whether to call ANALYZE TABLE as part of profile ingestion.If false, will ingest the results of the most recent ANALYZE TABLE call, if any.",
          "title": "Call Analyze",
          "type": "boolean"
        },
        "max_wait_secs": {
          "default": 3600,
          "description": "Maximum time to wait for an ANALYZE TABLE query to complete.",
          "title": "Max Wait Secs",
          "type": "integer"
        },
        "max_workers": {
          "default": 20,
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        }
      },
      "title": "UnityCatalogAnalyzeProfilerConfig",
      "type": "object"
    },
    "UnityCatalogGEProfilerConfig": {
      "additionalProperties": false,
      "properties": {
        "method": {
          "const": "ge",
          "default": "ge",
          "title": "Method",
          "type": "string"
        },
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        },
        "limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Max number of documents to profile. By default, profiles all documents.",
          "title": "Limit"
        },
        "offset": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Offset in documents to profile. By default, uses no offset.",
          "title": "Offset"
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only, or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "include_field_null_count": {
          "default": true,
          "description": "Whether to profile for the number of nulls for each column.",
          "title": "Include Field Null Count",
          "type": "boolean"
        },
        "include_field_distinct_count": {
          "default": true,
          "description": "Whether to profile for the number of distinct values for each column.",
          "title": "Include Field Distinct Count",
          "type": "boolean"
        },
        "include_field_min_value": {
          "default": true,
          "description": "Whether to profile for the min value of numeric columns.",
          "title": "Include Field Min Value",
          "type": "boolean"
        },
        "include_field_max_value": {
          "default": true,
          "description": "Whether to profile for the max value of numeric columns.",
          "title": "Include Field Max Value",
          "type": "boolean"
        },
        "include_field_mean_value": {
          "default": true,
          "description": "Whether to profile for the mean value of numeric columns.",
          "title": "Include Field Mean Value",
          "type": "boolean"
        },
        "include_field_median_value": {
          "default": true,
          "description": "Whether to profile for the median value of numeric columns.",
          "title": "Include Field Median Value",
          "type": "boolean"
        },
        "include_field_stddev_value": {
          "default": true,
          "description": "Whether to profile for the standard deviation of numeric columns.",
          "title": "Include Field Stddev Value",
          "type": "boolean"
        },
        "include_field_quantiles": {
          "default": false,
          "description": "Whether to profile for the quantiles of numeric columns.",
          "title": "Include Field Quantiles",
          "type": "boolean"
        },
        "include_field_distinct_value_frequencies": {
          "default": false,
          "description": "Whether to profile for distinct value frequencies.",
          "title": "Include Field Distinct Value Frequencies",
          "type": "boolean"
        },
        "include_field_histogram": {
          "default": false,
          "description": "Whether to profile for the histogram for numeric fields.",
          "title": "Include Field Histogram",
          "type": "boolean"
        },
        "include_field_sample_values": {
          "default": true,
          "description": "Whether to profile for the sample values for all columns.",
          "title": "Include Field Sample Values",
          "type": "boolean"
        },
        "max_workers": {
          "default": 20,
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "report_dropped_profiles": {
          "default": false,
          "description": "Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes.",
          "title": "Report Dropped Profiles",
          "type": "boolean"
        },
        "turn_off_expensive_profiling_metrics": {
          "default": false,
          "description": "Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.",
          "title": "Turn Off Expensive Profiling Metrics",
          "type": "boolean"
        },
        "field_sample_values_limit": {
          "default": 20,
          "description": "Upper limit for number of sample values to collect for all columns.",
          "title": "Field Sample Values Limit",
          "type": "integer"
        },
        "max_number_of_fields_to_profile": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
          "title": "Max Number Of Fields To Profile"
        },
        "profile_if_updated_since_days": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "dremio"
            ]
          },
          "title": "Profile If Updated Since Days"
        },
        "profile_table_size_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5,
          "description": "Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "unity-catalog",
              "oracle",
              "teradata"
            ]
          },
          "title": "Profile Table Size Limit"
        },
        "profile_table_row_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5000000,
          "description": "Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "oracle"
            ]
          },
          "title": "Profile Table Row Limit"
        },
        "profile_table_row_count_estimate_only": {
          "default": false,
          "description": "Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. ",
          "schema_extra": {
            "supported_sources": [
              "postgres",
              "mysql"
            ]
          },
          "title": "Profile Table Row Count Estimate Only",
          "type": "boolean"
        },
        "query_combiner_enabled": {
          "default": true,
          "description": "*This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.",
          "title": "Query Combiner Enabled",
          "type": "boolean"
        },
        "catch_exceptions": {
          "default": true,
          "description": "",
          "title": "Catch Exceptions",
          "type": "boolean"
        },
        "partition_profiling_enabled": {
          "default": true,
          "description": "Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling.",
          "schema_extra": {
            "supported_sources": [
              "athena",
              "bigquery"
            ]
          },
          "title": "Partition Profiling Enabled",
          "type": "boolean"
        },
        "partition_datetime": {
          "anyOf": [
            {
              "format": "date-time",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this.",
          "schema_extra": {
            "supported_sources": [
              "bigquery"
            ]
          },
          "title": "Partition Datetime"
        },
        "use_sampling": {
          "default": true,
          "description": "Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables. ",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Use Sampling",
          "type": "boolean"
        },
        "sample_size": {
          "default": 10000,
          "description": "Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True.",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Sample Size",
          "type": "integer"
        },
        "profile_external_tables": {
          "default": false,
          "description": "Whether to profile external tables. Only Snowflake and Redshift supports this.",
          "schema_extra": {
            "supported_sources": [
              "redshift",
              "snowflake"
            ]
          },
          "title": "Profile External Tables",
          "type": "boolean"
        },
        "tags_to_ignore_sampling": {
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
          "description": "Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`.",
          "title": "Tags To Ignore Sampling"
        },
        "profile_nested_fields": {
          "default": false,
          "description": "Whether to profile complex types like structs, arrays and maps. ",
          "title": "Profile Nested Fields",
          "type": "boolean"
        },
        "nested_field_max_depth": {
          "default": 10,
          "description": "Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch).",
          "exclusiveMinimum": 0,
          "title": "Nested Field Max Depth",
          "type": "integer"
        },
        "warehouse_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "SQL Warehouse id, for running profiling queries.",
          "title": "Warehouse Id"
        },
        "pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter tables for profiling during ingestion. Specify regex to match the `catalog.schema.table` format. Note that only tables allowed by the `table_pattern` will be considered."
        },
        "max_wait_secs": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Maximum time to wait for a table to be profiled.",
          "title": "Max Wait Secs"
        }
      },
      "title": "UnityCatalogGEProfilerConfig",
      "type": "object"
    },
    "UnityCatalogSQLAlchemyProfilerConfig": {
      "additionalProperties": false,
      "properties": {
        "method": {
          "const": "sqlalchemy",
          "default": "sqlalchemy",
          "title": "Method",
          "type": "string"
        },
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        },
        "limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Max number of documents to profile. By default, profiles all documents.",
          "title": "Limit"
        },
        "offset": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Offset in documents to profile. By default, uses no offset.",
          "title": "Offset"
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only, or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "include_field_null_count": {
          "default": true,
          "description": "Whether to profile for the number of nulls for each column.",
          "title": "Include Field Null Count",
          "type": "boolean"
        },
        "include_field_distinct_count": {
          "default": true,
          "description": "Whether to profile for the number of distinct values for each column.",
          "title": "Include Field Distinct Count",
          "type": "boolean"
        },
        "include_field_min_value": {
          "default": true,
          "description": "Whether to profile for the min value of numeric columns.",
          "title": "Include Field Min Value",
          "type": "boolean"
        },
        "include_field_max_value": {
          "default": true,
          "description": "Whether to profile for the max value of numeric columns.",
          "title": "Include Field Max Value",
          "type": "boolean"
        },
        "include_field_mean_value": {
          "default": true,
          "description": "Whether to profile for the mean value of numeric columns.",
          "title": "Include Field Mean Value",
          "type": "boolean"
        },
        "include_field_median_value": {
          "default": true,
          "description": "Whether to profile for the median value of numeric columns.",
          "title": "Include Field Median Value",
          "type": "boolean"
        },
        "include_field_stddev_value": {
          "default": true,
          "description": "Whether to profile for the standard deviation of numeric columns.",
          "title": "Include Field Stddev Value",
          "type": "boolean"
        },
        "include_field_quantiles": {
          "default": false,
          "description": "Whether to profile for the quantiles of numeric columns.",
          "title": "Include Field Quantiles",
          "type": "boolean"
        },
        "include_field_distinct_value_frequencies": {
          "default": false,
          "description": "Whether to profile for distinct value frequencies.",
          "title": "Include Field Distinct Value Frequencies",
          "type": "boolean"
        },
        "include_field_histogram": {
          "default": false,
          "description": "Whether to profile for the histogram for numeric fields.",
          "title": "Include Field Histogram",
          "type": "boolean"
        },
        "include_field_sample_values": {
          "default": true,
          "description": "Whether to profile for the sample values for all columns.",
          "title": "Include Field Sample Values",
          "type": "boolean"
        },
        "max_workers": {
          "default": 20,
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "report_dropped_profiles": {
          "default": false,
          "description": "Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes.",
          "title": "Report Dropped Profiles",
          "type": "boolean"
        },
        "turn_off_expensive_profiling_metrics": {
          "default": false,
          "description": "Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.",
          "title": "Turn Off Expensive Profiling Metrics",
          "type": "boolean"
        },
        "field_sample_values_limit": {
          "default": 20,
          "description": "Upper limit for number of sample values to collect for all columns.",
          "title": "Field Sample Values Limit",
          "type": "integer"
        },
        "max_number_of_fields_to_profile": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
          "title": "Max Number Of Fields To Profile"
        },
        "profile_if_updated_since_days": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "dremio"
            ]
          },
          "title": "Profile If Updated Since Days"
        },
        "profile_table_size_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5,
          "description": "Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "unity-catalog",
              "oracle",
              "teradata"
            ]
          },
          "title": "Profile Table Size Limit"
        },
        "profile_table_row_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5000000,
          "description": "Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "oracle"
            ]
          },
          "title": "Profile Table Row Limit"
        },
        "profile_table_row_count_estimate_only": {
          "default": false,
          "description": "Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. ",
          "schema_extra": {
            "supported_sources": [
              "postgres",
              "mysql"
            ]
          },
          "title": "Profile Table Row Count Estimate Only",
          "type": "boolean"
        },
        "query_combiner_enabled": {
          "default": true,
          "description": "*This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.",
          "title": "Query Combiner Enabled",
          "type": "boolean"
        },
        "catch_exceptions": {
          "default": true,
          "description": "",
          "title": "Catch Exceptions",
          "type": "boolean"
        },
        "partition_profiling_enabled": {
          "default": true,
          "description": "Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling.",
          "schema_extra": {
            "supported_sources": [
              "athena",
              "bigquery"
            ]
          },
          "title": "Partition Profiling Enabled",
          "type": "boolean"
        },
        "partition_datetime": {
          "anyOf": [
            {
              "format": "date-time",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this.",
          "schema_extra": {
            "supported_sources": [
              "bigquery"
            ]
          },
          "title": "Partition Datetime"
        },
        "use_sampling": {
          "default": true,
          "description": "Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables. ",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Use Sampling",
          "type": "boolean"
        },
        "sample_size": {
          "default": 10000,
          "description": "Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True.",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Sample Size",
          "type": "integer"
        },
        "profile_external_tables": {
          "default": false,
          "description": "Whether to profile external tables. Only Snowflake and Redshift supports this.",
          "schema_extra": {
            "supported_sources": [
              "redshift",
              "snowflake"
            ]
          },
          "title": "Profile External Tables",
          "type": "boolean"
        },
        "tags_to_ignore_sampling": {
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
          "description": "Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`.",
          "title": "Tags To Ignore Sampling"
        },
        "profile_nested_fields": {
          "default": false,
          "description": "Whether to profile complex types like structs, arrays and maps. ",
          "title": "Profile Nested Fields",
          "type": "boolean"
        },
        "nested_field_max_depth": {
          "default": 10,
          "description": "Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch).",
          "exclusiveMinimum": 0,
          "title": "Nested Field Max Depth",
          "type": "integer"
        },
        "warehouse_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "SQL Warehouse id, for running profiling queries.",
          "title": "Warehouse Id"
        },
        "pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter tables for profiling during ingestion. Specify regex to match the `catalog.schema.table` format. Note that only tables allowed by the `table_pattern` will be considered."
        },
        "max_wait_secs": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Maximum time to wait for a table to be profiled.",
          "title": "Max Wait Secs"
        }
      },
      "title": "UnityCatalogSQLAlchemyProfilerConfig",
      "type": "object"
    },
    "UsageDataSource": {
      "enum": [
        "AUTO",
        "SYSTEM_TABLES",
        "API"
      ],
      "title": "UsageDataSource",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "properties": {
    "incremental_properties": {
      "default": false,
      "description": "When enabled, emits dataset properties as incremental to existing dataset properties in DataHub. When disabled, re-states dataset properties on each run.",
      "title": "Incremental Properties",
      "type": "boolean"
    },
    "incremental_ownership": {
      "default": false,
      "description": "When enabled, emits ownership as incremental to existing ownership in DataHub. When disabled, re-states ownership on each run.",
      "title": "Incremental Ownership",
      "type": "boolean"
    },
    "schema_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for schemas to filter in ingestion. Specify regex to the full `metastore.catalog.schema` name. e.g. to match all tables in schema analytics, use the regex `^mymetastore\\.mycatalog\\.analytics$`."
    },
    "table_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in `catalog.schema.table` format. e.g. to match all tables starting with customer in Customer catalog and public schema, use the regex `Customer\\.public\\.customer.*`."
    },
    "view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "classification": {
      "$ref": "#/$defs/ClassificationConfig",
      "default": {
        "enabled": false,
        "sample_size": 100,
        "max_workers": 4,
        "table_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "column_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "info_type_to_term": {},
        "classifiers": [
          {
            "config": null,
            "type": "datahub"
          }
        ]
      },
      "description": "For details, refer to [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md)."
    },
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
    "enable_stateful_profiling": {
      "default": true,
      "description": "Enable stateful profiling. This will store profiling timestamps per dataset after successful profiling. and will not run profiling again in subsequent run if table has not been updated. ",
      "title": "Enable Stateful Profiling",
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
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "bucket_duration": {
      "$ref": "#/$defs/BucketDuration",
      "default": "DAY",
      "description": "Size of the time window to aggregate usage stats."
    },
    "end_time": {
      "description": "Latest date of lineage/usage to consider. Default: Current time in UTC",
      "format": "date-time",
      "title": "End Time",
      "type": "string"
    },
    "start_time": {
      "default": null,
      "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
      "format": "date-time",
      "title": "Start Time",
      "type": "string"
    },
    "top_n_queries": {
      "default": 10,
      "description": "Number of top queries to save to each table.",
      "exclusiveMinimum": 0,
      "title": "Top N Queries",
      "type": "integer"
    },
    "user_email_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for user emails to filter in usage."
    },
    "include_operational_stats": {
      "default": true,
      "description": "Whether to display operational stats.",
      "title": "Include Operational Stats",
      "type": "boolean"
    },
    "include_read_operational_stats": {
      "default": false,
      "description": "Whether to report read operational stats. Experimental.",
      "title": "Include Read Operational Stats",
      "type": "boolean"
    },
    "format_sql_queries": {
      "default": false,
      "description": "Whether to format sql queries",
      "title": "Format Sql Queries",
      "type": "boolean"
    },
    "include_top_n_queries": {
      "default": true,
      "description": "Whether to ingest the top_n_queries.",
      "title": "Include Top N Queries",
      "type": "boolean"
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
      "description": "Unity Catalog Stateful Ingestion Config."
    },
    "options": {
      "additionalProperties": true,
      "description": "Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.",
      "title": "Options",
      "type": "object"
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
      "description": "Regex patterns to filter tables (or specific columns) for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered."
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "default": {},
      "description": "Attach domains to catalogs, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like \"Marketing\".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.",
      "title": "Domain",
      "type": "object"
    },
    "include_views": {
      "default": true,
      "description": "Whether views should be ingested.",
      "title": "Include Views",
      "type": "boolean"
    },
    "include_tables": {
      "default": true,
      "description": "Whether tables should be ingested.",
      "title": "Include Tables",
      "type": "boolean"
    },
    "include_table_location_lineage": {
      "default": true,
      "description": "If the source supports it, include table lineage to the underlying storage location.",
      "title": "Include Table Location Lineage",
      "type": "boolean"
    },
    "include_view_lineage": {
      "default": true,
      "description": "Populates view->view and table->view lineage using DataHub's sql parser.",
      "title": "Include View Lineage",
      "type": "boolean"
    },
    "include_view_column_lineage": {
      "default": true,
      "description": "Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser. Requires `include_view_lineage` to be enabled.",
      "title": "Include View Column Lineage",
      "type": "boolean"
    },
    "use_file_backed_cache": {
      "default": true,
      "description": "Whether to use a file backed cache for the view definitions.",
      "title": "Use File Backed Cache",
      "type": "boolean"
    },
    "profiling": {
      "default": {
        "method": "sqlalchemy",
        "enabled": false,
        "operation_config": {
          "lower_freq_profile_enabled": false,
          "profile_date_of_month": null,
          "profile_day_of_week": null
        },
        "limit": null,
        "offset": null,
        "profile_table_level_only": false,
        "include_field_null_count": true,
        "include_field_distinct_count": true,
        "include_field_min_value": true,
        "include_field_max_value": true,
        "include_field_mean_value": true,
        "include_field_median_value": true,
        "include_field_stddev_value": true,
        "include_field_quantiles": false,
        "include_field_distinct_value_frequencies": false,
        "include_field_histogram": false,
        "include_field_sample_values": true,
        "max_workers": 20,
        "report_dropped_profiles": false,
        "turn_off_expensive_profiling_metrics": false,
        "field_sample_values_limit": 20,
        "max_number_of_fields_to_profile": null,
        "profile_if_updated_since_days": null,
        "profile_table_size_limit": 5,
        "profile_table_row_limit": 5000000,
        "profile_table_row_count_estimate_only": false,
        "query_combiner_enabled": true,
        "catch_exceptions": true,
        "partition_profiling_enabled": true,
        "partition_datetime": null,
        "use_sampling": true,
        "sample_size": 10000,
        "profile_external_tables": false,
        "tags_to_ignore_sampling": null,
        "profile_nested_fields": false,
        "nested_field_max_depth": 10,
        "warehouse_id": null,
        "pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "max_wait_secs": null
      },
      "description": "Data profiling configuration",
      "discriminator": {
        "mapping": {
          "analyze": "#/$defs/UnityCatalogAnalyzeProfilerConfig",
          "ge": "#/$defs/UnityCatalogGEProfilerConfig",
          "sqlalchemy": "#/$defs/UnityCatalogSQLAlchemyProfilerConfig"
        },
        "propertyName": "method"
      },
      "oneOf": [
        {
          "$ref": "#/$defs/UnityCatalogGEProfilerConfig"
        },
        {
          "$ref": "#/$defs/UnityCatalogAnalyzeProfilerConfig"
        },
        {
          "$ref": "#/$defs/UnityCatalogSQLAlchemyProfilerConfig"
        }
      ],
      "title": "Profiling"
    },
    "scheme": {
      "default": "databricks",
      "title": "Scheme",
      "type": "string"
    },
    "token": {
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
      "description": "Databricks personal access token",
      "title": "Token"
    },
    "azure_auth": {
      "anyOf": [
        {
          "$ref": "#/$defs/AzureAuthConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Azure configuration"
    },
    "client_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Databricks service principal client ID",
      "title": "Client Id"
    },
    "client_secret": {
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
      "description": "Databricks service principal client secret",
      "title": "Client Secret"
    },
    "workspace_url": {
      "description": "Databricks workspace url. e.g. https://my-workspace.cloud.databricks.com",
      "title": "Workspace Url",
      "type": "string"
    },
    "warehouse_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "SQL Warehouse id, for running queries. Must be explicitly provided to enable SQL-based features. Required for the following features that need SQL access: 1) Tag extraction (include_tags=True) - queries system.information_schema.tags 2) Hive Metastore catalog (include_hive_metastore=True) - queries legacy hive_metastore catalog 3) System table lineage (lineage_data_source=SYSTEM_TABLES) - queries system.access.table_lineage/column_lineage 4) Data profiling (profiling.enabled=True) - runs SELECT/ANALYZE queries on tables. When warehouse_id is missing, these features will be automatically disabled (with warnings) to allow ingestion to continue.",
      "title": "Warehouse Id"
    },
    "extra_client_options": {
      "additionalProperties": true,
      "default": {},
      "description": "Additional options to pass to Databricks SQLAlchemy client.",
      "title": "Extra Client Options",
      "type": "object"
    },
    "include_metastore": {
      "default": false,
      "description": "Whether to ingest the workspace's metastore as a container and include it in all urns. Changing this will affect the urns of all entities in the workspace. This config is deprecated and will be removed in the future, so it is recommended to not set this to `True` for new ingestions. If you have an existing unity catalog ingestion, you'll want to avoid duplicates by soft deleting existing data. If stateful ingestion is enabled, running with `include_metastore: false` should be sufficient. Otherwise, we recommend deleting via the cli: `datahub delete --platform databricks` and re-ingesting with `include_metastore: false`.",
      "title": "Include Metastore",
      "type": "boolean"
    },
    "ingest_data_platform_instance_aspect": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Option to enable/disable ingestion of the data platform instance aspect. The default data platform instance id for a dataset is workspace_name",
      "title": "Ingest Data Platform Instance Aspect"
    },
    "catalogs": {
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
      "description": "Fixed list of catalogs to ingest. If not specified, catalogs will be ingested based on `catalog_pattern`.",
      "title": "Catalogs"
    },
    "catalog_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for catalogs to filter in ingestion. Specify regex to match the full `metastore.catalog` name."
    },
    "notebook_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for notebooks to filter in ingestion, based on notebook *path*. Specify regex to match the entire notebook path in `/<dir>/.../<name>` format. e.g. to match all notebooks in the root Shared directory, use the regex `/Shared/.*`."
    },
    "metric_view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for Unity Catalog Metric Views to filter in ingestion. Specify regex to match the full `catalog.schema.metric_view_name`. Only applies when `include_metric_views` is True."
    },
    "include_metric_views": {
      "default": false,
      "description": "Enable enriched ingestion of Unity Catalog Metric Views: YAML body as ViewProperties, upstream and column-level lineage from `source` / `joins` / `dimensions.expr` / `measures.expr`, `Dimension` / `Measure` tags on matching columns, `materialization` \u2192 `ViewProperties.materialized`, and `filter` as a custom property. Metric views are always assigned the 'Metric View' subtype regardless of this flag (they are never plain Tables); this flag only controls the additional enrichment above. Requires a `databricks-sdk` recent enough to expose `TableType.METRIC_VIEW`.",
      "title": "Include Metric Views",
      "type": "boolean"
    },
    "include_table_lineage": {
      "default": true,
      "description": "Option to enable/disable lineage generation.",
      "title": "Include Table Lineage",
      "type": "boolean"
    },
    "include_external_lineage": {
      "default": true,
      "description": "Option to enable/disable lineage generation for external tables. Only external S3 tables are supported at the moment.",
      "title": "Include External Lineage",
      "type": "boolean"
    },
    "include_notebooks": {
      "default": false,
      "description": "Ingest notebooks, represented as DataHub datasets.",
      "title": "Include Notebooks",
      "type": "boolean"
    },
    "include_ownership": {
      "default": false,
      "description": "Option to enable/disable ownership generation for metastores, catalogs, schemas, and tables.",
      "title": "Include Ownership",
      "type": "boolean"
    },
    "include_tags": {
      "default": true,
      "description": "Option to enable/disable column/table tag extraction. Requires warehouse_id to be set since tag extraction needs to query system.information_schema.tags. If warehouse_id is not provided, this will be automatically disabled to allow ingestion to continue.",
      "title": "Include Tags",
      "type": "boolean"
    },
    "include_column_lineage": {
      "default": true,
      "description": "Option to enable/disable lineage generation. Currently we have to call a rest call per column to get column level lineage due to the Databrick api which can slow down ingestion. ",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "include_federation_lineage": {
      "default": true,
      "description": "Emit an upstream COPY lineage edge from each Lakehouse Federation foreign catalog table to its external source dataset (with column-level lineage when include_column_lineage is set). Disable to skip the cross-platform link.",
      "title": "Include Federation Lineage",
      "type": "boolean"
    },
    "emit_federation_structured_properties": {
      "default": true,
      "description": "Define and assign structured properties marking foreign catalogs as federated (facetable in the UI).",
      "title": "Emit Federation Structured Properties",
      "type": "boolean"
    },
    "federation_structured_property_namespace": {
      "default": "databricks.federation",
      "description": "Qualified-name prefix for the federation structured properties; each property is this prefix plus its suffix (e.g. 'databricks.federation.platform').",
      "title": "Federation Structured Property Namespace",
      "type": "string"
    },
    "federation_connection_details": {
      "additionalProperties": {
        "$ref": "#/$defs/FederationConnectionDetail"
      },
      "description": "Per-connection overrides keyed by Unity Catalog connection name.",
      "title": "Federation Connection Details",
      "type": "object"
    },
    "include_federation_column_backfill": {
      "default": true,
      "description": "For foreign (Lakehouse Federation) catalog tables whose columns Unity Catalog has not synced yet, resolve the schema from the already-ingested external source dataset via the DataHub graph. Requires a graph connection (e.g. a datahub-rest sink) and the external source to be ingested; otherwise it is a no-op.",
      "title": "Include Federation Column Backfill",
      "type": "boolean"
    },
    "include_table_constraints": {
      "default": false,
      "description": "If enabled, fetches primary key and foreign key constraints for each table via an additional tables.get() API call per table (one call per table). Disabled by default to avoid unexpected API load on large catalogs. Enables PK/FK visualisation in the DataHub schema view when set to true.",
      "title": "Include Table Constraints",
      "type": "boolean"
    },
    "include_partition_keys": {
      "default": false,
      "description": "If enabled, the `isPartitioningKey` field is populated on schema fields for columns that are part of the table's partition key. Partition key information is already present in the tables.list() response so no additional API calls are made.",
      "title": "Include Partition Keys",
      "type": "boolean"
    },
    "lineage_data_source": {
      "$ref": "#/$defs/LineageDataSource",
      "default": "AUTO",
      "description": "Source for lineage data extraction. Options: 'AUTO' - Use system tables when SQL warehouse is available, fallback to API; 'SYSTEM_TABLES' - Force use of system.access.table_lineage and system.access.column_lineage tables (requires SQL warehouse); 'API' - Force use of REST API endpoints for lineage data"
    },
    "ignore_start_time_lineage": {
      "default": false,
      "description": "Option to ignore the start_time and retrieve all available lineage. When enabled, the start_time filter will be set to zero to extract all lineage events regardless of the configured time window.",
      "title": "Ignore Start Time Lineage",
      "type": "boolean"
    },
    "column_lineage_column_limit": {
      "default": 300,
      "description": "Limit the number of columns to get column level lineage. ",
      "title": "Column Lineage Column Limit",
      "type": "integer"
    },
    "databricks_api_page_size": {
      "default": 0,
      "description": "Page size for Databricks API calls when listing resources (catalogs, schemas, tables, etc.). When set to 0 (default), uses server-side configured page length (recommended). When set to a positive value, the page length is the minimum of this value and the server configured value. Must be a non-negative integer.",
      "minimum": 0,
      "title": "Databricks Api Page Size",
      "type": "integer"
    },
    "include_usage_statistics": {
      "default": true,
      "description": "Generate usage statistics.",
      "title": "Include Usage Statistics",
      "type": "boolean"
    },
    "usage_data_source": {
      "$ref": "#/$defs/UsageDataSource",
      "default": "AUTO",
      "description": "Source for usage/query history data extraction. Options: 'AUTO' (default) - Automatically use system.query.history table when SQL warehouse is configured, otherwise fall back to REST API. This provides better performance for multi-workspace setups and large query volumes when warehouse_id is set. 'SYSTEM_TABLES' - Force use of system.query.history table (requires SQL warehouse and SELECT permission on system.query.history). 'API' - Force use of REST API endpoints for query history (legacy method, may have limitations with multiple workspaces)."
    },
    "include_queries": {
      "default": true,
      "description": "If enabled, emit DataHub Query entities for the SQL statements seen in query history (the statement text and the datasets it reads/writes). Only effective on the system-tables usage path; identical statements are de-duplicated by fingerprint.",
      "title": "Include Queries",
      "type": "boolean"
    },
    "include_query_usage_statistics": {
      "default": true,
      "description": "If enabled, emit per-query usage/popularity statistics (queryUsageStatistics) for the emitted Query entities. Only effective when include_queries is enabled.",
      "title": "Include Query Usage Statistics",
      "type": "boolean"
    },
    "push_down_database_pattern_access_history": {
      "default": false,
      "description": "If enabled, pushes down catalog pattern filtering to system.access.table_lineage for improved performance during usage extraction. This filters on source and target catalogs in table_lineage. Maps to Snowflake's database_pattern semantics via catalog_pattern (Unity catalog = Snowflake database). Only applies when usage is fetched via system tables (usage_data_source AUTO with warehouse or SYSTEM_TABLES). Also adds a statement_id semi-join against table_lineage, so only queries that have at least one lineage row in the configured time window are fetched; queries without system.access.table_lineage rows are omitted entirely (not sqlglot-fallbacked).",
      "title": "Push Down Database Pattern Access History",
      "type": "boolean"
    },
    "skip_sqlglot_when_system_table_lineage_missing": {
      "default": false,
      "description": "If enabled on the system-tables usage path, queries with no matching rows in system.access.table_lineage (for their statement_id within the configured time window) are skipped instead of parsed with sqlglot. Only applies when usage is fetched via system.query.history joined with system.access.table_lineage. Queries that have lineage but unresolvable dataset URNs still fall back to sqlglot.",
      "title": "Skip Sqlglot When System Table Lineage Missing",
      "type": "boolean"
    },
    "include_column_usage_stats": {
      "default": false,
      "description": "If enabled, force full sqlglot parsing of usage queries so column-level usage statistics (fieldCounts) are produced. This bypasses the faster system-table preparsed lineage path, so usage extraction is slower. Only changes behavior on the system-tables usage path (the REST API path already parses every query). Takes precedence over push_down_database_pattern_access_history and skip_sqlglot_when_system_table_lineage_missing, which are ignored when set.",
      "title": "Include Column Usage Stats",
      "type": "boolean"
    },
    "emit_siblings": {
      "default": true,
      "description": "Whether to emit siblings relation with corresponding delta-lake platform's table. If enabled, this will also ingest the corresponding delta-lake table.",
      "title": "Emit Siblings",
      "type": "boolean"
    },
    "delta_lake_options": {
      "$ref": "#/$defs/DeltaLakeDetails",
      "default": {
        "platform_instance_name": null,
        "env": "PROD"
      },
      "description": "Details about the delta lake, incase to emit siblings"
    },
    "include_ml_models": {
      "default": true,
      "description": "Whether to ingest ML models (MLModelGroups and MLModels) registered in Unity Catalog. Set to False to skip ML model discovery entirely \u2014 no calls are made to the registered-models API.",
      "title": "Include Ml Models",
      "type": "boolean"
    },
    "include_ml_model_aliases": {
      "default": false,
      "description": "Whether to include ML model aliases in the ingestion.",
      "title": "Include Ml Model Aliases",
      "type": "boolean"
    },
    "ml_model_max_results": {
      "default": 1000,
      "description": "Maximum total number of ML models to ingest per schema. Set to 0 to ingest none. To disable ML model ingestion entirely (including API calls), use `include_ml_models: false` instead.",
      "minimum": 0,
      "title": "Ml Model Max Results",
      "type": "integer"
    },
    "include_hive_metastore": {
      "default": true,
      "description": "Whether to ingest legacy `hive_metastore` catalog. This requires executing queries on SQL warehouse.",
      "title": "Include Hive Metastore",
      "type": "boolean"
    },
    "workspace_name": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Name of the workspace. Default to deployment name present in workspace_url",
      "title": "Workspace Name"
    }
  },
  "required": [
    "workspace_url"
  ],
  "title": "UnityCatalogSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Metric Views

[Unity Catalog Metric Views](https://docs.databricks.com/aws/en/metric-views/) are first-class semantic layer assets that expose dimensions and measures via a YAML specification. DataHub ingests them as datasets with subtype `Metric View` when you opt in with `include_metric_views: true`.

```yaml
source:
  type: unity-catalog
  config:
    include_metric_views: true
    metric_view_pattern:
      allow:
        - my_catalog\.analytics\..*
```

When enabled, each metric view emits:

- A `Metric View` subtype so it is distinguishable from regular tables and views in the UI.
- A `ViewProperties` aspect carrying the raw YAML body with `viewLanguage: YAML`. The `materialized` flag is set when the YAML contains `materialization: materialized` (v0.1 string form) or any `materialization:` object (v1.1 form).
- The dataset description, taken from the YAML top-level `comment` when present (falls back to the underlying Unity Catalog table comment otherwise).
- Upstream lineage parsed from the YAML `source` and `joins[].source` fields. Both 3-part (`catalog.schema.table`) and 2-part (`schema.table`, resolved against the metric view's own catalog) identifiers are supported, as are backtick-quoted parts (`` `db.with.dots`.schema.table ``). Nested join hierarchies (snowflake-style joins) are walked recursively — every join target along the chain becomes an upstream, and every alias along the chain becomes resolvable in dimension and measure expressions.
- Column-level lineage parsed from each `dimensions[].expr` / `measures[].expr` using the Databricks SQL dialect (requires `include_column_lineage: true`, which is the default). Unqualified columns map to the source table; `<join_name>.column` references map through the join's `source`.
- Intra-view field-to-field lineage for `MEASURE(name)` composable measure references. A measure expressed as `MEASURE(total_revenue) / MEASURE(order_count)` emits two upstream edges to the `total_revenue` and `order_count` measures within the same metric view dataset. Matching is case-insensitive; the emitted URN uses the canonical case from the spec.
- A `Dimension` tag on schema fields matching a YAML `dimensions[].name`, and a `Measure` tag on those matching `measures[].name`. Measures with a non-empty `window:` block get an additional `Window Measure` tag alongside `Measure`.
- Per-column descriptions taken from the YAML `dimensions[].comment` / `measures[].comment` when present, with `description` accepted as a v0.1 fallback (falls back to the underlying Unity Catalog column comment otherwise).
- A filtered set of custom properties: the Spark engine config snapshot Unity Catalog injects as `view.sqlConfig.spark.*` keys (~150 entries per view) is dropped, since it is identical across views in a workspace and crowds the UI. All other source-table properties are preserved unchanged.

#### Metric view custom properties

The following spec-level properties are surfaced on the metric view dataset so the spec is inspectable without re-reading the YAML body:

- `metric_view.spec_version` — the spec version (e.g. `1.1`).
- `metric_view.filter` — the top-level `filter:` expression.
- `metric_view.joins` — the entire `joins:` hierarchy as a JSON string, preserving `on:` predicates, `using:` shorthand, and any nested joins.
- `metric_view.materialization.schedule` / `metric_view.materialization.mode` / `metric_view.materialization.materialized_views` — when `materialization:` is a v1.1 object, each subkey lands as a queryable property.

Per-field agent metadata (Databricks Runtime 17.3+, YAML 1.1) is exposed as dataset-level custom properties keyed `metric_view.field.<name>.*`:

- `display_name` — human-readable label for the field. Truncated to 255 characters; truncation events are counted in the ingestion report.
- `synonyms` — comma-joined alternative names. Each entry is limited to 255 characters and the list is capped at 10 entries per field (per the Databricks v1.1 spec). Per-item truncations, invalid-type drops, and the 10-cap are each recorded in the ingestion report.
- `format.type` and its subkeys (for dimensions and measures) — `number`, `currency`, `percentage`, `byte`, `date`, and `date_time` formats are supported. Each known subkey is exposed individually (including nested `decimal_places.type` and `decimal_places.places`); unknown subkeys are dropped and counted in the ingestion report.
- `window.order` / `window.range` / `window.semiadditive` for single-entry window measures, or `window` as a JSON string for multi-entry windows. Window properties are emitted on measures only.

If a metric view's `source` is a SQL subquery, or if it uses a 1-part identifier that DataHub can't resolve, the YAML lineage path is skipped and DataHub falls back to the Unity Catalog table-lineage REST API for upstream resolution.

`include_metric_views` is `false` by default for backwards compatibility — when the flag is off (or when the installed `databricks-sdk` predates `TableType.METRIC_VIEW`), metric views continue to be emitted as plain `Table` entities with no view body.

#### Usage statistics

Usage is enabled by default (`include_usage_statistics: true`). Choose how query history is read with `usage_data_source`:

- `AUTO` (default) — system tables when `warehouse_id` is set; otherwise the REST API.
- `SYSTEM_TABLES` — `system.query.history` only (requires `warehouse_id`).
- `API` — REST API only.

On the **system-tables path**, query history is joined to `system.access.table_lineage` on `statement_id`. When lineage rows exist, dataset references come from lineage; otherwise queries are parsed with sqlglot. Set `skip_sqlglot_when_system_table_lineage_missing: true` to skip queries with no lineage rows instead of parsing them.

- `include_operational_stats` (default `true`) — when `false`, only `SELECT` statements are fetched.
- `include_queries` / `include_query_usage_statistics` — emit Query entities and per-query popularity (system-tables path only).
- `include_column_usage_stats` (default `false`) — when `true`, force full sqlglot parsing of every usage query so column-level usage statistics (`fieldCounts`) are produced. This bypasses the faster preparsed system-table lineage path and is slower; it also overrides `push_down_database_pattern_access_history` and `skip_sqlglot_when_system_table_lineage_missing`.

`push_down_database_pattern_access_history: true` applies `catalog_pattern` filtering in `system.access.table_lineage` and semi-joins query history to statements that have lineage in the configured time window. Statements without lineage rows are not fetched (even when `catalog_pattern` allows all catalogs).

:::warning Coverage vs. speed tradeoff

`skip_sqlglot_when_system_table_lineage_missing` and `push_down_database_pattern_access_history` trade usage **coverage** for speed, not just parsing time. Databricks only records a `system.access.table_lineage` row for statements that emit a lineage event (typically a minority of warehouse/serverless queries) — `CREATE`, `DESCRIBE`, `SET`, and most other statements have no lineage row at all. Enabling either option therefore drops usage and operations for every statement that lacks lineage in the time window, which is usually the large majority of activity. They are off by default for this reason; leave them off unless you specifically want to restrict usage to the lineage-bearing subset in exchange for faster, lighter ingestion.

The default preparsed path emits table-level usage only (no column `fieldCounts`). Set `include_column_usage_stats: true` to regain column-level usage statistics via full sqlglot parsing at the cost of speed.

:::

#### Delta Lake External Tables

When `emit_siblings` is enabled (the default), the connector emits sibling relationships between Unity Catalog external tables and their corresponding `delta-lake` platform entities for tables stored on S3 or other object storage. This means you may see a second dataset entity for each external Delta table — one under the `databricks` platform and one under `delta-lake` — linked as siblings in DataHub. Set `emit_siblings: false` in your recipe to disable this behavior if you don't need cross-platform linkage.

#### Lakehouse Federation (foreign catalogs)

DataHub detects Unity Catalog **foreign catalogs** (Lakehouse Federation) and links their tables to the external source dataset each one mirrors (PostgreSQL, SQL Server, MySQL, Snowflake, Redshift, BigQuery, Oracle, Teradata, another Databricks workspace, or Glue/Hive).

- `include_federation_lineage` (default `true`) emits an upstream **COPY** lineage edge from each foreign-catalog table to the external source dataset it mirrors. Column-level lineage is added when `include_column_lineage` is set. Set it to `false` to skip the cross-platform link.
- `emit_federation_structured_properties` (default `true`) marks the foreign catalog with structured properties (`platform`, `remote_database`, `connection`, `catalog_type`) so federated catalogs are facetable in the UI.
- `include_federation_column_backfill` (default `true`) fills in a foreign-catalog table's columns from the external source when Unity Catalog has not synced them yet (structure only — governance is not copied).
- For the link to resolve, the external source must be ingested separately, and its `platform_instance` and case-folding must match. Use `federation_connection_details` (keyed by Unity Catalog connection name) to align them:

```yaml
source:
  type: unity-catalog
  config:
    include_federation_lineage: true
    federation_connection_details:
      pg_conn:
        platform_instance: prod-pg
        env: PROD
```

:::caution Dangling lineage to an un-ingested external source

The upstream lineage edge only resolves if the external source is **also ingested into DataHub** as its own recipe, using the exact same `platform_instance` (and `convert_urns_to_lowercase`) that you set in `federation_connection_details`. If the external source is never ingested, or is ingested with a different `platform_instance` or case-folding setting, the edge points at a dataset URN that DataHub never creates — a dangling external dataset that never reconciles with the real one.

:::

The `emit_siblings` option described under _Delta Lake External Tables_ above is unrelated: it governs only the Delta Lake (S3 external table) sibling path, not Lakehouse Federation.

#### Advanced

##### Multiple Databricks Workspaces

If you have multiple databricks workspaces **that point to the same Unity Catalog metastore**, our suggestion is to use separate recipes for ingesting the workspace-specific Hive Metastore catalog and Unity Catalog metastore's information schema.

To ingest Hive metastore information schema

- Setup one ingestion recipe per workspace
- Use platform instance equivalent to workspace name
- Ingest only hive_metastore catalog in the recipe using config `catalogs: ["hive_metastore"]`

To ingest Unity Catalog information schema

- Disable hive metastore catalog ingestion in the recipe using config `include_hive_metastore: False`
- Ideally, just ingest from one workspace
- To ingest from both workspaces (e.g. if each workspace has different permissions and therefore restricted view of the UC metastore):
  - Use same platform instance for all workspaces using same UC metastore
  - Ingest usage from only one workspace (you lose usage from other workspace)
  - Use filters to only ingest each catalog once, but shouldn’t be necessary

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

#### No data lineage captured or missing lineage

Check that you meet the [Unity Catalog lineage requirements](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements).

Also check the [Unity Catalog limitations](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#limitations) to make sure that lineage would be expected to exist in this case.

#### Lineage extraction is too slow

Unity Catalog REST API requires one call per table (table lineage) and one call per column (column lineage). To improve performance, disable column lineage with `include_column_lineage: false`.

Similarly, `include_table_constraints: true` adds one `tables.get()` call per non-Hive table to fetch primary key and foreign key constraints. For workspaces with thousands of tables this adds latency; leave the flag disabled (the default) if Primary Key / Foreign Key metadata is not needed.

#### Missing or incomplete usage statistics

- On the system-tables path, queries without rows in `system.access.table_lineage` are parsed with sqlglot unless `skip_sqlglot_when_system_table_lineage_missing: true`.
- With `push_down_database_pattern_access_history: true`, only statements with lineage in the time window are fetched. Disable pushdown or relax `catalog_pattern` if usage looks incomplete.


### Code Coordinates
- Class Name: `datahub.ingestion.source.unity.source.UnityCatalogSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/unity/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Databricks, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
