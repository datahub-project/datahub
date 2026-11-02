### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

:::note Not Azure Fabric
This connector is for **Azure Data Factory** (classic), not Azure Fabric's Data Factory. Azure Fabric support is planned for a future release.
:::

#### Authentication Methods

The connector supports multiple authentication methods:

| Method                     | Best For                                         | Configuration                                       |
| -------------------------- | ------------------------------------------------ | --------------------------------------------------- |
| **Service Principal**      | Production environments                          | `authentication_method: service_principal`          |
| **Managed Identity**       | Azure-hosted deployments (VMs, AKS, App Service) | `authentication_method: managed_identity`           |
| **Azure CLI**              | Local development                                | `authentication_method: cli` (run `az login` first) |
| **DefaultAzureCredential** | Flexible environments                            | `authentication_method: default`                    |

For service principal setup, see [Register an application with Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app).

#### Lineage Extraction

##### Which Activities Produce Lineage?

The connector extracts **table-level lineage** from these ADF activity types:

| Activity Type       | Lineage Behavior                                           |
| ------------------- | ---------------------------------------------------------- |
| **Copy Activity**   | Creates lineage from input dataset(s) to output dataset    |
| **Data Flow**       | Extracts sources, sinks, and transformation script         |
| **Lookup Activity** | Creates input lineage from the lookup dataset              |
| **ExecutePipeline** | Creates pipeline-to-pipeline lineage to the child pipeline |

Lineage is enabled by default (`include_lineage: true`).

##### How Lineage Resolution Works

For lineage to connect properly to datasets ingested from other sources (e.g., Snowflake, BigQuery), the connector needs to know which DataHub platform each ADF linked service corresponds to.

**Step 1: Automatic Platform Mapping**

The connector automatically maps ADF linked service types to DataHub platforms. For example, a `Snowflake` linked service maps to the `snowflake` platform.

<details>
<summary>View all supported linked service mappings</summary>

| ADF Linked Service Type  | DataHub Platform |
| ------------------------ | ---------------- |
| AzureBlobStorage         | `abs`            |
| AzureBlobFS              | `abs`            |
| AzureDataLakeStore       | `abs`            |
| AzureFileStorage         | `abs`            |
| AzureSqlDatabase         | `mssql`          |
| AzureSqlDW               | `mssql`          |
| AzureSynapseAnalytics    | `mssql`          |
| AzureSqlMI               | `mssql`          |
| SqlServer                | `mssql`          |
| AmazonRdsForSqlServer    | `mssql`          |
| AzureDatabricks          | `databricks`     |
| AzureDatabricksDeltaLake | `databricks`     |
| AmazonS3                 | `s3`             |
| AmazonS3Compatible       | `s3`             |
| AmazonRedshift           | `redshift`       |
| GoogleCloudStorage       | `gcs`            |
| GoogleBigQuery           | `bigquery`       |
| GoogleBigQueryV2         | `bigquery`       |
| GoogleSheets             | `google_sheets`  |
| Snowflake                | `snowflake`      |
| SnowflakeV2              | `snowflake`      |
| PostgreSql               | `postgres`       |
| PostgreSqlV2             | `postgres`       |
| AzurePostgreSql          | `postgres`       |
| MySql                    | `mysql`          |
| AzureMySql               | `mysql`          |
| MariaDB                  | `mariadb`        |
| AzureMariaDB             | `mariadb`        |
| Oracle                   | `oracle`         |
| OracleServiceCloud       | `oracle`         |
| AmazonRdsForOracle       | `oracle`         |
| Db2                      | `db2`            |
| Teradata                 | `teradata`       |
| Vertica                  | `vertica`        |
| Informix                 | `informix`       |
| Presto                   | `presto`         |
| Cassandra                | `cassandra`      |
| Couchbase                | `couchbase`      |
| SapHana                  | `hana`           |
| MongoDb                  | `mongodb`        |
| MongoDbV2                | `mongodb`        |
| MongoDbAtlas             | `mongodb`        |
| CosmosDbMongoDbApi       | `mongodb`        |
| Hive                     | `hive`           |
| Spark                    | `spark`          |
| Hdfs                     | `hdfs`           |
| Salesforce               | `salesforce`     |
| SalesforceServiceCloud   | `salesforce`     |
| SalesforceServiceCloudV2 | `salesforce`     |
| SalesforceMarketingCloud | `salesforce`     |
| SalesforceV2             | `salesforce`     |

Unsupported linked service types log a warning and skip lineage for that dataset.

</details>

**Step 2: Platform Instance Mapping (for cross-recipe lineage)**

If you're ingesting the same data sources with other DataHub connectors (e.g., Snowflake, BigQuery), you need to ensure the `platform_instance` values match. Use `platform_instance_map` to map your ADF linked service names to the platform instance used in your other recipes:

```yaml
# ADF Recipe
source:
  type: azure-data-factory
  config:
    subscription_id: ${AZURE_SUBSCRIPTION_ID}
    platform_instance_map:
      # Key: Your ADF linked service name (exact match required)
      # Value: The platform_instance from your other source recipe
      "snowflake-prod-connection": "prod_warehouse"
      "bigquery-analytics": "analytics_project"
```

```yaml
# Corresponding Snowflake Recipe (platform_instance must match)
source:
  type: snowflake
  config:
    platform_instance: "prod_warehouse" # Must match the value in platform_instance_map
    # ... other config
```

Without matching `platform_instance` values, lineage will create separate dataset entities instead of connecting to your existing ingested datasets.

**Step 3: Databricks Catalog and Metastore**

ADF's own API never exposes a Databricks table's catalog or Unity Catalog metastore — only its schema and table name. To fill this gap:

- **`platform_instance`** is left unset for Databricks by default, matching a default Unity Catalog recipe. When a `databricks_catalog_map` entry sets `metastore` (your Unity Catalog source uses `include_metastore`), it's derived from the linked service's workspace URL (e.g. `adb-1234567890123456`), as that source does. Use `platform_instance_map` if your Databricks recipe sets its own `platform_instance` or `workspace_name`; an empty value turns the derived one off.
- **`databricks_default_catalog`** fully qualifies every Databricks reference with a single catalog name, for tenants with one workspace/catalog.
- **`databricks_catalog_map`** maps individual linked service names to their own catalog (and optionally Unity Catalog metastore name), for tenants with multiple Databricks workspaces that don't share a catalog:

```yaml
source:
  type: azure-data-factory
  config:
    subscription_id: ${AZURE_SUBSCRIPTION_ID}
    databricks_catalog_map:
      # Key: Your ADF linked service name (exact match required)
      "MyDatabricksLS":
        catalog: "prod_catalog"
        # Only set metastore if your Unity Catalog source was ingested
        # with include_metastore enabled.
        metastore: "prod_metastore"
```

Without one of these set, Databricks references are left as `schema.table` rather than guessing a catalog that may not match reality.

##### Data Flow Transformation Scripts

For Data Flow activities, the connector extracts the transformation script and stores it in the `dataTransformLogic` aspect, visible in the DataHub UI under activity details.

##### Column-Level Lineage

The connector extracts **column-level lineage** from Copy activities, enabled by default (`include_column_lineage: true`).

**Supported Mapping Formats**

| Format                | Description                                                                 | ADF Configuration                                       |
| --------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------- |
| **Dictionary Format** | Legacy format with direct source-to-sink column mapping                     | `translator.columnMappings: {"src_col": "sink_col"}`    |
| **List Format**       | Current format with structured source/sink objects                          | `translator.mappings: [{source: {name}, sink: {name}}]` |
| **Auto-mapping**      | Inferred 1:1 mappings when no explicit mappings and source schema available | TabularTranslator with no columnMappings or mappings    |

**Column Lineage for Dynamic (ForEach-Looped) Copy Activities**

The mapping formats above rely on a static ADF dataset schema, which a fully dynamic Copy activity (e.g. a `ForEach` loop that copies whichever table `@item()` resolves to) never has - the same reason table-level lineage for these needs execution history (see [How Lineage Resolution Works](#how-lineage-resolution-works) above). When `include_execution_history` is enabled, the connector additionally parses the resolved query text recorded on each `ActivityRun` (the only place a per-iteration value is ever exposed) to recover column-level lineage for that specific run's real source and destination table, assuming a same-name column copy - the same default ADF itself uses when no explicit translator is configured. This is skipped (not guessed) when the query uses `SELECT *` (no live schema access to expand it) or when the activity has its own explicit translator (which takes precedence).

**Limitations**

- **Copy Activity Only**: Column lineage is currently extracted only from Copy activities. Other activity types (Data Flow, Lookup, etc.) produce table-level lineage only.
- **Schema Availability**: Auto-mapping inference requires source dataset schema information (defined in ADF dataset's `schema` or `structure` property). If schema is unavailable, only explicit mappings are extracted, unless the dynamic per-iteration recovery above applies.

#### Execution History

Pipeline runs are extracted as `DataProcessInstance` entities by default:

```yaml
source:
  type: azure-data-factory
  config:
    include_execution_history: true # default
    execution_history_days: 7 # 1-90 days
```

This provides run status, duration, timestamps, trigger info, parameters, and activity-level details.

#### Advanced: Multi-Environment Setup

##### When to Use `platform_instance`

Use the ADF connector's `platform_instance` config to distinguish **separate ADF deployments** when ingesting from multiple subscriptions or tenants:

| Scenario               | Risk                           | Solution     |
| ---------------------- | ------------------------------ | ------------ |
| Single subscription    | None                           | Not needed   |
| Multiple subscriptions | Low                            | Recommended  |
| Multiple tenants       | **High** - name collision risk | **Required** |

```yaml
# Multi-tenant example
source:
  type: azure-data-factory
  config:
    subscription_id: "tenant-a-sub"
    platform_instance: "tenant-a" # Prevents URN collisions
```

:::warning
Factory names are unique within Azure, but different tenants could have identically-named factories. Use `platform_instance` to prevent entity overwrites.
:::

##### URN Format

Pipeline URNs follow this format:

```
urn:li:dataFlow:(azure-data-factory,{factory_name}.{pipeline_name},{env})
```

With `platform_instance`:

```
urn:li:dataFlow:(azure-data-factory,{platform_instance}.{factory_name}.{pipeline_name},{env})
```

For Azure naming rules, see [Azure Data Factory naming rules](https://learn.microsoft.com/en-us/azure/data-factory/naming-rules).

#### Probe support

`datahub recipe probe` reads the Data Factory management API with the recipe's credential and answers the way ingestion will, so you can check scope and lineage resolution before a run. See the [probe interface guide](https://docs.datahub.com/docs/metadata-ingestion/docs/dev_guides/probe_interface) for the general workflow.

| Command           | Params                | Returns                                                                                                                                                            |
| ----------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `factories`       | `limit`               | every factory in scope (narrowed by `resource_group` when set), including those `factory_pattern` excludes, with its resource group and region                     |
| `pipelines`       | `factory`, `limit`    | every pipeline in one factory, including those `pipeline_pattern` excludes, with its folder and activity count                                                     |
| `activities`      | `factory`, `pipeline` | the top-level activities ingestion emits as DataJobs, with type, DataHub subtype, dependencies and input/output dataset names                                      |
| `linked_services` | `factory`, `limit`    | each linked service's type, the DataHub platform it maps to (null means no lineage resolves through it), its `platform_instance_map` entry and integration runtime |
| `datasets`        | `factory`, `limit`    | each ADF dataset with the DataHub URN ingestion resolves it to for lineage, or `unresolved_reason` when it resolves to none                                        |

`probe filter --kind "Data Factory"`, `--kind Pipeline` and `--kind Activity` judge `factory_pattern` and `pipeline_pattern` against bare names, as ingestion does. `pipeline_pattern` is not qualified by the factory, so a same-named pipeline in two factories gets the same verdict. Activities have no pattern and follow their pipeline. A factory whose pipelines `pipelines` lists can still be skipped by ingestion: a run drops the whole factory when it cannot list the factory's triggers or, with `include_lineage` on, its datasets, linked services or data flows. Activities nested inside ForEach, IfCondition, Until or Switch are counted as `nested_activities` on their container and reported in `warnings`, because ingestion does not emit them.

To find the datasets that will produce no lineage before a run, look for a null `urn`:

```shell
datahub recipe probe run datasets --recipe recipe.yml --factory my-factory
datahub recipe probe filter --recipe recipe.yml --kind Pipeline --parent my-factory --name sales_pipeline
```

The probe returns names, types, references and resolved platforms only. Linked-service, dataset, activity and factory definitions are never returned, because that is where ADF keeps connection strings, keys, request headers, SQL text and parameter values. There is no raw API passthrough. The probe needs the same read actions as the reader role in `datahub-adf-reader-role.json`.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
