


# Azure Data Factory

## Overview

Azure Data Factory is a streaming or integration platform. Learn more in the [official Azure Data Factory documentation](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app).

The DataHub integration for Azure Data Factory covers streaming/integration entities such as topics, connectors, pipelines, or jobs. It also captures table- and column-level lineage and stateful deletion detection.

## Concept Mapping

| ADF Concept  | DataHub Entity      |
| ------------ | ------------------- |
| Data Factory | Container           |
| Pipeline     | DataFlow            |
| Activity     | DataJob             |
| Dataset      | Dataset             |
| Pipeline Run | DataProcessInstance |


## Module `azure-data-factory`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Data Factory. |
| Column-level Lineage | ✅ | Extracts column-level lineage from Copy activities. Supported for types - Copy Activity. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Extracts lineage from Copy and Data Flow activities. Supported for types - Copy Activity, Data Flow Activity. |

### Overview

The `azure-data-factory` module ingests metadata from Azure Data Factory into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

#### Required Permissions

The connector only performs **read operations**. Grant one of the following:

**Option 1: Built-in Reader Role** (recommended)

Assign the [Reader](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#reader) role at subscription, resource group, or Data Factory level.

**Option 2: Custom Role with Minimal Permissions**

Download [`datahub-adf-reader-role.json`](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/datahub-adf-reader-role.json), update the `{subscription-id}`, then:

```bash
# Create custom role
az role definition create --role-definition datahub-adf-reader-role.json

# Assign to service principal
az role assignment create \
  --assignee <service-principal-id> \
  --role "DataHub ADF Reader" \
  --scope /subscriptions/{subscription-id}
```

For detailed instructions, see [Azure custom roles](https://learn.microsoft.com/en-us/azure/role-based-access-control/custom-roles).

#### Setup

1. Configure authentication for the connector runtime.
2. Grant read permissions on the target Data Factory resources.
3. Provide a subscription scope and optional pattern filters in the ingestion recipe.

This section intentionally complements (and does not duplicate) the generated **Starter Recipe** section.


### Install the Plugin
```shell
pip install 'acryl-datahub[azure-data-factory]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
# Example recipe for Azure Data Factory source
# See README.md for full configuration options

source:
  type: azure-data-factory
  config:
    # Required: Azure subscription containing Data Factories
    subscription_id: ${AZURE_SUBSCRIPTION_ID}
    
    # Optional: Filter to specific resource group
    # resource_group: my-resource-group
    
    # Authentication (using service principal)
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}
    
    # Optional: Filter factories by name pattern
    factory_pattern:
      allow:
        - ".*"  # Allow all factories by default
      deny: []
    
    # Optional: Filter pipelines by name pattern
    pipeline_pattern:
      allow:
        - ".*"  # Allow all pipelines by default
      deny: []
    
    # Feature flags
    include_lineage: true
    include_column_lineage: false  # Advanced: requires Data Flow parsing
    include_execution_history: false  # Set to true for pipeline run history
    execution_history_days: 7  # Only used when include_execution_history is true
    
    # Optional: Map linked services to platform instances for accurate lineage
    # platform_instance_map:
    #   "my-snowflake-connection": "prod_snowflake"
    
    # Optional: Platform instance for this ADF connector
    # platform_instance: "main-adf"
    
    # Environment
    env: PROD
    
    # Optional: Stateful ingestion for stale entity removal
    # stateful_ingestion:
    #   enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"


```

### Config Details
Configuration schema is not auto-generated for this module. Refer to the source code coordinates and module guidance below.

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


**View all supported linked service mappings**

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
| AzureDatabricks          | `databricks`     |
| AzureDatabricksDeltaLake | `databricks`     |
| AmazonS3                 | `s3`             |
| AmazonS3Compatible       | `s3`             |
| AmazonRedshift           | `redshift`       |
| GoogleCloudStorage       | `gcs`            |
| GoogleBigQuery           | `bigquery`       |
| Snowflake                | `snowflake`      |
| PostgreSql               | `postgres`       |
| AzurePostgreSql          | `postgres`       |
| MySql                    | `mysql`          |
| AzureMySql               | `mysql`          |
| Oracle                   | `oracle`         |
| OracleServiceCloud       | `oracle`         |
| Db2                      | `db2`            |
| Teradata                 | `teradata`       |
| Vertica                  | `vertica`        |
| Hive                     | `hive`           |
| Spark                    | `spark`          |
| Hdfs                     | `hdfs`           |
| Salesforce               | `salesforce`     |
| SalesforceServiceCloud   | `salesforce`     |
| SalesforceMarketingCloud | `salesforce`     |

Unsupported linked service types log a warning and skip lineage for that dataset.



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

**Limitations**

- **Copy Activity Only**: Column lineage is currently extracted only from Copy activities. Other activity types (Data Flow, Lookup, etc.) produce table-level lineage only.
- **Schema Availability**: Auto-mapping inference requires source dataset schema information (defined in ADF dataset's `schema` or `structure` property). If schema is unavailable, only explicit mappings are extracted.

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

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.azure_data_factory.adf_source.AzureDataFactorySource`

:::tip Questions?

If you've got any questions on configuring ingestion for Azure Data Factory, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
