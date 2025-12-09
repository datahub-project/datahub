## Overview

This connector extracts metadata from Azure Data Factory (ADF), including:

- **Data Factories** as Containers
- **Pipelines** as DataFlows
- **Activities** as DataJobs (Copy, Data Flow, Lookup, etc.)
- **Lineage** between source and destination datasets
- **Execution History** as DataProcessInstance (optional)

:::note Not Azure Fabric
This connector is for **Azure Data Factory** (classic), not Azure Fabric's Data Factory. Azure Fabric support is planned for a future release.
:::

## Prerequisites

### Azure Authentication

The connector supports multiple authentication methods:

| Method                     | Best For                                         | Configuration                                       |
| -------------------------- | ------------------------------------------------ | --------------------------------------------------- |
| **Service Principal**      | Production environments                          | `authentication_method: service_principal`          |
| **Managed Identity**       | Azure-hosted deployments (VMs, AKS, App Service) | `authentication_method: managed_identity`           |
| **Azure CLI**              | Local development                                | `authentication_method: cli` (run `az login` first) |
| **DefaultAzureCredential** | Flexible environments                            | `authentication_method: default`                    |

### Required Azure Permissions

Grant the following role to your identity on the Data Factory resources:

| Role                         | Required For                        |
| ---------------------------- | ----------------------------------- |
| **Reader**                   | Basic metadata extraction           |
| **Data Factory Contributor** | Full access including pipeline runs |

To set up a service principal:

1. Create an App Registration in Azure Portal > Microsoft Entra ID > App registrations
2. Create a client secret under Certificates & secrets
3. Grant the service principal **Reader** or **Data Factory Contributor** role on your resource group or Data Factory

## Concept Mapping

| Azure Data Factory | DataHub Entity                                                                                         | SubType                      |
| ------------------ | ------------------------------------------------------------------------------------------------------ | ---------------------------- |
| Data Factory       | [Container](https://docs.datahub.com/docs/generated/metamodel/entities/container/)                     | Data Factory                 |
| Pipeline           | [DataFlow](https://docs.datahub.com/docs/generated/metamodel/entities/dataflow/)                       | Pipeline                     |
| Activity           | [DataJob](https://docs.datahub.com/docs/generated/metamodel/entities/datajob/)                         | Copy, DataFlow, Lookup, etc. |
| Dataset            | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)                         | Based on linked service type |
| Pipeline Run       | [DataProcessInstance](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) | -                            |

## Capabilities

| Capability            | Status | Notes                                       |
| --------------------- | ------ | ------------------------------------------- |
| Platform Instance     | ✅     | Enabled by default                          |
| Containers            | ✅     | Data Factories as containers                |
| Lineage (Table-level) | ✅     | From activity inputs/outputs and Data Flows |
| Data Flow Scripts     | ✅     | Stored as transformation logic              |
| Execution History     | ✅     | Optional, via `include_execution_history`   |
| Stateful Ingestion    | ✅     | Stale entity removal                        |

## Lineage Extraction

The connector extracts lineage from:

1. **Copy Activities**: Maps input/output datasets to DataHub datasets
2. **Data Flow Activities**: Extracts sources and sinks from Data Flow definitions
3. **Lookup Activities**: Maps lookup datasets as inputs

### Supported Linked Service Mappings

| ADF Linked Service                         | DataHub Platform     |
| ------------------------------------------ | -------------------- |
| AzureBlobStorage, AzureBlobFS              | `azure_blob_storage` |
| AzureDataLakeStore, AzureDataLakeStoreGen2 | `azure_data_lake`    |
| AzureSqlDatabase, AzureSqlDW               | `mssql`              |
| AzureSynapseAnalytics                      | `synapse`            |
| Snowflake                                  | `snowflake`          |
| AmazonS3                                   | `s3`                 |
| GoogleBigQuery                             | `bigquery`           |
| PostgreSql, AzurePostgreSql                | `postgres`           |
| MySql, AzureMySql                          | `mysql`              |
| Oracle                                     | `oracle`             |
| Salesforce                                 | `salesforce`         |
| CosmosDb                                   | `cosmos`             |
| AzureDatabricks, DatabricksDeltaLake       | `databricks`         |

### Platform Instance Mapping

For accurate lineage resolution to existing datasets in DataHub, map linked service names to platform instances:

```yaml
source:
  type: azure-data-factory
  config:
    platform_instance_map:
      "snowflake-prod-connection": "prod_snowflake"
      "synapse-analytics-connection": "prod_synapse"
```

## Data Flow Scripts

For activities that execute ADF Data Flows (mapping data flows), the connector extracts the Data Flow script and stores it as transformation logic on the DataJob entity.

This enables:

- Viewing the complete Data Flow transformation script in DataHub
- Understanding the data transformations applied by each Data Flow activity
- Searching for Data Flows by their transformation logic

The script is stored in the `dataTransformLogic` aspect and is visible in the DataHub UI under the activity's details.

## Execution History

When `include_execution_history: true`, the connector extracts pipeline runs as `DataProcessInstance` entities:

```yaml
source:
  type: azure-data-factory
  config:
    include_execution_history: true
    execution_history_days: 7 # 1-90 days
```

This provides:

- Pipeline run status (Succeeded, Failed, Cancelled, In Progress)
- Run duration and timestamps
- Trigger information (who/what started the run)
- Run parameters

## When to Use Platform Instance

The `platform_instance` configuration is used to distinguish between **separate ADF deployments** (e.g., different Azure subscriptions or tenants), not for separating factories within the same deployment.

### When to Use `platform_instance`

| Scenario                         | Example Configuration                    |
| -------------------------------- | ---------------------------------------- |
| **Multiple Azure Subscriptions** | Different subscriptions for prod vs dev  |
| **Multi-Tenant Organizations**   | Separate Azure tenants per business unit |
| **Multi-Region Deployments**     | US-East vs EU-West deployments           |

**Example: Multiple Subscriptions**

```yaml
# Production subscription
source:
  type: azure-data-factory
  config:
    subscription_id: "prod-subscription-id"
    platform_instance: "production"

# Development subscription
source:
  type: azure-data-factory
  config:
    subscription_id: "dev-subscription-id"
    platform_instance: "development"
```

**Example: Multi-Region**

```yaml
# US Region
source:
  type: azure-data-factory
  config:
    subscription_id: "us-east-subscription"
    platform_instance: "us-east"

# EU Region
source:
  type: azure-data-factory
  config:
    subscription_id: "eu-west-subscription"
    platform_instance: "eu-west"
```

### When NOT to Use `platform_instance`

- **Single subscription** - Factory names in URNs already provide uniqueness
- **Multiple factories in same subscription** - The factory name is included in the URN automatically
- **Same logical environment** - Don't use it just to differentiate factories

:::note URN Uniqueness
The connector automatically includes the factory name in pipeline URNs (e.g., `my-factory.ETL-Pipeline`), so you don't need `platform_instance` to distinguish pipelines across factories within the same subscription.
:::

## URN Format

Pipeline URNs include the factory name for uniqueness across multiple factories:

```
urn:li:dataFlow:(azure_data_factory,{factory_name}.{pipeline_name},{env})
```

Example: `urn:li:dataFlow:(azure_data_factory,my-factory.ETL-Pipeline,PROD)`

Activity URNs reference their parent pipeline:

```
urn:li:dataJob:(urn:li:dataFlow:(azure_data_factory,{factory_name}.{pipeline_name},{env}),{activity_name})
```

With `platform_instance` set, it's prepended to the URN:

```
urn:li:dataFlow:(azure_data_factory,{platform_instance}.{factory_name}.{pipeline_name},{env})
```

Example: `urn:li:dataFlow:(azure_data_factory,production.my-factory.ETL-Pipeline,PROD)`
