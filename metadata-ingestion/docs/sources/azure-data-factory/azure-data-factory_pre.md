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

## Concept Mapping

| Azure Data Factory | DataHub Entity                                                                                         | SubType                      |
| ------------------ | ------------------------------------------------------------------------------------------------------ | ---------------------------- |
| Data Factory       | [Container](https://docs.datahub.com/docs/generated/metamodel/entities/container/)                     | Data Factory                 |
| Pipeline           | [DataFlow](https://docs.datahub.com/docs/generated/metamodel/entities/dataflow/)                       | Pipeline                     |
| Activity           | [DataJob](https://docs.datahub.com/docs/generated/metamodel/entities/datajob/)                         | Copy, DataFlow, Lookup, etc. |
| Dataset            | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)                         | Based on linked service type |
| Pipeline Run       | [DataProcessInstance](https://docs.datahub.com/docs/generated/metamodel/entities/dataprocessinstance/) | -                            |

## Capabilities

| Capability             | Status | Notes                                              |
| ---------------------- | ------ | -------------------------------------------------- |
| Platform Instance      | ✅     | Enabled by default                                 |
| Containers             | ✅     | Data Factories as containers                       |
| Lineage (Table-level)  | ✅     | From activity inputs/outputs and Data Flows        |
| Lineage (Column-level) | 🔜     | Coming soon for Copy Activity mappings             |
| Pipeline-to-Pipeline   | ✅     | ExecutePipeline activities create lineage          |
| Data Flow Scripts      | ✅     | Stored as transformation logic                     |
| Execution History      | ✅     | Enabled by default via `include_execution_history` |
| Stateful Ingestion     | ✅     | Stale entity removal                               |

## Prerequisites

### Authentication

The connector supports multiple authentication methods:

| Method                     | Best For                                         | Configuration                                       |
| -------------------------- | ------------------------------------------------ | --------------------------------------------------- |
| **Service Principal**      | Production environments                          | `authentication_method: service_principal`          |
| **Managed Identity**       | Azure-hosted deployments (VMs, AKS, App Service) | `authentication_method: managed_identity`           |
| **Azure CLI**              | Local development                                | `authentication_method: cli` (run `az login` first) |
| **DefaultAzureCredential** | Flexible environments                            | `authentication_method: default`                    |

For service principal setup, see [Register an application with Microsoft Entra ID](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app).

### Required Permissions

The connector only performs **read operations**. Grant one of the following:

**Option 1: Built-in Reader Role** (recommended)

Assign the [Reader](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#reader) role at subscription, resource group, or Data Factory level.

**Option 2: Custom Role with Minimal Permissions**

Download [`datahub-adf-reader-role.json`](./datahub-adf-reader-role.json), update the `{subscription-id}`, then:

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

## Lineage

The connector extracts lineage from:

1. **Copy Activities**: Maps input/output datasets to DataHub datasets
2. **Data Flow Activities**: Extracts sources and sinks, plus transformation scripts
3. **Lookup Activities**: Maps lookup datasets as inputs
4. **ExecutePipeline Activities**: Creates pipeline-to-pipeline lineage

:::tip Column-Level Lineage Coming Soon
Column-level lineage support is planned for Copy Activity `translator.columnMappings`.
:::

### Data Flow Scripts

For Data Flow activities, the connector extracts the transformation script and stores it in the `dataTransformLogic` aspect, visible in the DataHub UI under activity details.

### Pipeline-to-Pipeline Lineage

When a pipeline calls another via `ExecutePipeline`, the connector creates lineage from the calling activity to the child pipeline's first activity. The ExecutePipeline DataJob includes custom properties: `calls_pipeline`, `child_pipeline_urn`, and `child_first_activity`.

### Supported Linked Service Mappings

| Category          | ADF Linked Service          | DataHub Platform |
| ----------------- | --------------------------- | ---------------- |
| **Azure Storage** | AzureBlobStorage            | `abs`            |
|                   | AzureBlobFS                 | `abs`            |
|                   | AzureDataLakeStore          | `abs`            |
|                   | AzureFileStorage            | `abs`            |
| **Azure SQL**     | AzureSqlDatabase            | `mssql`          |
|                   | AzureSqlDW                  | `mssql`          |
|                   | AzureSynapseAnalytics       | `mssql`          |
|                   | AzureSqlMI                  | `mssql`          |
|                   | SqlServer                   | `mssql`          |
| **Databricks**    | AzureDatabricks             | `databricks`     |
|                   | AzureDatabricksDeltaLake    | `databricks`     |
| **Cloud - AWS**   | AmazonS3                    | `s3`             |
|                   | AmazonS3Compatible          | `s3`             |
|                   | AmazonRedshift              | `redshift`       |
| **Cloud - GCP**   | GoogleCloudStorage          | `gcs`            |
|                   | GoogleBigQuery              | `bigquery`       |
| **Cloud - Other** | Snowflake                   | `snowflake`      |
| **Databases**     | PostgreSql, AzurePostgreSql | `postgres`       |
|                   | MySql, AzureMySql           | `mysql`          |
|                   | Oracle, OracleServiceCloud  | `oracle`         |
|                   | Db2                         | `db2`            |
|                   | Teradata                    | `teradata`       |
|                   | Vertica                     | `vertica`        |
| **Big Data**      | Hive                        | `hive`           |
|                   | Spark                       | `spark`          |
|                   | Hdfs                        | `hdfs`           |
| **SaaS**          | Salesforce                  | `salesforce`     |
|                   | SalesforceServiceCloud      | `salesforce`     |
|                   | SalesforceMarketingCloud    | `salesforce`     |

Unsupported linked services log a warning and skip lineage for that dataset.

### Platform Instance Mapping

Map linked service names to DataHub platform instances for accurate lineage resolution:

```yaml
source:
  type: azure-data-factory
  config:
    platform_instance_map:
      "snowflake-prod-connection": "prod_snowflake"
      "synapse-analytics-connection": "prod_synapse"
```

## Execution History

Pipeline runs are extracted as `DataProcessInstance` entities by default:

```yaml
source:
  type: azure-data-factory
  config:
    include_execution_history: true # default
    execution_history_days: 7 # 1-90 days
```

This provides run status, duration, timestamps, trigger info, parameters, and activity-level details.

## Advanced: Multi-Environment Setup

### How DataHub Handles Uniqueness

The connector constructs URNs using `{factory_name}.{pipeline_name}` format:

- **Factory names are globally unique** in Azure, preventing collisions within a subscription
- **Pipeline names are unique within a factory**, so the combination is globally unique
- **No additional namespacing needed** for single-subscription deployments

### When to Use `platform_instance`

Use `platform_instance` to distinguish **separate ADF deployments** (different subscriptions or tenants):

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

### URN Format

Pipeline URNs include the factory name:

```
urn:li:dataFlow:(azure-data-factory,{factory_name}.{pipeline_name},{env})
```

With `platform_instance`:

```
urn:li:dataFlow:(azure-data-factory,{platform_instance}.{factory_name}.{pipeline_name},{env})
```

For Azure naming rules, see [Azure Data Factory naming rules](https://learn.microsoft.com/en-us/azure/data-factory/naming-rules).
