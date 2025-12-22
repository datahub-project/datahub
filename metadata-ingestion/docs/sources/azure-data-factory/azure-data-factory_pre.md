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

The connector only performs **read operations** and does not modify any Azure resources.

#### Option 1: Built-in Reader Role

**Minimum Required Role:** [**Reader**](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#reader)

Grant this role at one of the following scopes:

- **Subscription level** - Access all Data Factories in the subscription
- **Resource Group level** - Access Data Factories in specific resource group(s)
- **Data Factory level** - Access specific Data Factory instance(s)

#### Option 2: Custom Role with Fine-Grained Permissions

Create a custom role with only the specific Data Factory read operations needed.

**Download the role definition:** [`datahub-adf-reader-role.json`](./datahub-adf-reader-role.json)

<details>
<summary>View role definition</summary>

```json
{
  "Name": "DataHub ADF Reader",
  "Description": "Custom role for DataHub Azure Data Factory connector with minimal read permissions",
  "IsCustom": true,
  "Actions": [
    "Microsoft.DataFactory/factories/read",
    "Microsoft.DataFactory/factories/pipelines/read",
    "Microsoft.DataFactory/factories/datasets/read",
    "Microsoft.DataFactory/factories/linkedservices/read",
    "Microsoft.DataFactory/factories/dataflows/read",
    "Microsoft.DataFactory/factories/triggers/read",
    "Microsoft.DataFactory/factories/pipelineruns/read",
    "Microsoft.DataFactory/factories/pipelineruns/activityruns/read"
  ],
  "NotActions": [],
  "DataActions": [],
  "NotDataActions": [],
  "AssignableScopes": ["/subscriptions/{subscription-id}"]
}
```

</details>

**Deployment Instructions:**

**Using Azure CLI:**

```bash
# 1. Download the role definition file and update {subscription-id}
# 2. Create the custom role
az role definition create --role-definition datahub-adf-reader-role.json

# 3. Assign to service principal at subscription level
az role assignment create \
  --assignee <service-principal-object-id> \
  --role "DataHub ADF Reader" \
  --scope /subscriptions/{subscription-id}

# Or assign at resource group level for more restrictive scope
az role assignment create \
  --assignee <service-principal-object-id> \
  --role "DataHub ADF Reader" \
  --scope /subscriptions/{subscription-id}/resourceGroups/{resource-group-name}
```

**Using Azure Portal:**

1. Navigate to your **Subscription** or **Resource Group** in Azure Portal
2. Click **Access control (IAM)** → **Add** → **Add custom role**
3. Click **Start from JSON** → **Select a file** → Upload `datahub-adf-reader-role.json`
4. Update the subscription ID in the **Assignable scopes** tab
5. Click **Review + create** → **Create**
6. Once created, go back to **Access control (IAM)** → **Add role assignment**
7. Select **DataHub ADF Reader** role → Next
8. Click **Select members** → Search for your service principal → Select
9. Click **Review + assign**

:::note Custom Role Permissions
The custom role includes only the exact permissions needed:

- **factories/read** - List and get Data Factory instances
- **pipelines/read** - List and get pipeline definitions
- **datasets/read** - List and get dataset definitions (for lineage)
- **linkedservices/read** - List and get linked service definitions (for platform mapping)
- **dataflows/read** - List and get data flow definitions (for lineage and scripts)
- **triggers/read** - List and get trigger definitions (for pipeline metadata)
- **pipelineruns/read** - Query pipeline execution history
- **pipelineruns/activityruns/read** - Query activity execution details within pipeline runs
  :::

**To set up a service principal:**

1. Create an App Registration in Azure Portal → Microsoft Entra ID → App registrations
2. Create a client secret under Certificates & secrets
3. Grant the service principal either the **Reader** role (Option 1) or the **DataHub ADF Reader** custom role (Option 2):
   - Navigate to the resource in Azure Portal
   - Click **Access control (IAM)** → **Add role assignment**
   - Select the appropriate role
   - Assign to your service principal

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

## Lineage Extraction

The connector extracts lineage from:

1. **Copy Activities**: Maps input/output datasets to DataHub datasets
2. **Data Flow Activities**: Extracts sources and sinks from Data Flow definitions
3. **Lookup Activities**: Maps lookup datasets as inputs
4. **ExecutePipeline Activities**: Creates pipeline-to-pipeline lineage to child pipelines

:::tip Column-Level Lineage Coming Soon
Column-level lineage support is planned for Copy Activity `translator.columnMappings`. This will extract field-to-field mappings from Copy activities, enabling fine-grained impact analysis at the column level.
:::

### Pipeline-to-Pipeline Lineage

When a pipeline calls another pipeline via an `ExecutePipeline` activity, the connector creates a lineage edge showing the calling activity as **upstream** of the child pipeline's first activity. This enables:

- Tracing orchestration hierarchies across nested pipelines
- Impact analysis when modifying child pipelines
- Understanding dependencies between modular pipelines

**Lineage Direction:** `ExecutePipeline` → `ChildFirstActivity`

The ExecutePipeline activity's DataJob entity will include:

- Custom property `calls_pipeline`: Name of the child pipeline
- Custom property `child_pipeline_urn`: URN of the child DataFlow
- Custom property `child_first_activity`: Name of the first activity in the child pipeline

The child pipeline's first activity will have the ExecutePipeline as its input/upstream dependency.

### Supported Linked Service Mappings

The connector maps ADF linked services to DataHub platforms for lineage resolution:

| Category          | ADF Linked Service                                                         | DataHub Platform |
| ----------------- | -------------------------------------------------------------------------- | ---------------- |
| **Azure Storage** | AzureBlobStorage, AzureBlobFS, AzureDataLakeStore, AzureFileStorage        | `abs`            |
| **Azure SQL**     | AzureSqlDatabase, AzureSqlDW, AzureSynapseAnalytics, AzureSqlMI, SqlServer | `mssql`          |
| **Databricks**    | AzureDatabricks, AzureDatabricksDeltaLake                                  | `databricks`     |
| **Cloud - AWS**   | AmazonS3, AmazonS3Compatible                                               | `s3`             |
|                   | AmazonRedshift                                                             | `redshift`       |
| **Cloud - GCP**   | GoogleCloudStorage                                                         | `gcs`            |
|                   | GoogleBigQuery                                                             | `bigquery`       |
| **Cloud - Other** | Snowflake                                                                  | `snowflake`      |
| **Databases**     | PostgreSql, AzurePostgreSql                                                | `postgres`       |
|                   | MySql, AzureMySql                                                          | `mysql`          |
|                   | Oracle, OracleServiceCloud                                                 | `oracle`         |
|                   | Db2                                                                        | `db2`            |
|                   | Teradata                                                                   | `teradata`       |
|                   | Vertica                                                                    | `vertica`        |
| **Big Data**      | Hive                                                                       | `hive`           |
|                   | Spark                                                                      | `spark`          |
|                   | Hdfs                                                                       | `hdfs`           |
| **SaaS**          | Salesforce, SalesforceServiceCloud, SalesforceMarketingCloud               | `salesforce`     |

For linked services not in this list (e.g., CosmosDb, CosmosDbMongoDbApi, ServiceNow, Dynamics), the connector logs a warning and skips lineage for that dataset. You can check the `unmapped_platforms` counter in the ingestion report to identify any unmapped services.

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

By default, the connector extracts pipeline runs as `DataProcessInstance` entities. This is controlled by `include_execution_history` (default: `true`).

```yaml
source:
  type: azure-data-factory
  config:
    # Execution history is enabled by default
    include_execution_history: true # Set to false to disable
    execution_history_days: 7 # 1-90 days (default: 7)
```

This provides:

- Pipeline run status (Succeeded, Failed, Cancelled, In Progress)
- Run duration and timestamps
- Trigger information (who/what started the run)
- Run parameters
- Activity-level run details

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
urn:li:dataFlow:(azure-data-factory,{factory_name}.{pipeline_name},{env})
```

Example: `urn:li:dataFlow:(azure-data-factory,my-factory.ETL-Pipeline,PROD)`

Activity URNs reference their parent pipeline:

```
urn:li:dataJob:(urn:li:dataFlow:(azure-data-factory,{factory_name}.{pipeline_name},{env}),{activity_name})
```

With `platform_instance` set, it's prepended to the URN:

```
urn:li:dataFlow:(azure-data-factory,{platform_instance}.{factory_name}.{pipeline_name},{env})
```

Example: `urn:li:dataFlow:(azure-data-factory,production.my-factory.ETL-Pipeline,PROD)`

## Naming Rules and Uniqueness

### Azure Naming Rules

Azure Data Factory enforces specific naming rules documented at [Azure Data Factory naming rules](https://learn.microsoft.com/en-us/azure/data-factory/naming-rules):

| Resource        | Uniqueness                   | Case Sensitivity |
| --------------- | ---------------------------- | ---------------- |
| Data Factory    | Globally unique across Azure | Case-insensitive |
| Pipelines       | Unique within a factory      | Case-insensitive |
| Datasets        | Unique within a factory      | Case-insensitive |
| Linked Services | Unique within a factory      | Case-insensitive |
| Data Flows      | Unique within a factory      | Case-insensitive |

### How DataHub Handles Uniqueness

The connector constructs URNs using `{factory_name}.{pipeline_name}` format:

- **Factory names are globally unique** in Azure, preventing collisions within a subscription
- **Pipeline names are unique within a factory**, so the combination is globally unique
- **No additional namespacing needed** for single-subscription deployments

### Multi-Subscription and Multi-Tenant Scenarios

:::warning Important
Factory names are globally unique _within Azure_, but different Azure tenants or subscriptions in different regions could have identically-named factories.
:::

| Scenario                             | Risk                                  | Solution                                           |
| ------------------------------------ | ------------------------------------- | -------------------------------------------------- |
| Single subscription                  | None                                  | Default URN format works                           |
| Multiple subscriptions (same tenant) | Low - factory names still unique      | Default works, but `platform_instance` recommended |
| Multiple tenants                     | **High** - same factory name possible | **Must use `platform_instance`**                   |

**Example: Multi-Tenant Setup**

```yaml
# Tenant A
source:
  type: azure-data-factory
  config:
    subscription_id: "tenant-a-sub"
    platform_instance: "tenant-a"

# Tenant B (could have same factory name!)
source:
  type: azure-data-factory
  config:
    subscription_id: "tenant-b-sub"
    platform_instance: "tenant-b"
```

### Case Sensitivity

Azure treats names as **case-insensitive** (e.g., `MyFactory` and `myfactory` are the same factory). DataHub URNs are case-sensitive, but this doesn't cause issues because:

1. Azure prevents creating duplicate names with different casing at the source
2. The connector uses exact names from the Azure API response
3. Consistent casing is maintained throughout ingestion

:::tip
If you're ingesting from multiple Azure tenants and see unexpected entity overwrites in DataHub, ensure each ingestion recipe uses a unique `platform_instance` value.
:::
