:::note Not Azure Fabric
This connector is for **Azure Data Factory** (classic), not Azure Fabric's Data Factory. Azure Fabric support is planned for a future release.
:::

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

## Lineage Extraction

### Which Activities Produce Lineage?

The connector extracts both **table-level** and **column-level** lineage from Azure Data Factory activities. Table-level lineage shows which datasets connect to which datasets, while column-level lineage (available for Copy Activities) shows how individual columns map between source and destination.

| Activity Type       | Lineage Behavior                                           | Table-Level Lineage | Column-Level Lineage             |
| ------------------- | ---------------------------------------------------------- | ------------------- | -------------------------------- |
| **Copy Activity**   | Creates lineage from input dataset(s) to output dataset    | ✅ Yes              | ✅ Yes (when mappings available) |
| **Data Flow**       | Extracts sources, sinks, and transformation script         | ✅ Yes              | ❌ No (table-level only)         |
| **Lookup Activity** | Creates input lineage from the lookup dataset              | ✅ Yes              | ❌ No (table-level only)         |
| **ExecutePipeline** | Creates pipeline-to-pipeline lineage to the child pipeline | ✅ Yes              | ❌ No (table-level only)         |

### Column-Level Lineage

The connector extracts **column-level lineage** from Copy Activity column mappings, enabling fine-grained visibility into how individual columns flow through your data pipelines.

#### Supported Copy Activity Mappings

Column lineage is extracted from three mapping formats:

**1. Dictionary Format (Legacy)**

```json
{
  "translator": {
    "type": "TabularTranslator",
    "columnMappings": {
      "source_column": "destination_column",
      "customer_id": "id",
      "customer_name": "name"
    }
  }
}
```

**2. List Format (Current, Recommended)**

```json
{
  "translator": {
    "type": "TabularTranslator",
    "mappings": [
      {
        "source": { "name": "order_id", "type": "Int32" },
        "sink": { "name": "id", "type": "Int32" }
      },
      {
        "source": { "name": "order_date", "type": "DateTime" },
        "sink": { "name": "date", "type": "DateTime" }
      }
    ]
  }
}
```

**3. Auto-Mapping Inference**

When a Copy Activity uses `TabularTranslator` without explicit column mappings, the connector infers 1:1 column mappings from the source dataset schema. This works when:

- The source dataset has schema information available
- The translator type is `TabularTranslator`
- No explicit `columnMappings` or `mappings` are defined

Auto-mapping assumes columns map by name (e.g., `customer_id` → `customer_id`).

#### Limitations

- **Copy Activity Only**: Column lineage is currently only extracted from Copy Activities. Other activity types (Data Flow, Lookup, etc.) produce table-level lineage only.
- **Schema Availability**: Auto-mapping inference requires source dataset schema information. If schema is unavailable, only explicit mappings are extracted.
- **Single Source/Sink**: The connector uses the first input and first output dataset for column lineage. Activities with multiple inputs/outputs may not capture all column mappings.

#### Troubleshooting

**No column lineage appearing?**

- Verify `include_lineage: true` and `include_column_lineage: true` in your recipe
- Check that your Copy Activity has a `translator` property with `type: "TabularTranslator"`
- For auto-mapping, ensure the source dataset has schema information
- Review ingestion logs for column lineage extraction messages

**Missing some column mappings?**

- Verify column mappings are defined in the Copy Activity's translator
- Check that source and sink dataset URNs are correctly resolved (see [Platform Instance Mapping](#step-2-platform-instance-mapping-for-cross-recipe-lineage))
- Review the activity's `typeProperties` or flattened `translator` field in Azure SDK responses

### How Lineage Resolution Works

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

### Data Flow Transformation Scripts

For Data Flow activities, the connector extracts the transformation script and stores it in the `dataTransformLogic` aspect, visible in the DataHub UI under activity details.

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

### When to Use `platform_instance`

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

### URN Format

Pipeline URNs follow this format:

```
urn:li:dataFlow:(azure-data-factory,{factory_name}.{pipeline_name},{env})
```

With `platform_instance`:

```
urn:li:dataFlow:(azure-data-factory,{platform_instance}.{factory_name}.{pipeline_name},{env})
```

For Azure naming rules, see [Azure Data Factory naming rules](https://learn.microsoft.com/en-us/azure/data-factory/naming-rules).
