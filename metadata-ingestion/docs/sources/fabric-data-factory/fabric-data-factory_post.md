### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Authentication Methods

The connector supports four authentication methods. See **Prerequisites > Authentication** above for detailed configuration and examples.

| Method                     | Best For                                         | Configuration                                       |
| -------------------------- | ------------------------------------------------ | --------------------------------------------------- |
| **Service Principal**      | Production environments                          | `authentication_method: service_principal`          |
| **Managed Identity**       | Azure-hosted deployments (VMs, AKS, App Service) | `authentication_method: managed_identity`           |
| **Azure CLI**              | Local development                                | `authentication_method: cli` (run `az login` first) |
| **DefaultAzureCredential** | Flexible auto-detection                          | `authentication_method: default`                    |

Additional options:

- **User-assigned managed identity**: Set `managed_identity_client_id` when using `managed_identity` method
- **DefaultAzureCredential tuning**: Use `exclude_cli_credential`, `exclude_environment_credential`, or `exclude_managed_identity_credential` to skip specific credential sources

#### Lineage Extraction

##### Which Activities Produce Lineage?

The connector extracts **dataset-level lineage** from these Fabric activity types:

| Activity Type      | Lineage Behavior                                           |
| ------------------ | ---------------------------------------------------------- |
| **Copy**           | Creates lineage from input dataset(s) to output dataset    |
| **InvokePipeline** | Creates pipeline-to-pipeline lineage to the child pipeline |

Lineage is enabled by default (`include_lineage: true`).

##### How Lineage Resolution Works

For lineage to connect properly to datasets ingested from other sources (e.g., Snowflake, BigQuery), the connector resolves Fabric connections to DataHub platforms.

**Step 1: Automatic Connection Mapping**

The connector automatically maps Fabric connection types to DataHub platforms (e.g., a `Snowflake` connection maps to the `snowflake` platform). See [`FABRIC_CONNECTION_PLATFORM_MAP`](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/fabric/common/constants.py) for the full list of supported mappings. Unsupported connection types fall back to using the connection type string as the platform name.

**Step 2: Platform Instance Mapping (for cross-recipe lineage)**

If you're ingesting the same data sources with other DataHub connectors (e.g., Snowflake, BigQuery), you need to ensure the `platform_instance` values match. Use `platform_instance_map` to map your Fabric connection names to the platform instance used in your other recipes:

```yaml
# Fabric Data Factory Recipe
source:
  type: fabric-data-factory
  config:
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}
    platform_instance_map:
      # Key: Your Fabric connection name (exact match required)
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

#### Execution History

Pipeline and activity runs are extracted as `DataProcessInstance` entities by default:

```yaml
source:
  type: fabric-data-factory
  config:
    include_execution_history: true # default
    execution_history_days: 7 # 1-90 days
```

This provides run status, duration, timestamps, invoke type, and activity-level details including error messages and retry attempts.

:::note
The Fabric API returns at most 100 recently completed runs per pipeline. Run ingestion more frequently to capture deeper history.
:::

#### Advanced: Multi-Tenant Setup

##### When to Use `platform_instance`

Use the connector's `platform_instance` config to distinguish **separate Fabric tenants** when ingesting from multiple environments:

| Scenario         | Risk                           | Solution     |
| ---------------- | ------------------------------ | ------------ |
| Single tenant    | None                           | Not needed   |
| Multiple tenants | **High** - name collision risk | **Required** |

```yaml
# Multi-tenant example
source:
  type: fabric-data-factory
  config:
    platform_instance: "contoso-tenant" # Prevents URN collisions
```

:::warning
Different Fabric tenants could have identically-named workspaces and pipelines. Use `platform_instance` to prevent entity overwrites.
:::

##### URN Format

Pipeline URNs follow this format:

```
urn:li:dataFlow:(fabric-data-factory,{workspace_id}.{pipeline_id},{env})
```

With `platform_instance`:

```
urn:li:dataFlow:(fabric-data-factory,{platform_instance}.{workspace_id}.{pipeline_id},{env})
```

### Limitations

- **Run history limit**: The Fabric API returns at most 100 recently completed runs per pipeline. If `execution_history_days` covers more runs than this limit, only the most recent 100 are returned. Run ingestion more frequently to capture deeper history.
- **No Dataflow Gen2 support**: Dataflow Gen2 items (standalone workspace-level items with transformation logic) are not extracted.
- **No CopyJob support**: Standalone CopyJob items at the workspace level are not extracted. Only Copy activities embedded within pipelines produce lineage.
- **No trigger/schedule metadata**: Pipeline triggers and schedules are not extracted.
- **ExecutePipeline not supported**: The `ExecutePipeline` activity type is marked as legacy in Fabric and is not supported for cross-pipeline lineage.

#### Lineage

- **Lineage scope**: Only Copy and InvokePipeline activities produce dataset or pipeline lineage. Other activity types (Lookup, Wait, ForEach, Script, etc.) are ingested as DataJobs without dataset-level lineage.
- **InvokePipeline Activity operation types**: Only the `InvokeFabricPipeline` operation type is supported for cross-pipeline lineage. Other operation types (`InvokeAdfPipeline`, `InvokeExternalPipeline`) are not resolved and will be skipped.
- **Query-based Copy sources**: When a Copy activity uses `sqlReaderQuery` or `sqlReaderStoredProcedureName` instead of a direct table reference, lineage is **not extracted**.
- **No column-level lineage**: The connector extracts dataset-level lineage only. Column-to-column mappings from Copy activity translator configurations are not extracted.
- **No Notebook/SparkJobDefinition lineage**: Notebook and SparkJobDefinition activities are ingested as DataJobs but their lineage is not resolved.
- **Connection resolution**: Unmapped connection types fall back to using the connection type string as the platform name, which may not match your existing DataHub platform names. Use `platform_instance_map` to explicitly map connection names.

### Troubleshooting

- **401/403 errors**: Ensure the service principal has the correct Fabric API permissions and is added as a workspace member.
- **Empty results**: Check that `workspace_pattern` and `pipeline_pattern` are not filtering out all items.
- **Missing lineage**: Verify that `include_lineage: true` is set and that Fabric connections are properly configured for the pipelines. Also review the [Lineage limitations](#lineage) section for unsupported activity types and scenarios.
- **Stale entities**: Enable `stateful_ingestion` to automatically remove entities that no longer exist in Fabric.
