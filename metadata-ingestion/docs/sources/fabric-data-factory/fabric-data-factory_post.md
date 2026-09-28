### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

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

##### Column-Level Lineage

For Copy activities whose source and destination both resolve to datasets, the connector also emits column-level lineage (enabled by default, `include_column_lineage: true`). It is derived from the Copy activity's `translator` (the **Mapping** tab in the Fabric UI):

| Mapping configuration                                                                     | Column lineage behavior                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Explicit mappings** (`TabularTranslator.mappings`, or the legacy `columnMappings`)      | One column-to-column edge per mapping. Column names are matched case-insensitively to the dataset schemas in DataHub when available, otherwise emitted as written in the pipeline.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| **Default mapping** (no translator, or a `TabularTranslator` without explicit mappings)   | Fabric maps columns by name at runtime. The connector reproduces this only when **both** source and destination columns are known, and emits an edge for each column name present on both sides (case-insensitive).                                                                                                                                                                                                                                                                                                                                                                                    |
| **Auto-created destination** (default mapping with `tableOption: autoCreate` on the sink) | If the destination schema is unknown (e.g. the table does not exist yet), its columns are taken to equal the source columns, because Fabric creates the table from the source schema. Requires the source schema; counted under `column_lineage_activities_auto_created_sink`. Only the pipeline definition is read: Fabric's own runtime prerequisites are not checked (for example, a Warehouse destination auto-created from a Lakehouse table source needs staging enabled, `enableStaging: true`, for the copy to run). If the destination schema is known, the default by-name matching applies. |
| **Dynamic mapping** (translator set via dynamic content / an expression)                  | Not extracted, because the mapping is only known at runtime.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

For default mappings, source and destination columns come from the dataset schema imported into the activity, or otherwise from the `schemaMetadata` of the dataset already in DataHub. The DataHub lookup requires a DataHub graph connection, which is available automatically when using the `datahub-rest` sink (or configure `datahub_api` in the recipe). Ingest the upstream and downstream platforms (e.g. OneLake, Snowflake) before this connector so their schemas are present. When a schema is unavailable, no column lineage is emitted for that activity (it is never guessed) and the activity is counted in the ingestion report under `column_lineage_skipped_no_schema`. A failed DataHub lookup (for example an authentication error) is reported as a `Column Lineage Schema Lookup Failed` warning and counted under `column_lineage_schema_lookup_failed`.

`include_column_lineage` requires `include_lineage`; setting `include_column_lineage: true` while `include_lineage: false` fails config validation.

Activities skipped for other reasons are also counted in the report: `column_lineage_skipped_unresolvable_mappings` (explicit mappings that are not name-based, such as ordinal-only mappings for header-less delimited text, which are never replaced by by-name matching), `column_lineage_skipped_dynamic_translator` / `column_lineage_skipped_unsupported_translator` (listed in `column_lineage_skipped_translator_details`), `column_lineage_mappings_skipped` (non-name mapping entries dropped from otherwise-mapped activities), and `column_lineage_unmatched_columns` (source columns with no same-named destination column under default mapping).

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
- **Column-level lineage scope**: Only Copy activities produce column-level lineage. Default (by-name) mappings require both source and destination schemas to be known, except for auto-created destinations, which require only the source schema (see [Column-Level Lineage](#column-level-lineage)); dynamic (expression-based) and ordinal (position-based) mappings are not extracted. Hierarchical (JSON) `path` mappings are emitted as dotted column paths and may not match nested field paths ingested by other connectors.
- **All-zero workspace IDs**: Fabric saves pipeline definitions whose OneLake items use the all-zero workspace ID (`00000000-0000-0000-0000-000000000000`), but such activities fail at runtime until a real workspace is set. The connector resolves that placeholder, like a missing workspace ID, to the pipeline's own workspace, so lineage reflects the definition even if the activity has never run successfully.
- **No Notebook/SparkJobDefinition lineage**: Notebook and SparkJobDefinition activities are ingested as DataJobs but their lineage is not resolved: the pipeline definition names the notebook, not the tables it reads or writes. Notebook lineage (including column-level) comes from the Spark runtime instead: see [Microsoft Fabric (OneLake) in the OpenLineage docs](https://docs.datahub.com/docs/lineage/openlineage#microsoft-fabric-onelake). That lineage lands on separate `spark` DataFlows, one per notebook; the pipeline's Notebook activity DataJob is not linked to them.
- **Connection resolution**: Unmapped connection types fall back to using the connection type string as the platform name, which may not match your existing DataHub platform names. Use `platform_instance_map` to explicitly map connection names.

### Troubleshooting

- **401/403 errors**: Ensure the service principal has the correct Fabric API permissions and is added as a workspace member.
- **Empty results**: Check that `workspace_pattern` and `pipeline_pattern` are not filtering out all items.
- **Missing lineage**: Verify that `include_lineage: true` is set and that Fabric connections are properly configured for the pipelines. Also review the [Lineage limitations](#lineage) section for unsupported activity types and scenarios.
- **Stale entities**: Enable `stateful_ingestion` to automatically remove entities that no longer exist in Fabric.
