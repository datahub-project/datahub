# Matillion

This source extracts metadata from [Matillion Data Productivity Cloud](https://www.matillion.com/), a cloud-native data transformation and orchestration platform.

## Capabilities

| Capability          | Status | Details                                    |
| ------------------- | ------ | ------------------------------------------ |
| Platform Instance   | ✅     | Enabled by default                         |
| Data Containers     | ✅     | Projects and Environments                  |
| Pipelines           | ✅     | Orchestration and Transformation pipelines |
| Streaming Pipelines | ✅     | CDC (Change Data Capture) pipelines        |
| Pipeline Executions | ✅     | Execution history as DataProcessInstances  |
| Dataset Lineage     | ✅     | Table-level lineage from pipeline graphs   |
| Column Lineage      | ✅     | Field-level lineage when available         |
| Deletion Detection  | ✅     | Via stateful ingestion                     |

## Concept Mapping

| Matillion Object     | DataHub Entity                                                                          | Description                                   |
| -------------------- | --------------------------------------------------------------------------------------- | --------------------------------------------- |
| `Project`            | [Container](../../docs/generated/metamodel/entities/container.md)                       | Top-level organizational unit                 |
| `Environment`        | [Container](../../docs/generated/metamodel/entities/container.md)                       | Sub-container within a project                |
| `Pipeline`           | [Data Flow](../../docs/generated/metamodel/entities/dataFlow.md)                        | Orchestration or transformation pipeline      |
| `Streaming Pipeline` | [Data Flow](../../docs/generated/metamodel/entities/dataFlow.md)                        | CDC pipeline for real-time data ingestion     |
| `Pipeline Execution` | [Data Process Instance](../../docs/generated/metamodel/entities/dataProcessInstance.md) | Execution instance with status and statistics |
| `Connection`         | [Dataset](../../docs/generated/metamodel/entities/dataset.md)                           | Database/warehouse connection (optional)      |

## Features

### Pipeline Ingestion

Extracts Matillion pipelines as DataHub DataFlows, including:

- **Orchestration Pipelines**: Standard transformation and orchestration workflows
- **Streaming Pipelines**: CDC (Change Data Capture) pipelines for real-time data ingestion
- Pipeline metadata (name, description, version, branch)
- Project and environment organization
- Schedule information (cron expressions)
- Git repository integration

**Note on Granularity:**

- Matillion pipelines are ingested as single DataFlow entities (pipeline-level)
- Individual jobs/tasks/steps within pipelines are not exposed by the Matillion API
- Lineage graph shows transformation flow across the entire pipeline

### Lineage Extraction

Automatically extracts lineage from Matillion pipeline graphs:

- **Table-Level Lineage**: Upstream and downstream dataset dependencies
- **Column-Level Lineage**: Fine-grained field-to-field lineage mappings with intelligent column matching
- **Cross-Platform Support**: Lineage to/from Snowflake, BigQuery, Redshift, Postgres, and more
- **Transform Transparency**: Shows how data flows through transformation steps
- **Fuzzy Column Matching**: Handles case sensitivity differences (e.g., Snowflake's lowercase normalization)

**Important - Known URNs Approach:**

- **Dataset Lineage**: Emitted as `UpstreamLineage` aspect on Dataset entities that already exist in DataHub
- **Column Lineage**: Emitted as `fineGrainedLineages` within the `UpstreamLineage` aspect when `include_column_lineage: true`
- **Why**: This prevents broken lineage references and ensures data quality by validating datasets exist before linking them
- **Requirement**: Ingest source datasets (Snowflake, BigQuery, etc.) before running Matillion ingestion to get full lineage propagation
- **Limitation**: Pipeline-level lineage (DataJob inlet/outlet) is not available since the Matillion API doesn't expose individual job/task-level details within pipelines

### Execution History

Captures pipeline execution runs as DataProcessInstances:

- Execution status (success, failed, cancelled)
- Duration and row counts
- Trigger information (scheduled vs ad-hoc)
- Error messages and debugging information

### Container Organization

Projects and Environments are extracted as DataHub containers for hierarchical navigation:

- **Projects** → Top-level containers
- **Environments** → Sub-containers within projects
- **Pipelines** → Organized under environments or projects
- **Streaming Pipelines** → Separate organization for CDC workloads
- **Connections** → Grouped under a "Connections" container within each project

### Connection Entities

Matillion connections can be emitted as DataHub Dataset entities (enabled by default):

- Represents database and warehouse connections used by pipelines
- Includes connection type, configuration details, and metadata
- Links pipelines to their source/target databases
- Organized under a "Connections" container within each project

## Compatibility

### Supported Platforms for Lineage

The connector automatically maps Matillion connection types to DataHub platform names for proper lineage tracking.

<details>
<summary><b>Data Warehouses</b></summary>

- Snowflake
- Google BigQuery
- Amazon Redshift
- Databricks
- Azure Synapse Analytics
- SAP HANA
- Firebolt
- Dremio

</details>

<details>
<summary><b>Databases</b></summary>

- PostgreSQL
- MySQL
- Microsoft SQL Server
- Oracle
- IBM DB2
- Teradata
- MongoDB
- Cassandra

</details>

<details>
<summary><b>Cloud Storage</b></summary>

- Amazon S3
- Google Cloud Storage (GCS)
- Azure Blob Storage (ABS)

</details>

<details>
<summary><b>Streaming Platforms</b></summary>

- Apache Kafka
- Delta Lake
- Elasticsearch

</details>

:::note
If your source or destination type is not automatically recognized, it will be mapped to `external_db` as a fallback platform. For the complete list of supported Matillion connectors, refer to the [Matillion documentation](https://docs.matillion.com/).
:::

## Setup

### Prerequisites

Generate a Matillion API token from your Matillion account:

1. Log into your Matillion Data Productivity Cloud account
2. Navigate to **Settings** → **API Tokens**
3. Click **Generate New Token**
4. Copy the token (you'll need it for configuration)

### Quickstart

```yaml
source:
  type: matillion
  config:
    api_config:
      api_token: "${MATILLION_API_TOKEN}"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Advanced Configuration

```yaml
source:
  type: matillion
  config:
    # REQUIRED: API Configuration
    api_config:
      api_token: "${MATILLION_API_TOKEN}"
      # Optional: Change region (EU1 or US1)
      region: "EU1" # Options: EU1, US1 (Default: EU1)
      # Or use custom base URL:
      # custom_base_url: "https://custom.matillion.com/dpc"
      request_timeout_sec: 30 # Default: 30

    # OPTIONAL: Environment and instance
    env: "PROD" # Default: PROD
    platform_instance: "matillion-prod" # Optional

    # Lineage extraction
    include_lineage: true # Default: true
    include_column_lineage: true # Default: true (requires include_lineage)

    # Streaming pipelines (CDC)
    include_streaming_pipelines: true # Default: true
    streaming_pipeline_patterns:
      allow:
        - ".*" # All streaming pipelines
      deny:
        - "test-.*"

    # Audit Events & Consumption Metrics
    include_audit_events: true # Default: true. Includes last modified info on pipelines.
    include_consumption_metrics: false # Default: false. Includes credit usage on projects.

    # Connections
    emit_connection_datasets: true # Default: true. Emits Matillion connections as DataHub Dataset entities.

    # Pipeline execution history
    include_pipeline_executions: true # Default: true
    max_executions_per_pipeline: 10 # Default: 10

    # Container organization
    extract_projects_to_containers: true # Default: true

    # Filtering
    pipeline_patterns:
      allow:
        - "prod-.*"
      deny:
        - "test-.*"

    project_patterns:
      allow:
        - "analytics.*"

    # Stateful ingestion
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Configuration Options

| Option                           | Type             | Default   | Description                                               |
| -------------------------------- | ---------------- | --------- | --------------------------------------------------------- |
| **API Configuration**            |                  |           |                                                           |
| `api_config.api_token`           | string           | Required  | Matillion API bearer token                                |
| `api_config.region`              | enum             | `EU1`     | Matillion region: `EU1` or `US1`                          |
| `api_config.custom_base_url`     | string           | None      | Custom API base URL (overrides region)                    |
| `api_config.request_timeout_sec` | int              | 30        | Request timeout in seconds                                |
| **Environment & Instance**       |                  |           |                                                           |
| `env`                            | string           | `PROD`    | Environment for all emitted assets                        |
| `platform_instance`              | string           | None      | Platform instance identifier                              |
| **Lineage**                      |                  |           |                                                           |
| `include_lineage`                | boolean          | true      | Extract dataset lineage from pipelines                    |
| `include_column_lineage`         | boolean          | true      | Extract column-level lineage (requires `include_lineage`) |
| **Streaming Pipelines**          |                  |           |                                                           |
| `include_streaming_pipelines`    | boolean          | true      | Ingest CDC streaming pipelines                            |
| `streaming_pipeline_patterns`    | AllowDenyPattern | Allow all | Filter streaming pipelines by name                        |
| **Audit & Consumption**          |                  |           |                                                           |
| `include_audit_events`           | boolean          | true      | Include audit events as custom properties on pipelines    |
| `include_consumption_metrics`    | boolean          | false     | Include credit usage as custom properties on projects     |
| **Connections**                  |                  |           |                                                           |
| `emit_connection_datasets`       | boolean          | true      | Emit Matillion connections as DataHub Dataset entities    |
| **Pipeline Executions**          |                  |           |                                                           |
| `include_pipeline_executions`    | boolean          | true      | Ingest pipeline execution history                         |
| `max_executions_per_pipeline`    | int              | 10        | Max executions to ingest per pipeline                     |
| **Organization**                 |                  |           |                                                           |
| `extract_projects_to_containers` | boolean          | true      | Create containers for projects/environments               |
| **Filtering**                    |                  |           |                                                           |
| `pipeline_patterns`              | AllowDenyPattern | Allow all | Filter orchestration pipelines by name                    |
| `project_patterns`               | AllowDenyPattern | Allow all | Filter projects by name                                   |
| **Stateful Ingestion**           |                  |           |                                                           |
| `stateful_ingestion.enabled`     | boolean          | false     | Enable stateful ingestion                                 |

## Advanced Configuration

### Working with Platform Instances

If you have multiple instances of source or destination systems, configure platform instances to generate correct lineage edges.

#### Example: Multiple Snowflake Instances

```yaml
source:
  type: matillion
  config:
    api_config:
      api_token: "${MATILLION_API_TOKEN}"

    # Configure platform instance for Snowflake datasets
    platform_instance: "matillion-prod"

    # DataHub environment
    env: "PROD"
```

When ingesting datasets from Snowflake or other platforms, make sure the `platform_instance` and `env` match between the Matillion connector and your source platform connectors (e.g., Snowflake, BigQuery) to ensure correct lineage linking.

### Finding Project and Pipeline IDs

Project and pipeline IDs can be found in the Matillion UI:

1. **Project ID**: Navigate to a project, check the URL

   - URL format: `https://region.dpc.matillion.com/projects/{project_id}`
   - Example: `https://eu1.dpc.matillion.com/projects/proj-abc123` → project_id is `proj-abc123`

2. **Pipeline ID**: Navigate to a pipeline, check the URL
   - URL format: `https://region.dpc.matillion.com/pipelines/{pipeline_id}`
   - Example: `https://eu1.dpc.matillion.com/pipelines/pipe-xyz789` → pipeline_id is `pipe-xyz789`

Alternatively, you can use the Matillion API to list projects and pipelines:

```bash
# List projects
curl -H "Authorization: Bearer YOUR_API_TOKEN" \
     https://eu1.api.matillion.com/dpc/v1/projects

# List pipelines for a project
curl -H "Authorization: Bearer YOUR_API_TOKEN" \
     https://eu1.api.matillion.com/dpc/v1/pipelines?projectId=YOUR_PROJECT_ID
```

### Filtering Projects and Pipelines

Control which projects and pipelines are ingested using regex patterns:

```yaml
# Include only production projects
project_patterns:
  allow:
    - "prod-.*"
    - "production-.*"
  deny:
    - "test-.*"
    - ".*-dev"

# Include only specific pipelines
pipeline_patterns:
  allow:
    - "etl-.*"
    - "analytics-.*"
  deny:
    - ".*-draft"
    - "experimental-.*"

# Filter streaming pipelines separately
streaming_pipeline_patterns:
  allow:
    - "cdc-.*"
  deny:
    - ".*-testing"
```

### Controlling Pipeline Execution History

Limit the number of historical pipeline executions ingested:

```yaml
include_pipeline_executions: true
max_executions_per_pipeline: 10 # Last 10 runs per pipeline (default)
```

Set `include_pipeline_executions: false` to skip execution history entirely for faster ingestion.

### Lineage Configuration

Fine-tune lineage extraction behavior:

```yaml
# Enable/disable lineage extraction
include_lineage: true # Extract table-level lineage (default: true)
include_column_lineage: true # Extract column-level lineage (default: true)

# Important: Ingest source datasets first!
# Lineage is only emitted on datasets that already exist in DataHub
```

**Best Practice**: Run ingestion in this order:

1. Ingest source platforms (Snowflake, BigQuery, etc.) → DataHub
2. Run Matillion ingestion → Enjoy complete lineage!

### Audit Events and Consumption Metrics

Enrich pipeline metadata with audit and consumption information:

```yaml
# Add "last modified by" and "last modified at" to pipelines
include_audit_events: true # Default: true

# Add credit usage metrics to projects
include_consumption_metrics: false # Default: false (can be expensive for large workspaces)
```

### Connection Datasets

Control whether Matillion connections are emitted as DataHub entities:

```yaml
# Emit connections as Dataset entities
emit_connection_datasets: true # Default: true

# When disabled, connections are not created as entities,
# but lineage still flows through pipelines
```

## Current Limitations

1. **Job-Level Granularity**: The Matillion API does not expose individual job/task-level details within pipelines. Each pipeline is ingested as a single DataFlow entity, and lineage shows transformation flow across the entire pipeline.

2. **Pipeline Inlet/Outlet Lineage**: Pipeline-level lineage (DataJob inlet/outlet) is not available due to the lack of job-level granularity in the Matillion API.

3. **Known URNs Requirement**: Dataset and column-level lineage is only emitted on datasets that already exist in DataHub. This prevents broken lineage references and ensures data quality. Make sure to ingest source datasets before running Matillion ingestion.

4. **API Coverage**: The connector covers 11 out of 14 Matillion API endpoints. The following endpoints are not currently used:

   - SCIM endpoints (users/groups) - not yet integrated
   - Branches endpoint - not yet integrated
   - Webhooks endpoint - not yet integrated

5. **Column Lineage Availability**: Column-level lineage depends on the Matillion lineage API providing column-level information. Not all pipelines may have this data available.

6. **Region Support**: Currently supports EU1 and US1 regions. For custom or on-premise installations, use the `custom_base_url` configuration.

## Troubleshooting

### Authentication Errors

```
Error: 401 Unauthorized
```

**Solution**: Verify your API token is correct and has not expired. Generate a new token from the Matillion Settings → API Tokens page.

```
Error: 403 Forbidden
```

**Solution**: Your API token doesn't have sufficient permissions. Ensure the token has read access to projects, pipelines, environments, and executions.

### No metadata produced

1. **Check API token**: Ensure your API token is valid and has correct permissions

   ```bash
   # Test connectivity
   curl -H "Authorization: Bearer YOUR_TOKEN" \
        https://eu1.api.matillion.com/dpc/v1/projects
   ```

2. **Verify region**: Ensure `region` matches your Matillion account (EU1 vs US1)

   - EU accounts: `region: "EU1"` (default)
   - US accounts: `region: "US1"`

3. **Review patterns**: Check that `pipeline_patterns` and `project_patterns` aren't too restrictive

   ```yaml
   # Temporarily allow all to test
   project_patterns:
     allow:
       - ".*"
   ```

4. **Check logs**: Review DataHub ingestion logs for warnings/errors
   ```bash
   # Look for errors in ingestion logs
   grep -i "error\|warning" /path/to/datahub/logs/ingestion.log
   ```

### Missing pipelines

1. **Verify project access**: Ensure your API token has access to the projects

   - Check Matillion UI: Navigate to the project and verify you can see pipelines
   - Use API to list pipelines:
     ```bash
     curl -H "Authorization: Bearer YOUR_TOKEN" \
          "https://eu1.api.matillion.com/dpc/v1/pipelines?projectId=YOUR_PROJECT_ID"
     ```

2. **Check filters**: Review `pipeline_patterns` configuration

   ```yaml
   # Debug: Allow all pipelines temporarily
   pipeline_patterns:
     allow:
       - ".*"
     deny: []
   ```

3. **Confirm region**: Verify `region` matches your Matillion account (EU1 vs US1)

### Slow ingestion

1. **Reduce execution history**: Lower `max_executions_per_pipeline` value

   ```yaml
   max_executions_per_pipeline: 5 # Reduce from default 10
   # Or disable entirely:
   # include_pipeline_executions: false
   ```

2. **Increase timeout**: Raise `request_timeout_sec` if experiencing timeouts

   ```yaml
   api_config:
     request_timeout_sec: 60 # Increase from default 30
   ```

3. **Filter projects**: Use `project_patterns` to limit scope

   ```yaml
   project_patterns:
     allow:
       - "prod-.*" # Only production projects
   ```

4. **Disable expensive features**:
   ```yaml
   include_lineage: false # Skip lineage extraction
   include_consumption_metrics: false # Skip consumption data
   include_audit_events: false # Skip audit events
   ```

### Missing lineage

1. **Check pipeline lineage**: Not all pipelines may have lineage data available in Matillion API

   - Navigate to the pipeline in Matillion UI
   - Check if "Lineage" tab shows data
   - If empty in UI, it won't be available via API

2. **Verify platform support**: Ensure your source/target platforms are in the supported list

   - Check the "Supported Platforms for Lineage" section above
   - Unsupported platforms will be mapped to `external_db`

3. **Ingest datasets first**:

   - **Requirement**: Lineage is only emitted on datasets that already exist in DataHub
   - **Action**: Run dataset ingestion from source platforms before Matillion ingestion

   **Recommended ingestion order**:

   ```bash
   # Step 1: Ingest source platforms
   datahub ingest -c snowflake_recipe.yml
   datahub ingest -c bigquery_recipe.yml

   # Step 2: Ingest Matillion (lineage will now work)
   datahub ingest -c matillion_recipe.yml
   ```

4. **Review logs**: Check for warnings like:

   ```
   Skipping lineage for urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)
   - dataset not found in DataHub
   ```

5. **Platform instance mismatch**: Ensure `platform_instance` and `env` match between Matillion and source connectors

   ```yaml
   # Matillion config
   platform_instance: "matillion-prod"
   env: "PROD"

   # Snowflake config (must match!)
   platform_instance: "snowflake-prod" # Different instance name is OK
   env: "PROD" # Same env is required
   ```

6. **Enable debug logging**:
   ```yaml
   source:
     type: matillion
     config:
       # ... other config ...
       # debug: true # Enable detailed logging (if supported)
   ```

### Missing column-level lineage

1. **Enable feature**: Ensure `include_column_lineage: true`
2. **Requires table-level lineage**: Column lineage requires `include_lineage: true`
3. **Check Matillion lineage data**: Not all pipelines provide column-level information
4. **Verify schemas exist**: Column matching requires schemas to exist in DataHub for both source and target datasets
5. **Check logs for schema resolution warnings**:
   ```
   Failed to resolve schema for dataset urn:li:dataset:...
   ```

### Missing streaming pipelines

1. **Enable feature**: Ensure `include_streaming_pipelines: true`

   ```yaml
   include_streaming_pipelines: true
   ```

2. **Check filters**: Review `streaming_pipeline_patterns` configuration

   ```yaml
   # Debug: Allow all streaming pipelines
   streaming_pipeline_patterns:
     allow:
       - ".*"
     deny: []
   ```

3. **Verify CDC pipelines exist**: Confirm you have CDC/streaming pipelines configured in Matillion
   - Navigate to your Matillion project
   - Check if streaming pipelines are visible in the UI
   - Use API to verify:
     ```bash
     curl -H "Authorization: Bearer YOUR_TOKEN" \
          "https://eu1.api.matillion.com/dpc/v1/streaming-pipelines?projectId=YOUR_PROJECT_ID"
     ```

### Rate Limiting

```
Error: 429 Too Many Requests
```

**Solution**: The connector automatically retries with exponential backoff. For large workspaces, consider:

- Using filtering patterns to reduce the number of projects/pipelines processed
- Reducing `max_executions_per_pipeline` to limit API calls
- Adding delays between batches (contact DataHub support for batch configuration)

### Connection Timeout

```
Error: Request timeout
```

**Solutions**:

1. **Increase timeout**:

   ```yaml
   api_config:
     request_timeout_sec: 120 # Increase from default 30
   ```

2. **Check network connectivity**:

   ```bash
   # Test connectivity to Matillion API
   curl -I https://eu1.api.matillion.com/dpc/v1/projects
   ```

3. **Verify firewall rules**: Ensure outbound HTTPS traffic to `*.api.matillion.com` is allowed

## API Reference

Based on [Matillion Data Productivity Cloud Public API v1.0](https://matillion-docs.s3.eu-west-1.amazonaws.com/data-productivity-cloud-public-api/public-api-endpoint-reference.yaml)
