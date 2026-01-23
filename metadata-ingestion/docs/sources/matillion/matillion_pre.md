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

| Matillion Object     | DataHub Entity                                                                                      | Description                                                   |
| -------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `Project`            | [Container](../../metamodel/entities/container.md)                                                  | Top-level organizational unit                                 |
| `Environment`        | [Container](../../metamodel/entities/container.md)                                                  | Sub-container within a project                                |
| `Pipeline`           | [Data Flow](../../metamodel/entities/dataFlow.md) + [Data Job](../../metamodel/entities/dataJob.md) | Orchestration or transformation pipeline with executable job  |
| `Streaming Pipeline` | [Data Flow](../../metamodel/entities/dataFlow.md) + [Data Job](../../metamodel/entities/dataJob.md) | CDC pipeline for real-time data ingestion with executable job |
| `Pipeline Execution` | [Data Process Instance](../../metamodel/entities/dataProcessInstance.md)                            | Execution instance with status and statistics                 |

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

- Each Matillion pipeline is ingested as a DataFlow (workflow) with a corresponding DataJob (executable template)
- Individual jobs/tasks/steps within pipelines are not exposed by the Matillion API
- Lineage is extracted from OpenLineage events provided by the Matillion API

### Lineage Extraction

Automatically extracts lineage from OpenLineage events provided by the Matillion Public API:

- **Table-Level Lineage**: Upstream and downstream dataset dependencies from OpenLineage events
- **Column-Level Lineage**: Fine-grained field-to-field lineage mappings when available in OpenLineage data
- **SQL Parsing**: Optional SQL parsing for enhanced column-level lineage using DataHub's SqlParsingAggregator
- **Cross-Platform Support**: Lineage to/from Snowflake, BigQuery, Redshift, Postgres, and more
- **Platform-Specific Handling**: Automatic field name normalization (e.g., Snowflake's lowercase conversion)

**Lineage Emission:**

- **DataJob Lineage**: Emitted as `dataJobInputOutput` aspect on DataJob entities (pipeline-level)
- **Dataset Lineage**: Input/output datasets are linked to each pipeline execution
- **Column Lineage**: Emitted as `fineGrainedLineages` when available in OpenLineage events
- **SQL Queries**: Stored in `DataTransformLogicClass` aspect for visibility and further analysis

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
- **DataFlows & DataJobs** → Organized under environments with hierarchical browse paths

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
    parse_sql_for_lineage: true # Default: true. Use SQL parsing for enhanced column lineage

    # OpenLineage configuration
    namespace_to_platform_instance:
      "snowflake://prod-account":
        platform_instance: "snowflake_prod"
        env: "PROD"
        convert_urns_to_lowercase: true
      "postgresql://prod-db:5432":
        platform_instance: "postgres_prod"
        env: "PROD"
        database: "analytics"
        schema: "public"

    # Streaming pipelines (CDC)
    include_streaming_pipelines: true # Default: true
    streaming_pipeline_patterns:
      allow:
        - ".*" # All streaming pipelines
      deny:
        - "test-.*"

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
| `include_lineage`                | boolean          | true      | Extract dataset lineage from OpenLineage events           |
| `include_column_lineage`         | boolean          | true      | Extract column-level lineage (requires `include_lineage`) |
| `parse_sql_for_lineage`          | boolean          | true      | Use SQL parsing for enhanced column-level lineage         |
| `namespace_to_platform_instance` | dict             | {}        | Map OpenLineage namespaces to platform instances          |
| `lineage_start_days_ago`         | int              | 7         | Number of days to fetch OpenLineage events                |
| **Streaming Pipelines**          |                  |           |                                                           |
| `include_streaming_pipelines`    | boolean          | true      | Ingest CDC streaming pipelines                            |
| `streaming_pipeline_patterns`    | AllowDenyPattern | Allow all | Filter streaming pipelines by name                        |
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
include_lineage: true # Extract lineage from OpenLineage events (default: true)
include_column_lineage: true # Extract column-level lineage (default: true)
parse_sql_for_lineage: true # Use SQL parsing for enhanced lineage (default: true)

# Fetch OpenLineage events from the last N days
lineage_start_days_ago: 7 # Default: 7 days

# Map OpenLineage namespaces to platform instances
namespace_to_platform_instance:
  "snowflake://prod-account.us-east-1":
    platform_instance: "snowflake_prod"
    env: "PROD"
    database: "PROD_DB" # Optional: default database
    schema: "PUBLIC" # Optional: default schema
    convert_urns_to_lowercase: true # Match Snowflake connector behavior
```

**SQL Parsing for Enhanced Lineage:**

When `parse_sql_for_lineage: true`, the connector uses DataHub's `SqlParsingAggregator` to extract additional column-level lineage from SQL queries found in OpenLineage events. This provides:

- More accurate column-to-column transformations
- Detection of computed columns and expressions
- Lineage for columns not explicitly in the OpenLineage columnLineage facet

## Current Limitations

1. **Job-Level Granularity**: The Matillion API does not expose individual job/task/step-level details within pipelines. Each pipeline is ingested as a DataFlow with a single corresponding DataJob.

2. **OpenLineage Dependency**: Lineage extraction depends on OpenLineage events being available via the Matillion API. Events are only retained for a limited time window.

3. **Column Lineage Availability**: Column-level lineage depends on:

   - OpenLineage events containing `columnLineage` facets
   - SQL queries being present in OpenLineage events (for SQL parsing)
   - Not all pipelines may have this data available

4. **API Coverage**: The connector uses the following Matillion Public API endpoints:

   - `/v1/projects` - List projects
   - `/v1/environments` - List environments
   - `/v1/pipelines` - List orchestration pipelines
   - `/v1/streaming-pipelines` - List CDC pipelines
   - `/v1/pipeline-executions` - List pipeline execution history
   - `/v1/lineage/events` - Fetch OpenLineage events
   - `/v1/schedules` - List pipeline schedules

5. **Region Support**: Currently supports EU1 and US1 regions. For custom or on-premise installations, use the `custom_base_url` configuration.

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

1. **Check OpenLineage events**: Lineage is extracted from OpenLineage events provided by the Matillion API

   - Verify events exist for your time window:
     ```bash
     curl -H "Authorization: Bearer YOUR_TOKEN" \
          "https://eu1.api.matillion.com/dpc/v1/lineage/events?startTime=2024-01-01T00:00:00Z"
     ```
   - If no events are returned, lineage data is not available for that time range

2. **Adjust time window**: Increase `lineage_start_days_ago` to fetch older events

   ```yaml
   lineage_start_days_ago: 14 # Fetch last 14 days (default: 7)
   ```

3. **Configure namespace mappings**: Ensure OpenLineage namespaces are correctly mapped

   ```yaml
   namespace_to_platform_instance:
     "snowflake://prod-account":
       platform_instance: "snowflake_prod"
       env: "PROD"
       convert_urns_to_lowercase: true
   ```

4. **Verify platform support**: Check the pre-mapped platforms in the configuration documentation

   - Platforms are automatically detected from namespace URIs
   - Use `lineage_platform_mapping` to override defaults

5. **Review logs**: Check for warnings about failed lineage parsing

   ```
   Failed to parse lineage event: ...
   Error parsing dataset namespace/name: ...
   ```

### Missing column-level lineage

1. **Enable feature**: Ensure `include_column_lineage: true`
2. **Requires table-level lineage**: Column lineage requires `include_lineage: true`
3. **Check OpenLineage data**: Not all OpenLineage events contain `columnLineage` facets
4. **SQL parsing**: Enable `parse_sql_for_lineage: true` for enhanced column lineage from SQL queries
5. **Review logs**: Check for column lineage parsing warnings:
   ```
   No column lineage facet found in OpenLineage event
   Failed to parse SQL for lineage: unsupported dialect
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
