


# Matillion

## Overview

Matillion Data Productivity Cloud (DPC) is a cloud-native data integration platform for building, orchestrating, and monitoring data pipelines. Learn more in the [official Matillion documentation](https://docs.matillion.com/data-productivity-cloud/).

The DataHub integration for Matillion DPC ingests pipelines, streaming pipelines, projects, and environments as DataHub entities. It captures table- and column-level lineage via the Matillion OpenLineage API, pipeline execution history as operational metadata, and child pipeline dependency relationships for end-to-end orchestration visibility.

## Concept Mapping

| Source Concept              | DataHub Concept     | Notes                                                                |
| --------------------------- | ------------------- | -------------------------------------------------------------------- |
| Project                     | Container           | Top-level grouping of pipelines within a Matillion account.          |
| Environment                 | Container           | Deployment environment within a project (e.g. Production, Staging).  |
| Pipeline                    | DataFlow            | An orchestration pipeline that transforms or moves data.             |
| Pipeline Component / Step   | DataJob             | An individual step within a pipeline.                                |
| Streaming Pipeline          | DataFlow            | A CDC or streaming pipeline, emitted with `pipeline_type=streaming`. |
| Pipeline Execution          | DataProcessInstance | A single run of a pipeline, including status and timing.             |
| OpenLineage table reference | Dataset             | Upstream or downstream dataset referenced via OpenLineage events.    |
| Table/column lineage edge   | Lineage edge        | Extracted from OpenLineage events; column-level via SQL parsing.     |


## Module `matillion-dpc`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Column-level Lineage | ✅ | Enabled by default, can be disabled via configuration `parse_sql_for_lineage`. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default via OpenLineage data from pipeline executions. |

### Overview

The `matillion-dpc` module ingests metadata from Matillion Data Productivity Cloud (DPC) into DataHub. It extracts pipelines, streaming pipelines, projects, environments, execution history, and table and column-level lineage via the Matillion OpenLineage API.

### Prerequisites

#### Obtain API Credentials

The connector uses OAuth2 client credentials and automatically handles token generation and refresh.

1. Log into Matillion Data Productivity Cloud as a **Super Admin**
2. Navigate to **Profile & Account** → **API credentials**
3. Click **Set an API Credential**
4. Provide a descriptive name (e.g., "DataHub Integration")
5. Assign an **Account Role** with read permissions to required APIs
6. Click **Save** and immediately copy the **Client Secret** (not shown again)
7. Note the **Client ID** (remains visible)

For detailed instructions, see [Matillion API Authentication](https://docs.matillion.com/data-productivity-cloud/api/docs/authentication/).

#### Required Permissions

The API credentials must have an **Account Role** with **Read** permissions to:

- **Projects** (`/v1/projects`)
- **Environments** (`/v1/environments`)
- **Pipelines** (`/v1/pipelines`)
- **Schedules** (`/v1/schedules`)
- **Lineage Events** (`/v1/lineage/events`)
- **Pipeline Executions** (`/v1/pipeline-executions`) - optional
- **Streaming Pipelines** (`/v1/streaming-pipelines`) - optional

If using an account role other than **Super Admin**, grant project and environment-level roles as needed.

See [Matillion RBAC documentation](https://docs.matillion.com/data-productivity-cloud/hub/docs/role-based-access-control-overview/) for details.

#### Lineage and Dependencies

The connector automatically extracts:

1. **Table and Column-Level Lineage** - From OpenLineage Events API (`/v1/lineage/events`) ([docs](https://docs.matillion.com/data-productivity-cloud/api/docs/endpoint-reference/?fullpage=true#/Data%20Lineage/get-lineage-events))
2. **Operational Metadata** - Pipeline execution history from Pipeline Executions API (`/v1/pipeline-executions`) emitted as DataProcessInstance entities ([docs](https://docs.matillion.com/data-productivity-cloud/api/docs/endpoint-reference/?fullpage=true#/Pipeline%20Execution/getPipelineExecutions))
3. **Child Pipeline Dependencies** - Automatically tracks when pipelines call other pipelines, creating step-to-step dependency relationships for comprehensive pipeline orchestration visibility

#### OpenLineage Namespace Mapping (Optional)

**Optional**: Map OpenLineage namespace URIs to DataHub platform instances for lineage connections. If not configured, the connector extracts platform type from URIs (e.g., `postgresql://...` → `postgres`) with default environment (`PROD`).

**When to use**: Configure this when you need lineage to connect to existing datasets with platform instances.

Example namespaces: `postgresql://host:5432`, `snowflake://account.snowflakecomputing.com`, `bigquery://project`

```yaml
namespace_to_platform_instance:
  "postgresql://prod-db.us-east-1.rds.amazonaws.com:5432":
    platform_instance: postgres_prod
    env: PROD
    database: analytics
    schema: public

  "snowflake://prod-account.snowflakecomputing.com":
    platform_instance: snowflake_prod
    env: PROD
    convert_urns_to_lowercase: true
```

Platform instances must match those used when ingesting the source data platforms.


### Install the Plugin
```shell
pip install 'acryl-datahub[matillion-dpc]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: matillion-dpc
  config:
    api_config:
      client_id: "${MATILLION_CLIENT_ID}"
      client_secret: "${MATILLION_CLIENT_SECRET}"
      region: "EU1" # EU1, US1, or AU1

    env: "PROD"

    # Optional: Map OpenLineage namespaces to DataHub platform instances
    # Required if existing datasets use platform instances
    namespace_to_platform_instance:
      "postgresql://prod-db.us-east-1.rds.amazonaws.com:5432":
        platform_instance: postgres_prod
        env: PROD
        database: analytics
        schema: public

      "snowflake://prod-account.snowflakecomputing.com":
        platform_instance: snowflake_prod
        env: PROD
        convert_urns_to_lowercase: true

      "bigquery://my-gcp-project":
        platform_instance: bigquery_prod
        env: PROD

    include_streaming_pipelines: true
    include_unpublished_pipelines: true
    max_executions_per_pipeline: 10
    extract_projects_to_containers: true

    # Optional: Filter projects, environments, pipelines using regex patterns
    # project_patterns:
    #   allow: ["^prod-.*", "^staging-.*"]
    #   deny: [".*-deprecated$", ".*-archived$"]

    # environment_patterns:
    #   allow: ["^production$", "^staging$"]

    # pipeline_patterns:
    #   deny: ["^test_.*", ".*_backup$"]

    # streaming_pipeline_patterns:
    #   allow: ["^cdc_.*"]

    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_config</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">MatillionAPIConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">api_config.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if api_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Matillion API Client ID for OAuth2 authentication.  |
| <div className="path-line"><span className="path-prefix">api_config.</span><span className="path-main">client_secret</span>&nbsp;<abbr title="Required if api_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Matillion API Client Secret for OAuth2 authentication.  |
| <div className="path-line"><span className="path-prefix">api_config.</span><span className="path-main">custom_base_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Custom API base URL for VPC endpoints or on-premise installations. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">api_config.</span><span className="path-main">custom_oauth_token_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Custom OAuth2 token endpoint URL for VPC endpoints or on-premise installations. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">api_config.</span><span className="path-main">region</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "EU1", "US1", "AU1"  |
| <div className="path-line"><span className="path-prefix">api_config.</span><span className="path-main">request_timeout_sec</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Per-request timeout in seconds. On timeouts, prefer narrowing start_time/end_time or enabling stateful ingestion over a large timeout. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-main">extract_projects_to_containers</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to add the top-level Matillion project as a DataHub container. Environment and folder containers are always created; this only controls whether they are nested under a project container or hang at the root. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_run_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit run history (DataProcessInstances) for published pipelines by fetching their executions in the time window. Implied when include_unpublished_pipelines is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_dependent_pipelines</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest dependent (child) pipelines that are only referenced via lineage events and were not discovered as project pipelines (published or via execution history). When disabled, lineage is still emitted for discovered pipelines, but these lineage-only dependencies are not created as their own DataFlows/DataJobs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_external_urls</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to emit links back to the Matillion console (project, run, and pipeline). Off by default because the pipeline link searches the observability dashboard by file name and only resolves for pipelines that ran recently and whose editor name matches their file name. Enable to add all Matillion external links. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_streaming_pipelines</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest Matillion streaming pipelines (CDC pipelines). Streaming pipelines are emitted as separate DataFlows with pipeline_type='streaming'. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_unpublished_pipelines</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to discover and ingest unpublished pipelines from recent execution history. When enabled, the connector will discover pipelines that have been executed but not yet published. Disable this to only ingest published pipelines from the published-pipelines API. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">lineage_platform_mapping</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Override platform name mappings from OpenLineage namespaces to DataHub platforms. Only needed for non-standard platforms. See documentation for list of pre-mapped platforms. Example: {"customdb": "postgres", "mywarehouse": "snowflake"} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">max_executions_per_pipeline</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of recent pipeline executions to ingest per pipeline. Set to 0 to disable execution ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">parse_sql_for_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to parse SQL from OpenLineage events to extract additional column-level lineage. Requires DataHub graph access. When enabled, SQL queries are parsed to infer lineage beyond what's explicitly provided in OpenLineage column mappings. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by DataHub platform ingestion source belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">environment_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">environment_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">environment_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">environment_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">environment_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">environment_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">namespace_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of NamespacePlatformMapping, null</span></div> | Maps OpenLineage namespace prefixes to platform instance/environment using longest prefix matching. Unmapped namespaces extract platform from URI with defaults (env=PROD). Example: {"snowflake://prod-account": {"platform_instance": "snowflake_prod", "env": "PROD"}} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">namespace_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform instance to use for datasets from this namespace <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">namespace_to_platform_instance.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset URNs to lowercase for this namespace. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">namespace_to_platform_instance.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default database name to prepend if dataset name doesn't include database context <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">namespace_to_platform_instance.`key`.</span><span className="path-main">schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default schema name to prepend if dataset name doesn't include schema context <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">namespace_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Environment (PROD, DEV, etc.) to use for datasets from this namespace <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">pipeline_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">pipeline_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">pipeline_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">pipeline_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">pipeline_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">pipeline_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">project_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">project_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">project_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">project_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">project_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">streaming_pipeline_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">streaming_pipeline_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">streaming_pipeline_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">streaming_pipeline_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">streaming_pipeline_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">streaming_pipeline_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
    },
    "MatillionAPIConfig": {
      "additionalProperties": false,
      "properties": {
        "client_id": {
          "description": "Matillion API Client ID for OAuth2 authentication.",
          "format": "password",
          "title": "Client Id",
          "type": "string",
          "writeOnly": true
        },
        "client_secret": {
          "description": "Matillion API Client Secret for OAuth2 authentication.",
          "format": "password",
          "title": "Client Secret",
          "type": "string",
          "writeOnly": true
        },
        "region": {
          "$ref": "#/$defs/MatillionRegion",
          "default": "EU1",
          "description": "Matillion Data Productivity Cloud region (EU1, US1, or AU1)"
        },
        "custom_base_url": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Custom API base URL for VPC endpoints or on-premise installations.",
          "title": "Custom Base Url"
        },
        "custom_oauth_token_url": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Custom OAuth2 token endpoint URL for VPC endpoints or on-premise installations.",
          "title": "Custom Oauth Token Url"
        },
        "request_timeout_sec": {
          "default": 30,
          "description": "Per-request timeout in seconds. On timeouts, prefer narrowing start_time/end_time or enabling stateful ingestion over a large timeout.",
          "title": "Request Timeout Sec",
          "type": "integer"
        }
      },
      "required": [
        "client_id",
        "client_secret"
      ],
      "title": "MatillionAPIConfig",
      "type": "object"
    },
    "MatillionRegion": {
      "enum": [
        "EU1",
        "US1",
        "AU1"
      ],
      "title": "MatillionRegion",
      "type": "string"
    },
    "NamespacePlatformMapping": {
      "additionalProperties": false,
      "properties": {
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "DataHub platform instance to use for datasets from this namespace",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "Environment (PROD, DEV, etc.) to use for datasets from this namespace",
          "title": "Env",
          "type": "string"
        },
        "database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default database name to prepend if dataset name doesn't include database context",
          "title": "Database"
        },
        "schema": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default schema name to prepend if dataset name doesn't include schema context",
          "title": "Schema"
        },
        "convert_urns_to_lowercase": {
          "default": false,
          "description": "Whether to convert dataset URNs to lowercase for this namespace.",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "NamespacePlatformMapping",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by DataHub platform ingestion source belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion configuration."
    },
    "bucket_duration": {
      "$ref": "#/$defs/BucketDuration",
      "default": "DAY",
      "description": "Size of the time window to aggregate usage stats."
    },
    "end_time": {
      "description": "Latest date of lineage/usage to consider. Default: Current time in UTC",
      "format": "date-time",
      "title": "End Time",
      "type": "string"
    },
    "start_time": {
      "default": null,
      "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
      "format": "date-time",
      "title": "Start Time",
      "type": "string"
    },
    "api_config": {
      "$ref": "#/$defs/MatillionAPIConfig",
      "description": "Matillion API configuration"
    },
    "max_executions_per_pipeline": {
      "default": 10,
      "description": "Maximum number of recent pipeline executions to ingest per pipeline. Set to 0 to disable execution ingestion.",
      "title": "Max Executions Per Pipeline",
      "type": "integer"
    },
    "parse_sql_for_lineage": {
      "default": true,
      "description": "Whether to parse SQL from OpenLineage events to extract additional column-level lineage. Requires DataHub graph access. When enabled, SQL queries are parsed to infer lineage beyond what's explicitly provided in OpenLineage column mappings.",
      "title": "Parse Sql For Lineage",
      "type": "boolean"
    },
    "namespace_to_platform_instance": {
      "anyOf": [
        {
          "additionalProperties": {
            "$ref": "#/$defs/NamespacePlatformMapping"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Maps OpenLineage namespace prefixes to platform instance/environment using longest prefix matching. Unmapped namespaces extract platform from URI with defaults (env=PROD). Example: {\"snowflake://prod-account\": {\"platform_instance\": \"snowflake_prod\", \"env\": \"PROD\"}}",
      "title": "Namespace To Platform Instance"
    },
    "lineage_platform_mapping": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Override platform name mappings from OpenLineage namespaces to DataHub platforms. Only needed for non-standard platforms. See documentation for list of pre-mapped platforms. Example: {\"customdb\": \"postgres\", \"mywarehouse\": \"snowflake\"}",
      "title": "Lineage Platform Mapping"
    },
    "include_streaming_pipelines": {
      "default": true,
      "description": "Whether to ingest Matillion streaming pipelines (CDC pipelines). Streaming pipelines are emitted as separate DataFlows with pipeline_type='streaming'.",
      "title": "Include Streaming Pipelines",
      "type": "boolean"
    },
    "streaming_pipeline_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for filtering Matillion streaming pipelines to ingest."
    },
    "pipeline_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for filtering Matillion pipelines to ingest."
    },
    "project_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for filtering Matillion projects to ingest."
    },
    "environment_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for filtering Matillion environments to ingest."
    },
    "extract_projects_to_containers": {
      "default": true,
      "description": "Whether to add the top-level Matillion project as a DataHub container. Environment and folder containers are always created; this only controls whether they are nested under a project container or hang at the root.",
      "title": "Extract Projects To Containers",
      "type": "boolean"
    },
    "include_unpublished_pipelines": {
      "default": true,
      "description": "Whether to discover and ingest unpublished pipelines from recent execution history. When enabled, the connector will discover pipelines that have been executed but not yet published. Disable this to only ingest published pipelines from the published-pipelines API.",
      "title": "Include Unpublished Pipelines",
      "type": "boolean"
    },
    "extract_run_history": {
      "default": false,
      "description": "Emit run history (DataProcessInstances) for published pipelines by fetching their executions in the time window. Implied when include_unpublished_pipelines is enabled.",
      "title": "Extract Run History",
      "type": "boolean"
    },
    "include_dependent_pipelines": {
      "default": true,
      "description": "Whether to ingest dependent (child) pipelines that are only referenced via lineage events and were not discovered as project pipelines (published or via execution history). When disabled, lineage is still emitted for discovered pipelines, but these lineage-only dependencies are not created as their own DataFlows/DataJobs.",
      "title": "Include Dependent Pipelines",
      "type": "boolean"
    },
    "include_external_urls": {
      "default": false,
      "description": "Whether to emit links back to the Matillion console (project, run, and pipeline). Off by default because the pipeline link searches the observability dashboard by file name and only resolves for pipelines that ran recently and whose editor name matches their file name. Enable to add all Matillion external links.",
      "title": "Include External Urls",
      "type": "boolean"
    }
  },
  "required": [
    "api_config"
  ],
  "title": "MatillionSourceConfig",
  "type": "object"
}
```





### Capabilities

#### OpenLineage Namespace Mapping

**Optional configuration** to map OpenLineage namespace URIs to DataHub platform information. Without this, the connector extracts platform type from URIs with default environment.

**Fields:**

- **`platform_instance`**: Platform instance identifier (must match source ingestion)
- **`database`** / **`schema`**: Defaults for incomplete dataset names from OpenLineage
  - 3-tier platforms (Snowflake, Postgres, Redshift): `database.schema.table`
  - 2-tier platforms (MySQL, Hive): `schema.table`
- **`convert_urns_to_lowercase`**: Normalize URNs to lowercase (use `true` for Snowflake)
- **`env`**: Environment tag (PROD, DEV, etc.)

**Fallback behavior**: Unmapped namespaces extract platform type from the URI (e.g., `postgresql://...` → `postgres`) without platform instance assignment.

#### SQL Parsing for Column-Level Lineage

Enable `parse_sql_for_lineage: true` to parse SQL queries from OpenLineage events for additional column-level lineage.

**Requirements:**

- DataHub graph connection configured
- Schema information in OpenLineage events

#### Platform-Specific Handling

**Snowflake:** Use `convert_urns_to_lowercase: true` in namespace mapping

**BigQuery:** 3-tier naming (`project.dataset.table`). Set `database: project-id`, `schema: dataset-name`

**MySQL / 2-tier:** 2-tier naming (`schema.table`). Set `schema` only

**Postgres / Redshift:** 3-tier naming (`database.schema.table`). Set both `database` and `schema`

#### Container Hierarchy

Pipelines and their components are organized into a browsable container hierarchy that mirrors their
path in Matillion:

```
Project › Environment › <folder> › … › Pipeline › Component
```

The folder levels come from the pipeline's path (e.g. `ingest/staging/orders/load.orch.yaml`
yields `ingest › staging › orders` folders), so the browse tree lines up with the paths
you match on in `pipeline_patterns`. Components (DataJobs) live in their pipeline's folder and browse
directly under the pipeline.

The environment and folder levels are always built. `extract_projects_to_containers` (default `true`)
only controls whether the top-level **Project** container is added to the hierarchy; set it to `false`
to hang environments (and their folders) directly at the root instead of under a project.

#### Filtering Options

The connector supports flexible regex-based filtering to control what metadata is ingested.

##### Project Filtering

```yaml
project_patterns:
  allow: ["^prod-.*", "^staging-.*"]
  deny: [".*-deprecated$"]
```

##### Environment Filtering

```yaml
environment_patterns:
  allow: ["^production$", "^staging$"]
  deny: ["^sandbox.*"]
```

##### Pipeline Filtering

```yaml
pipeline_patterns:
  allow: [".*"]
  deny: ["^test_.*", ".*_backup$"]
```

##### Streaming Pipeline Filtering

```yaml
streaming_pipeline_patterns:
  allow: ["^cdc_.*"]
  deny: [".*_test$"]
```

All patterns are case-insensitive by default and support full regex syntax. Deny patterns take precedence over allow patterns.

#### Child Pipeline Dependencies

The connector automatically detects and tracks when pipelines call other pipelines (via "Run Pipeline" components). This creates step-level dependency relationships in DataHub, showing:

- Which pipeline steps trigger child pipelines
- Complete execution lineage across pipeline orchestrations
- Cross-pipeline data flow for comprehensive impact analysis

No configuration needed — this feature is automatic when execution history is ingested.

Child pipelines that only appear in lineage events (and were not discovered as project pipelines themselves) are, by default, created as their own DataFlows/DataJobs so the full dependency graph is captured. To suppress these lineage-only dependencies and keep ingestion scoped to discovered pipelines, disable:

```yaml
include_dependent_pipelines: false
```

Lineage is still emitted for discovered pipelines when this is disabled — only the lineage-only dependent pipelines are skipped.

#### Published vs Unpublished Pipelines

The connector can discover pipelines from two sources:

1. **Published Pipelines** — Pipelines explicitly published in Matillion DPC (fetched from `/published-pipelines` API)
2. **Unpublished Pipelines** — Pipelines discovered from recent execution history (fetched from `/pipeline-executions` API)

By default, both types are ingested. To only ingest published pipelines:

```yaml
include_unpublished_pipelines: false
```

This is useful when:

- You want to control what appears in DataHub via Matillion's publish workflow
- You have many development/test pipelines that run but shouldn't be documented
- You want to reduce ingestion time and API calls

#### Run History (DataProcessInstances)

Run history — per-execution `DataProcessInstance` entities with status and timing — is produced from the pipeline-executions API. Each execution surfaces a run on the **pipeline** (DataFlow) itself as well as on each of its **components** (DataJobs), so the "Runs" tab is populated at both levels. When `include_unpublished_pipelines: true`, this happens automatically as part of discovery.

When `include_unpublished_pipelines: false`, discovery only lists published pipelines and does **not** fetch executions, so no runs are emitted by default. To get run history for your published pipelines without also ingesting unpublished ones, enable:

```yaml
include_unpublished_pipelines: false
extract_run_history: true
```

This fetches executions within the configured time window and attaches runs to the matching published pipelines. It is off by default because it calls the pipeline-executions and per-execution steps APIs, which are slower and degrade over wider time windows — pair it with a narrow `start_time` / `end_time` and stateful ingestion.

Enabling it has a second benefit: lineage often references **unpublished child orchestrations** (e.g. `SRC_*_ORCH` pipelines invoked by a published schedule). The OpenLineage namespace only carries an opaque environment UUID, so when such a pipeline is neither published nor seen in executions, its environment cannot be resolved and the connector **skips** it rather than placing it in a pipeline with no environment. Because executions report the environment name, enabling `extract_run_history` (or `include_unpublished_pipelines`) lets these child orchestrations resolve their environment and nest correctly under it.

#### Time Window and Incremental Ingestion

Pipeline-execution discovery and lineage are bounded by `start_time` / `end_time`. If you do not set them, `end_time` defaults to now and `start_time` defaults to the start (00:00 UTC) of the previous day — i.e. at least the last 24 hours of jobs. Set `start_time` to backfill more history on the first run, especially if your pipelines do not run daily:

```yaml
start_time: "2024-01-01T00:00:00Z" # absolute
# start_time: "-30d"                # or relative to end_time
```

Enable stateful ingestion so subsequent runs only fetch new lineage instead of re-reading the whole window:

```yaml
stateful_ingestion:
  enabled: true
```

**Lineage endpoint performance**: the Matillion lineage events API paginates by offset and gets progressively slower the further back it reads. Wide time windows therefore both increase total runtime and make individual requests more likely to time out. Lineage requests are automatically split into sub-windows of at most 31 days (the API's hard limit per request), but each sub-window still pages through its full result set. Prefer a narrower window plus stateful ingestion over a single very large backfill, and only raise `api_config.request_timeout_sec` when a genuinely large window is unavoidable (a high timeout multiplies the worst-case wait, since failed requests are retried).

### Limitations

- SQL parsing for column-level lineage requires a DataHub graph connection and schema information in OpenLineage events. Unsupported SQL dialects or complex queries are skipped with a warning.
- Column-level lineage is only available when Matillion pipelines emit SQL via OpenLineage; transformations without SQL output will have coarse-grained lineage only.
- Matillion console links (project, run, and pipeline) are **off by default**; set `include_external_urls: true` to emit them. Pipeline names and the pipeline link use the pipeline **file name** — the only name the API exposes; a different display name set inside the Maia editor is not retrievable. The pipeline link opens the observability dashboard pre-filtered by that file name (there is no per-pipeline deep-link), so the pipeline must have run within the dashboard's time window to appear.

### Troubleshooting

#### Lineage Not Showing Up

1. Verify namespace mapping matches source ingestion platform instances
2. Check logs for `Processing OpenLineage event` messages
3. Confirm dataset names in OpenLineage match actual tables

#### Column-Level Lineage Missing

Enable `parse_sql_for_lineage: true` (requires DataHub graph connection).

#### Execution History Not Appearing

1. Adjust `start_time` to query further back in time if needed
2. Verify API permissions for Pipeline Executions API

#### Performance Issues or Request Timeouts

The lineage events endpoint paginates by offset and slows down the further back in time it reads, so wide windows are the most common cause of slow runs and `request_timeout_sec` timeouts. In order of preference:

1. Narrow the time window by adjusting `start_time` (e.g., last 7 days instead of 30) and enable `stateful_ingestion` so later runs stay incremental.
2. Use filtering patterns to reduce scope:
   - `project_patterns` to filter projects
   - `environment_patterns` to filter environments
   - `pipeline_patterns` to filter pipelines
   - `streaming_pipeline_patterns` to filter streaming pipelines
3. Disable `include_streaming_pipelines` if not needed.
4. As a last resort, raise `api_config.request_timeout_sec`. Keep it as low as practical — failed requests are retried, so a very high timeout multiplies the worst-case wait on a slow endpoint.


### Code Coordinates
- Class Name: `datahub.ingestion.source.matillion_dpc.matillion.MatillionSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/matillion_dpc/matillion.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Matillion, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
