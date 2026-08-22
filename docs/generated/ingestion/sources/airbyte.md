


# Airbyte

## Overview

[Airbyte](https://airbyte.com/) is an open-source data integration platform that syncs data from sources to destinations through configurable connections. It supports hundreds of pre-built connectors and lets you build custom ones.

This integration extracts metadata from Airbyte to give DataHub visibility into your data pipelines — including connections, sources, destinations, streams, and job execution history. It captures lineage between source and destination datasets at both the table and column level.

## Concept Mapping

Here's a table for **Concept Mapping** between Airbyte and DataHub to provide a clear overview of how entities and concepts in Airbyte are mapped to corresponding entities in DataHub:

| Source Concept     | DataHub Concept       | Notes                                                                  |
| ------------------ | --------------------- | ---------------------------------------------------------------------- |
| **Workspace**      | `DataFlow`            | Top-level container for Airbyte resources                              |
| **Connection**     | `DataFlow`            | Represents an Airbyte connection between source and destination        |
| **Source**         | `Dataset`             | Source datasets are mapped to DataHub datasets                         |
| **Destination**    | `Dataset`             | Destination datasets are mapped to DataHub datasets                    |
| **Stream**         | `DataJob`             | Each stream is represented as a DataJob within the Connection DataFlow |
| **Connection Job** | `DataProcessInstance` | Execution information for a connection run                             |
| **Source Schema**  | `SchemaMetadata`      | Schema information from source datasets                                |
| **Column Mapping** | `FineGrainedLineage`  | Column-level lineage between source and destination                    |


## Module `airbyte`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Column-level Lineage | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default when stateful ingestion is turned on. |
| Extract Tags | ✅ | Requires recipe configuration. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |

### Overview

This integration extracts metadata from Airbyte's API to capture information about your connections, sources, destinations, and the lineage between them.

### Prerequisites

You'll need to have an Airbyte instance running with configured sources and destinations, and access to the Airbyte API.

#### Steps to Get the Required Information

1. **Determine Your Deployment Type**:

   - **Open Source (OSS)**: If you're running a self-hosted Airbyte instance
   - **Cloud**: If you're using Airbyte Cloud

2. **Authentication Credentials**:

   - **For Open Source (OSS)**:

     - The URL of your Airbyte instance (host and port)
     - **OAuth2 client credentials** (Airbyte 1.0+) - obtain via:
       - UI: Navigate to **User > User settings > Applications** to create an application and copy credentials
       - CLI: Run `abctl local credentials` (abctl v0.11.0+)
     - Username and password if basic authentication is enabled
     - API token if available

   - **For Airbyte Cloud**:
     - OAuth2 client ID and client secret (required)
     - OAuth2 refresh token (optional — omit to use `client_credentials` grant; provide to use `refresh_token` grant)
     - Your Airbyte Cloud workspace ID

3. **API Access**:

   - For OSS users, ensure the API is accessible at `/api/public/v1` path prefix
   - Verify connectivity by testing the health endpoint: `http://localhost:8000/api/public/v1/health`
   - Ensure you have proper network connectivity between your DataHub instance and the Airbyte API

4. **Permissions**:
   - The authentication credentials should have permissions to:
     - Read workspace information
     - List and read sources, destinations, and connections
     - Access connection schemas and sync catalogs
     - View job execution history (if extracting job statuses)

#### Airbyte Version for Stream Namespaces

Airbyte's Public API returns a connection's stream list without the schema (namespace) each
stream is read from. DataHub recovers it from the `/streams` endpoint. Two things depend on it:

- **Dataset URNs.** Without a namespace, every stream falls back to the source connector's
  configured default schema, so streams living in other schemas resolve to the wrong table.
  Namespaces are only reported from **Airbyte 1.7.0 onwards** — older deployments answer
  `/streams` without any namespace field, and the run warns that namespaces were not reported.
  Set `default_schema` per source under `sources_to_platform_instance` to supply one manually.
- **Column-level lineage**, which is built from the field list `/streams` returns. This works on
  older versions too, since the field list does not depend on namespace support.


### Install the Plugin
```shell
pip install 'acryl-datahub[airbyte]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: airbyte
  config:
    # Deployment type - required
    deployment_type: oss               # Options: "oss" (self-hosted) or "cloud" (Airbyte Cloud)

    # Connection details for OSS deployment
    host_port: http://localhost:8000   # Airbyte API endpoint URL

    # Authentication for OSS deployment
    username: your_username            # Username for basic auth
    password: your_password            # Password for basic auth
    # api_key: your_api_key            # Alternative: API token if available

    # Authentication for Cloud deployment - uncomment if using Airbyte Cloud
    #deployment_type: cloud
    #oauth2_client_id: your_client_id           # OAuth2 client ID for Airbyte Cloud
    #oauth2_client_secret: your_client_secret   # OAuth2 client secret
    #oauth2_refresh_token: your_refresh_token   # OAuth2 refresh token
    #cloud_workspace_id: your_workspace_id      # Airbyte Cloud workspace ID

    # SSL configuration
    verify_ssl: false                  # Whether to verify SSL certificates
    #ssl_ca_cert: /path/to/cert.pem    # Path to CA certificate file (optional)

    # Data extraction options
    extract_column_level_lineage: true # Extract column-level lineage information
    include_statuses: true             # Include connection job statuses
    job_statuses_limit: 100            # Max number of job statuses to retrieve
    
    # Lineage emission mode
    incremental_lineage: true          # Emit lineage as patch (incremental) rather than full replacement
                                       # Set to false to re-state all lineage on each run

    # Optional: Extract tags
    extract_tags: false                # Extract tags from Airbyte metadata

    # Filtering options - uncomment to use
    #workspace_pattern:
    #  allow:
    #    - ".*"                        # Pattern to filter workspaces

    #connection_pattern:
    #  allow:
    #    - ".*"                        # Pattern to filter connections

    #source_pattern:
    #  allow:
    #    - ".*MySQL.*"                 # Pattern to filter sources

    #destination_pattern:
    #  allow:
    #    - ".*Postgres.*"              # Pattern to filter destinations

    # Platform instance configuration
    platform_instance: airbyte-instance # Custom platform instance name

    # Performance settings
    request_timeout: 30                # Timeout for API requests in seconds
    max_retries: 3                     # Max retries for failed requests
    retry_backoff_factor: 0.5          # Backoff factor for retries
    page_size: 20                      # Items per page in API requests

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | API key or Personal Access Token for authentication (OSS deployment) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">cloud_api_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Base URL for Airbyte Cloud API (defaults to production URL) <div className="default-line default-line-with-docs">Default: <span className="default-value">https://api.airbyte.com/v1</span></div> |
| <div className="path-line"><span className="path-main">cloud_oauth_token_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | OAuth token URL for Airbyte Cloud (defaults to production URL) <div className="default-line default-line-with-docs">Default: <span className="default-value">https://auth.airbyte.com/oauth/token</span></div> |
| <div className="path-line"><span className="path-main">cloud_workspace_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Workspace ID for Airbyte Cloud (required for cloud deployment) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">deployment_type</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "oss", "cloud"  |
| <div className="path-line"><span className="path-main">extra_headers</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Additional HTTP headers to send with each request <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract column-level lineage <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract tags from Airbyte metadata <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">host_port</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Airbyte API host and port (e.g., http://localhost:8000) - required for OSS deployment <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">include_inactive_connections</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Also ingest connections that are disabled in Airbyte. By default, connections reported as inactive are skipped with no warning, even if they still appear healthy in the Airbyte UI. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_statuses</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest run statuses <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">job_status_end_date</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | End date for job status retrieval (format: yyyy-mm-ddTHH:MM:SSZ). Default is current time. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">job_status_start_date</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Start date for job status retrieval (format: yyyy-mm-ddTHH:MM:SSZ). Default is 7 days ago. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">job_statuses_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of job statuses to retrieve per connection <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of retries for failed API requests <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">oauth2_client_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | OAuth2 client ID for OSS (Airbyte 1.0+) and Cloud deployments <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">oauth2_client_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | OAuth2 client secret for OSS (Airbyte 1.0+) and Cloud deployments <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">oauth2_refresh_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | OAuth2 refresh token (Cloud only). If provided, uses refresh_token grant; otherwise uses client_credentials <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of items to fetch per page in API requests <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Password for basic authentication (OSS deployment) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">request_timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Timeout for API requests in seconds <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">retry_backoff_factor</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Backoff factor for retries (wait time is {factor} * (2 ^ retry_number)) <div className="default-line default-line-with-docs">Default: <span className="default-value">0.5</span></div> |
| <div className="path-line"><span className="path-main">source_type_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">ssl_ca_cert</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to CA certificate file (.pem) for SSL verification <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Username for basic authentication (OSS deployment) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">verify_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to verify SSL certificates <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">connection_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">connection_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">destination_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">destination_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">destinations_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">destinations_to_platform_instance.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Override the platform type detection (e.g., 'postgres', 'mysql') <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destinations_to_platform_instance.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. Recommended for case-insensitive platforms to ensure lineage compatibility. Note: For Snowflake destinations, this also lowercases column names in lineage to match DataHub's native Snowflake connector behavior. For other platforms (MSSQL, Postgres, BigQuery, etc.), only dataset names are lowercased, not column names. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">destinations_to_platform_instance.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Schema to use when no per-stream schema is known (e.g. 'dbo' for SQL Server). Airbyte only exposes per-stream namespaces from version 1.7.0 onwards, so on older deployments this is the only way to get a schema tier into the dataset URN. Only a namespace reported by Airbyte, or a per-table schema in the connector configuration, takes precedence; this deliberately outranks the connector-wide schema key, which sometimes holds a database name rather than a schema. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destinations_to_platform_instance.`key`.</span><span className="path-main">include_schema_in_urn</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Include schema in the dataset URN when database is present. If None (default), automatically detects 2-tier vs 3-tier platforms by checking if schema equals database. Set to True to force 3-tier (database.schema.table), or False to force 2-tier (database.table). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destinations_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destinations_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Environment to use for dataset URNs (e.g., PROD, DEV, STAGING) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">source_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">source_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">sources_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Override the platform type detection (e.g., 'postgres', 'mysql') <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. Recommended for case-insensitive platforms to ensure lineage compatibility. Note: For Snowflake destinations, this also lowercases column names in lineage to match DataHub's native Snowflake connector behavior. For other platforms (MSSQL, Postgres, BigQuery, etc.), only dataset names are lowercased, not column names. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Schema to use when no per-stream schema is known (e.g. 'dbo' for SQL Server). Airbyte only exposes per-stream namespaces from version 1.7.0 onwards, so on older deployments this is the only way to get a schema tier into the dataset URN. Only a namespace reported by Airbyte, or a per-table schema in the connector configuration, takes precedence; this deliberately outranks the connector-wide schema key, which sometimes holds a database name rather than a schema. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">include_schema_in_urn</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Include schema in the dataset URN when database is present. If None (default), automatically detects 2-tier vs 3-tier platforms by checking if schema equals database. Set to True to force 3-tier (database.schema.table), or False to force 2-tier (database.table). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Environment to use for dataset URNs (e.g., PROD, DEV, STAGING) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">workspace_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">workspace_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AirbyteDeploymentType": {
      "enum": [
        "oss",
        "cloud"
      ],
      "title": "AirbyteDeploymentType",
      "type": "string"
    },
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes.\n\nPatterns are matched against the start of the string only, not the entire\nstring - a pattern does not need to match to the end to be considered a match.\nFor example, the pattern \"prod\" matches \"prod\", \"prod_east\", and \"production\".\nTo require an exact match, anchor your pattern explicitly, e.g. \"^prod$\".",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
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
    "PlatformDetail": {
      "additionalProperties": false,
      "properties": {
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Override the platform type detection (e.g., 'postgres', 'mysql')",
          "title": "Platform"
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
          "description": "The instance of the platform that all assets belong to",
          "title": "Platform Instance"
        },
        "env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Environment to use for dataset URNs (e.g., PROD, DEV, STAGING)",
          "title": "Env"
        },
        "include_schema_in_urn": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Include schema in the dataset URN when database is present. If None (default), automatically detects 2-tier vs 3-tier platforms by checking if schema equals database. Set to True to force 3-tier (database.schema.table), or False to force 2-tier (database.table).",
          "title": "Include Schema In Urn"
        },
        "default_schema": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Schema to use when no per-stream schema is known (e.g. 'dbo' for SQL Server). Airbyte only exposes per-stream namespaces from version 1.7.0 onwards, so on older deployments this is the only way to get a schema tier into the dataset URN. Only a namespace reported by Airbyte, or a per-table schema in the connector configuration, takes precedence; this deliberately outranks the connector-wide schema key, which sometimes holds a database name rather than a schema.",
          "title": "Default Schema"
        },
        "convert_urns_to_lowercase": {
          "default": true,
          "description": "Whether to convert dataset urns to lowercase. Recommended for case-insensitive platforms to ensure lineage compatibility. Note: For Snowflake destinations, this also lowercases column names in lineage to match DataHub's native Snowflake connector behavior. For other platforms (MSSQL, Postgres, BigQuery, etc.), only dataset names are lowercased, not column names.",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "PlatformDetail",
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
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
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
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
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
      "default": null
    },
    "deployment_type": {
      "$ref": "#/$defs/AirbyteDeploymentType",
      "default": "oss",
      "description": "Type of Airbyte deployment ('oss' or 'cloud')"
    },
    "host_port": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Airbyte API host and port (e.g., http://localhost:8000) - required for OSS deployment",
      "title": "Host Port"
    },
    "username": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Username for basic authentication (OSS deployment)",
      "title": "Username"
    },
    "password": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Password for basic authentication (OSS deployment)",
      "title": "Password"
    },
    "api_key": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "API key or Personal Access Token for authentication (OSS deployment)",
      "title": "Api Key"
    },
    "oauth2_client_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "OAuth2 client ID for OSS (Airbyte 1.0+) and Cloud deployments",
      "title": "Oauth2 Client Id"
    },
    "oauth2_client_secret": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "OAuth2 client secret for OSS (Airbyte 1.0+) and Cloud deployments",
      "title": "Oauth2 Client Secret"
    },
    "oauth2_refresh_token": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "OAuth2 refresh token (Cloud only). If provided, uses refresh_token grant; otherwise uses client_credentials",
      "title": "Oauth2 Refresh Token"
    },
    "verify_ssl": {
      "default": true,
      "description": "Whether to verify SSL certificates",
      "title": "Verify Ssl",
      "type": "boolean"
    },
    "ssl_ca_cert": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to CA certificate file (.pem) for SSL verification",
      "title": "Ssl Ca Cert"
    },
    "extra_headers": {
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
      "description": "Additional HTTP headers to send with each request",
      "title": "Extra Headers"
    },
    "request_timeout": {
      "default": 30,
      "description": "Timeout for API requests in seconds",
      "title": "Request Timeout",
      "type": "integer"
    },
    "max_retries": {
      "default": 3,
      "description": "Maximum number of retries for failed API requests",
      "title": "Max Retries",
      "type": "integer"
    },
    "retry_backoff_factor": {
      "default": 0.5,
      "description": "Backoff factor for retries (wait time is {factor} * (2 ^ retry_number))",
      "title": "Retry Backoff Factor",
      "type": "number"
    },
    "page_size": {
      "default": 20,
      "description": "Number of items to fetch per page in API requests",
      "title": "Page Size",
      "type": "integer"
    },
    "cloud_workspace_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Workspace ID for Airbyte Cloud (required for cloud deployment)",
      "title": "Cloud Workspace Id"
    },
    "cloud_api_url": {
      "default": "https://api.airbyte.com/v1",
      "description": "Base URL for Airbyte Cloud API (defaults to production URL)",
      "title": "Cloud Api Url",
      "type": "string"
    },
    "cloud_oauth_token_url": {
      "default": "https://auth.airbyte.com/oauth/token",
      "description": "OAuth token URL for Airbyte Cloud (defaults to production URL)",
      "title": "Cloud Oauth Token Url",
      "type": "string"
    },
    "extract_column_level_lineage": {
      "default": true,
      "description": "Extract column-level lineage",
      "title": "Extract Column Level Lineage",
      "type": "boolean"
    },
    "workspace_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter workspaces. Use the pattern format as in other DataHub sources."
    },
    "connection_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter connections. Use the pattern format as in other DataHub sources."
    },
    "source_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter sources. Use the pattern format as in other DataHub sources."
    },
    "destination_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter destinations. Use the pattern format as in other DataHub sources."
    },
    "source_type_mapping": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Mapping from Airbyte sourceType/destinationType to DataHub platform names. Use this to normalize Airbyte's source types to DataHub platform names. Example: {'PostgreSQL': 'postgres', 'MySQL': 'mysql'}. If not specified, the sourceType/destinationType from Airbyte is sanitized and used directly.",
      "title": "Source Type Mapping",
      "type": "object"
    },
    "sources_to_platform_instance": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "description": "A mapping from Airbyte source ID to its platform/instance/env/database details. Use this to override platform details for specific sources. Example: {'11111111-1111-1111-1111-111111111111': {'platform': 'postgres', 'platform_instance': 'prod-postgres', 'env': 'PROD'}}",
      "title": "Sources To Platform Instance",
      "type": "object"
    },
    "destinations_to_platform_instance": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "description": "A mapping from Airbyte destination ID to its platform/instance/env/database details. Use this to override platform details for specific destinations.",
      "title": "Destinations To Platform Instance",
      "type": "object"
    },
    "include_inactive_connections": {
      "default": false,
      "description": "Also ingest connections that are disabled in Airbyte. By default, connections reported as inactive are skipped with no warning, even if they still appear healthy in the Airbyte UI.",
      "title": "Include Inactive Connections",
      "type": "boolean"
    },
    "include_statuses": {
      "default": true,
      "description": "Whether to ingest run statuses",
      "title": "Include Statuses",
      "type": "boolean"
    },
    "job_status_start_date": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Start date for job status retrieval (format: yyyy-mm-ddTHH:MM:SSZ). Default is 7 days ago.",
      "title": "Job Status Start Date"
    },
    "job_status_end_date": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "End date for job status retrieval (format: yyyy-mm-ddTHH:MM:SSZ). Default is current time.",
      "title": "Job Status End Date"
    },
    "job_statuses_limit": {
      "default": 100,
      "description": "Maximum number of job statuses to retrieve per connection",
      "title": "Job Statuses Limit",
      "type": "integer"
    },
    "extract_tags": {
      "default": false,
      "description": "Extract tags from Airbyte metadata",
      "title": "Extract Tags",
      "type": "boolean"
    }
  },
  "title": "AirbyteSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Lineage

Column-level lineage is extracted from Airbyte's sync catalog when field mapping information is available in the connection configuration. Table-level lineage is always captured between source and destination datasets.

#### Job History

Connection job execution history is ingested as `DataProcessInstance` entities, capturing run status, start time, and duration for each sync job.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by Airbyte.

- Schema information is only available for sources that expose a sync catalog. Sources without schema discovery will produce datasets without schema metadata.
- Column-level lineage requires field mapping to be configured in the Airbyte connection.
- Job history depth is limited by the Airbyte API's pagination and retention settings.
- The Airbyte Public API only supports `limit` + `offset` pagination on list endpoints; cursor pagination is not exposed. Ingestion runs against an actively-mutating Airbyte instance may therefore skip or double-count entries inserted or deleted mid-scan. Schedule ingestion during quiet periods if exactness is required.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Missing Connections

By default only enabled Airbyte connections are ingested. Disabled connections are
skipped with no warning. If a connection is missing from DataHub, check whether it
is disabled in Airbyte (or whether its Public API `status` is `"inactive"`). To
ingest disabled connections as well, set `include_inactive_connections: true`.

A successful `GET /api/public/v1/connections/{connectionId}` does not mean DataHub
will ingest that connection. Ingestion only walks connections returned by
`GET /api/public/v1/connections?workspaceId=...` for each workspace allowed by
`workspace_pattern`. Confirm the connection's `workspaceId` matches the recipe,
that `connection_pattern` / `source_pattern` / `destination_pattern` allow it
(those skips are silent), and that it appears in the list response — not only in
a by-id GET. Offset pagination can also skip entries on a mutating Airbyte
instance; see Limitations above.

#### Authentication Errors

Verify that your OAuth2 client credentials are correct and have not expired. For OSS deployments, confirm the API is reachable at the `/api/public/v1` path prefix.

#### Missing or Ambiguous Stream Namespaces

A `Stream Metadata Unavailable` warning means the `/streams` endpoint could not be read (404,
5xx after retries, or a connection error), so stream namespaces and column-level lineage are
skipped for that source. The connection itself is still ingested. For streams without a
namespace already present in the connection catalog, a per-table schema in the connector
config, or a configured `default_schema`, dataset lineage is also skipped — emitting URNs
from the connector-wide schema key during a transient `/streams` blip would leave phantom
datasets and edges that stale-entity removal never reconciles. The warning's context carries
the HTTP status when Airbyte returned one, or notes a network/connection error when there was
no status. On versions that expose `/streams`, a 404 usually means the source is not
accessible to the credentials in the recipe, and a 5xx means Airbyte failed while describing
the source.

A `Stream Namespaces Not Reported` warning means `/streams` described the source's streams but
gave no namespace for any of them, and nothing else supplied a schema for the streams it names,
so their dataset URNs carry no schema tier. Airbyte only reports stream namespaces from 1.7.0
onwards. Either upgrade Airbyte, or set the schema for that source in the recipe:

```yaml
sources_to_platform_instance:
  <airbyte-source-id>:
    platform: mssql
    default_schema: dbo
```

`default_schema` is only overridden by a namespace reported by Airbyte or a per-table schema in the
connector's own configuration, so it is safe to leave in place after an upgrade — and once it
applies, the warning stops for the streams it covers. It deliberately outranks the connector-wide
schema key, which sometimes holds a database name rather than a schema.

A `Stream Namespace Missing` warning names streams that Airbyte left without a namespace on a
source where it reported one for others, so the Airbyte version is not the cause. Set the schema
for those tables in the Airbyte connector's own configuration, or `default_schema` as above if
every stream on that source shares one schema.

A `Stream Schema Guessed` warning means the same thing happened on a source that replicates
several schemas. One name cannot be right for all of them, so every stream gets the same schema
tier and the streams living elsewhere point at another table's URN. `default_schema` cannot fix
this — only Airbyte knows which stream came from which schema, and it only says so from 1.7.0
onwards, so upgrading is the resolution. Ignore the warning if every stream really does live in
the one schema named in the warning's context.

An `Ambiguous Stream Namespace` warning means several schemas expose a stream with the same
name and Airbyte does not say which configured stream belongs to which schema. DataHub leaves
the namespace unset rather than guessing. Set the schema explicitly on the affected connection
in Airbyte to resolve it.

#### Missing Schema Metadata

If datasets are ingested without schema information, confirm that the Airbyte source supports schema discovery and that the sync catalog is populated in the connection settings.


### Code Coordinates
- Class Name: `datahub.ingestion.source.airbyte.source.AirbyteSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/airbyte/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Airbyte, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
