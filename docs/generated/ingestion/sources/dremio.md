


# Dremio

## Overview

Dremio is a DataHub utility or metadata-focused integration.

The DataHub integration for Dremio covers metadata entities and operational objects relevant to this connector. It also captures table- and column-level lineage, usage statistics, data profiling, ownership, and stateful deletion detection.

## Concept Mapping

| Source Concept             | DataHub Concept | Notes                                                      |
| -------------------------- | --------------- | ---------------------------------------------------------- |
| **Physical Dataset/Table** | `Dataset`       | Subtype: `Table`                                           |
| **Virtual Dataset/Views**  | `Dataset`       | Subtype: `View`                                            |
| **Spaces**                 | `Container`     | Mapped to DataHub’s `Container` aspect. Subtype: `Space`   |
| **Folders**                | `Container`     | Mapped as a `Container` in DataHub. Subtype: `Folder`      |
| **Sources**                | `Container`     | Represented as a `Container` in DataHub. Subtype: `Source` |


## Module `dremio`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Dremio Space, Dremio Source. |
| Column-level Lineage | ✅ | Extract column-level lineage. Supported for types - Table. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration. |
| Dataset Usage | ✅ | Enabled by default to get usage stats. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| Extract Ownership | ✅ | Enabled by default. |
| [Operation Capture](../../../api/tutorials/operations.md) | ✅ | Optionally enabled via `include_query_lineage`; generated from Dremio job history. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. Supported for types - Table. |

### Overview

Dremio is a data lakehouse platform that provides SQL query capabilities across diverse data sources without copying data. The DataHub integration connects directly to Dremio via REST API and system table queries to discover and catalog your data assets.

This module captures physical datasets (tables), virtual datasets (views), spaces, folders, and data sources as containers. Optional capabilities include column-level lineage from view definitions, query-based lineage from job history, data profiling, and stateful deletion of removed entities.

### Prerequisites

You need a running Dremio instance with API access, a user account with appropriate permissions, and network connectivity between DataHub and Dremio.

#### Authentication

Generate a Personal Access Token for programmatic access:

- Log in to your Dremio instance
- Navigate to your user profile (top-right corner)
- Select **Generate API Token** to create a Personal Access Token
- Save the token securely — it is only displayed once

#### Required Permissions

Your user account needs the following Dremio privileges:

**Container Access (Required for container discovery)**

```sql
-- Grant access to view and use sources and spaces
GRANT READ METADATA ON SOURCE <source_name> TO USER <username>
GRANT READ METADATA ON SPACE <space_name> TO USER <username>

-- Or grant at system level for all containers
GRANT READ METADATA ON SYSTEM TO USER <username>
```

**System Tables Access (Required for metadata extraction)**

```sql
-- Grant access to system tables for dataset metadata
GRANT SELECT ON SYSTEM TO USER <username>
```

**Dataset Access (Optional — only needed for data profiling)**

```sql
-- For data profiling (only if profiling is enabled)
GRANT SELECT ON SOURCE <source_name> TO USER <username>
GRANT SELECT ON SPACE <space_name> TO USER <username>
```

**Query Lineage Access (Optional — only if `include_query_lineage: true`)**

```sql
-- Required for query lineage extraction
GRANT VIEW JOB HISTORY ON SYSTEM TO USER <username>
```

What each permission does:

- `READ METADATA`: Access to view containers and their metadata via API calls
- `SELECT ON SYSTEM`: Access to system tables (`SYS.*`, `INFORMATION_SCHEMA.*`) for dataset metadata
- `SELECT ON SOURCE/SPACE`: Read actual data for profiling (optional)
- `VIEW JOB HISTORY`: Access query history tables for lineage (optional)

#### External Data Source Verification

If your Dremio instance connects to external data sources (AWS S3, databases, etc.), ensure that:

- Dremio has proper credentials configured for those sources
- The DataHub user can access datasets from those external sources
- Network connectivity exists between Dremio and external sources


### Install the Plugin
```shell
pip install 'acryl-datahub[dremio]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: dremio
  config:
    # Coordinates
    hostname: localhost
    port: 9047
    tls: true

    # Credentials — Personal Access Token (recommended)
    # The user must have READ METADATA and SELECT ON SYSTEM privileges.
    authentication_method: PAT
    password: pass
    # OR basic auth:
    # authentication_method: password
    # username: user
    # password: pass

    # For Dremio Cloud instances
    # is_dremio_cloud: True
    # dremio_cloud_project_id: <project_id>

    # Extract ownership information
    ingest_owner: true

    # --- Lineage ---

    # Enable query-based lineage (reads SYS.JOBS_RECENT)
    include_query_lineage: true

    # Emit lineage as PATCH rather than full-replace, preserving any
    # lineage edges added manually in the DataHub UI between runs.
    # Defaults to false; set true if your GMS supports patch aspects.
    incremental_lineage: false

    # --- Incremental properties ---

    # Emit dataset properties as PATCH rather than full-replace, so
    # descriptions and tags set in the DataHub UI are not overwritten on
    # subsequent runs. Particularly useful when Dremio acts as a semantic
    # layer on top of raw sources that are re-documented in DataHub.
    incremental_properties: false

    # --- Stateful ingestion (checkpointing) ---

    # Stateful ingestion records a checkpoint after each run so that
    # subsequent runs can skip already-processed data and remove stale entities.
    stateful_ingestion:
      enabled: true

    # Advance the query lineage extraction window to start from the end of
    # the previous run, avoiding re-processing job history already ingested.
    # Requires stateful_ingestion to be configured.
    enable_stateful_time_window: false

    # --- Profiling ---

    profiling:
      enabled: false
      # Skip re-profiling tables that DataHub profiled within this many days.
      # Dremio has no table modification timestamps, so this is compared against
      # the last-profiled time rather than last-modified time.
      # Requires stateful_ingestion to be enabled.
      # profile_if_updated_since_days: 1

    # --- Filtering ---

    # Optional: map Dremio sources to external platforms for cross-source lineage
    source_mappings:
      - platform: s3
        source_name: samples

    # Optional: register Dremio source types not yet in the built-in map
    # (e.g. Dremio ARP connectors). Scoped to this recipe.
    # source_type_mappings:
    #   MYNEWCONNECTOR:        # Dremio source type (case-insensitive)
    #     platform: myplatform # DataHub platform name to emit
    #     category: database   # optional: "database" or "file_object_storage"

    # Optional: restrict ingestion to specific schemas or datasets
    schema_pattern:
      allow:
        - "<source_name>.*"
    dataset_pattern:
      allow:
        - "<source_name>.<schema>.*"

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">authentication_method</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Authentication method: 'password' or 'PAT' (Personal Access Token) <div className="default-line default-line-with-docs">Default: <span className="default-value">PAT</span></div> |
| <div className="path-line"><span className="path-main">batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of rows to fetch per page when reading Dremio's system tables (datasets, columns, view definitions, and queries) via LIMIT/OFFSET. Dremio's system tables can be slow, so a larger batch reduces the number of round-trips. Set to 0 to fetch as much as possible per page. Values are clamped to a safety ceiling of 1,000,000 rows per page to keep pagination reliable. A larger batch also raises the amount Dremio must materialize per job, so lower this value if Dremio runs out of memory during extraction. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-main">disable_certificate_verification</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Disable TLS certificate verification <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Domain for all source objects. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">dremio_cloud_project_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | ID of Dremio Cloud Project. Found in Project Settings in the Dremio Cloud UI <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">dremio_cloud_region</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "US", "EU" <div className="default-line default-line-with-docs">Default: <span className="default-value">US</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_time_window</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful time window tracking for query lineage/usage extraction. When enabled, subsequent runs will skip time windows already fully processed, avoiding redundant API calls. Requires stateful_ingestion to be configured. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-main">hostname</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Hostname or IP Address of the Dremio server <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">include_query_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to include query-based lineage information. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_system_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to include system tables and schemas (INFORMATION_SCHEMA, SYS) in ingestion.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, lineage aspects are emitted as PATCH operations rather than full overwrites. This preserves any lineage edges that were manually added in DataHub between runs. Disable if you want each run to fully replace lineage. PATCH emission requires a GMS that supports patch aspects. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">incremental_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits dataset properties as incremental to existing dataset properties in DataHub. When disabled, re-states dataset properties on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_owner</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest Owner from source. This will override Owner info entered from UI <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">is_dremio_cloud</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether this is a Dremio Cloud instance <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use for parallel processing <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Dremio password or Personal Access Token <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">path_to_certificates</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to SSL certificates <div className="default-line default-line-with-docs">Default: <span className="default-value">/home/runner/work/datahub/datahub/metadata-ingesti...</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">port</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Port of the Dremio REST API <div className="default-line default-line-with-docs">Default: <span className="default-value">9047</span></div> |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tls</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether the Dremio REST API port is encrypted <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Dremio username <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">dataset_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dataset_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">source_mappings</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Mappings from Dremio sources to DataHub platforms and datasets. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">source_mappings.</span><span className="path-main">DremioSourceMapping</span></div> <div className="type-name-line"><span className="type-name">DremioSourceMapping</span></div> |   |
| <div className="path-line"><span className="path-prefix">source_mappings.DremioSourceMapping.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if DremioSourceMapping is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Source connection made by Dremio (e.g. S3, Snowflake)  |
| <div className="path-line"><span className="path-prefix">source_mappings.DremioSourceMapping.</span><span className="path-main">source_name</span>&nbsp;<abbr title="Required if DremioSourceMapping is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Alias of platform in Dremio connection  |
| <div className="path-line"><span className="path-prefix">source_mappings.DremioSourceMapping.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">source_mappings.DremioSourceMapping.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">source_type_mappings</span></div> <div className="type-name-line"><span className="type-name">map(str,DremioSourceTypeOverride)</span></div> |   |
| <div className="path-line"><span className="path-prefix">source_type_mappings.`key`.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if source_type_mappings is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | DataHub platform name to emit for this Dremio source type (e.g. `kafka`, `iceberg`, `snowflake`).  |
| <div className="path-line"><span className="path-prefix">source_type_mappings.`key`.</span><span className="path-main">category</span></div> <div className="type-name-line"><span className="type-name">One of Enum, null</span></div> | Whether the Dremio source uses dot-notation database/schema paths (`database`) or slash-notation file paths (`file_object_storage`). Defaults to `unknown`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">ProfileConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">catch_exceptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">field_sample_values_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Upper limit for number of sample values to collect for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of distinct values for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_value_frequencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for distinct value frequencies. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_histogram</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the histogram for numeric fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_mean_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the mean value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_quantiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the quantiles of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_sample_values</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the sample values for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_stddev_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the standard deviation of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Max number of documents to profile. By default, profiles all documents. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_number_of_fields_to_profile</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">method</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "ge", "sqlalchemy" <div className="default-line default-line-with-docs">Default: <span className="default-value">sqlalchemy</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">nested_field_max_depth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch). <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Offset in documents to profile. By default, uses no offset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_datetime</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), null</span></div> | If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_external_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile external tables. Only Snowflake and Redshift supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_if_updated_since_days</span></div> <div className="type-name-line"><span className="type-name">One of number, null</span></div> | Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_nested_fields</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile complex types like structs, arrays and maps.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_count_estimate_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">5000000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_size_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_combiner_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | *This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Time before cancelling Dremio profiling query <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">report_dropped_profiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">turn_off_expensive_profiling_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">use_sampling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">tags_to_ignore_sampling</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.tags_to_ignore_sampling.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion config with stale-entity removal support. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "DremioSourceMapping": {
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
          "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "The environment that all assets produced by this connector belong to",
          "title": "Env",
          "type": "string"
        },
        "platform": {
          "description": "Source connection made by Dremio (e.g. S3, Snowflake)",
          "title": "Platform",
          "type": "string"
        },
        "source_name": {
          "description": "Alias of platform in Dremio connection",
          "title": "Source Name",
          "type": "string"
        }
      },
      "required": [
        "platform",
        "source_name"
      ],
      "title": "DremioSourceMapping",
      "type": "object"
    },
    "DremioSourceTypeOverride": {
      "additionalProperties": false,
      "properties": {
        "platform": {
          "description": "DataHub platform name to emit for this Dremio source type (e.g. `kafka`, `iceberg`, `snowflake`).",
          "title": "Platform",
          "type": "string"
        },
        "category": {
          "anyOf": [
            {
              "enum": [
                "database",
                "file_object_storage"
              ],
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Whether the Dremio source uses dot-notation database/schema paths (`database`) or slash-notation file paths (`file_object_storage`). Defaults to `unknown`.",
          "title": "Category"
        }
      },
      "required": [
        "platform"
      ],
      "title": "DremioSourceTypeOverride",
      "type": "object"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
      "type": "object"
    },
    "ProfileConfig": {
      "additionalProperties": false,
      "properties": {
        "method": {
          "default": "sqlalchemy",
          "description": "Profiling method to use. `sqlalchemy` (default) runs profiling queries directly against your source's existing SQLAlchemy connection. `ge` selects the legacy Great Expectations profiler, which is deprecated and requires `pip install 'acryl-datahub[profiling-ge]'`.",
          "enum": [
            "ge",
            "sqlalchemy"
          ],
          "title": "Method",
          "type": "string"
        },
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        },
        "limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Max number of documents to profile. By default, profiles all documents.",
          "title": "Limit"
        },
        "offset": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Offset in documents to profile. By default, uses no offset.",
          "title": "Offset"
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only, or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "include_field_null_count": {
          "default": true,
          "description": "Whether to profile for the number of nulls for each column.",
          "title": "Include Field Null Count",
          "type": "boolean"
        },
        "include_field_distinct_count": {
          "default": true,
          "description": "Whether to profile for the number of distinct values for each column.",
          "title": "Include Field Distinct Count",
          "type": "boolean"
        },
        "include_field_min_value": {
          "default": true,
          "description": "Whether to profile for the min value of numeric columns.",
          "title": "Include Field Min Value",
          "type": "boolean"
        },
        "include_field_max_value": {
          "default": true,
          "description": "Whether to profile for the max value of numeric columns.",
          "title": "Include Field Max Value",
          "type": "boolean"
        },
        "include_field_mean_value": {
          "default": true,
          "description": "Whether to profile for the mean value of numeric columns.",
          "title": "Include Field Mean Value",
          "type": "boolean"
        },
        "include_field_stddev_value": {
          "default": true,
          "description": "Whether to profile for the standard deviation of numeric columns.",
          "title": "Include Field Stddev Value",
          "type": "boolean"
        },
        "include_field_quantiles": {
          "default": false,
          "description": "Whether to profile for the quantiles of numeric columns.",
          "title": "Include Field Quantiles",
          "type": "boolean"
        },
        "include_field_distinct_value_frequencies": {
          "default": false,
          "description": "Whether to profile for distinct value frequencies.",
          "title": "Include Field Distinct Value Frequencies",
          "type": "boolean"
        },
        "include_field_histogram": {
          "default": false,
          "description": "Whether to profile for the histogram for numeric fields.",
          "title": "Include Field Histogram",
          "type": "boolean"
        },
        "include_field_sample_values": {
          "default": true,
          "description": "Whether to profile for the sample values for all columns.",
          "title": "Include Field Sample Values",
          "type": "boolean"
        },
        "max_workers": {
          "default": 20,
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "report_dropped_profiles": {
          "default": false,
          "description": "Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes.",
          "title": "Report Dropped Profiles",
          "type": "boolean"
        },
        "turn_off_expensive_profiling_metrics": {
          "default": false,
          "description": "Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.",
          "title": "Turn Off Expensive Profiling Metrics",
          "type": "boolean"
        },
        "field_sample_values_limit": {
          "default": 20,
          "description": "Upper limit for number of sample values to collect for all columns.",
          "title": "Field Sample Values Limit",
          "type": "integer"
        },
        "max_number_of_fields_to_profile": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
          "title": "Max Number Of Fields To Profile"
        },
        "profile_if_updated_since_days": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "dremio"
            ]
          },
          "title": "Profile If Updated Since Days"
        },
        "profile_table_size_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5,
          "description": "Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "unity-catalog",
              "oracle",
              "teradata"
            ]
          },
          "title": "Profile Table Size Limit"
        },
        "profile_table_row_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5000000,
          "description": "Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "oracle"
            ]
          },
          "title": "Profile Table Row Limit"
        },
        "profile_table_row_count_estimate_only": {
          "default": false,
          "description": "Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. ",
          "schema_extra": {
            "supported_sources": [
              "postgres",
              "mysql"
            ]
          },
          "title": "Profile Table Row Count Estimate Only",
          "type": "boolean"
        },
        "query_combiner_enabled": {
          "default": true,
          "description": "*This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.",
          "title": "Query Combiner Enabled",
          "type": "boolean"
        },
        "catch_exceptions": {
          "default": true,
          "description": "",
          "title": "Catch Exceptions",
          "type": "boolean"
        },
        "partition_profiling_enabled": {
          "default": true,
          "description": "Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling.",
          "schema_extra": {
            "supported_sources": [
              "athena",
              "bigquery"
            ]
          },
          "title": "Partition Profiling Enabled",
          "type": "boolean"
        },
        "partition_datetime": {
          "anyOf": [
            {
              "format": "date-time",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this.",
          "schema_extra": {
            "supported_sources": [
              "bigquery"
            ]
          },
          "title": "Partition Datetime"
        },
        "use_sampling": {
          "default": true,
          "description": "Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables. ",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Use Sampling",
          "type": "boolean"
        },
        "sample_size": {
          "default": 10000,
          "description": "Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True.",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Sample Size",
          "type": "integer"
        },
        "profile_external_tables": {
          "default": false,
          "description": "Whether to profile external tables. Only Snowflake and Redshift supports this.",
          "schema_extra": {
            "supported_sources": [
              "redshift",
              "snowflake"
            ]
          },
          "title": "Profile External Tables",
          "type": "boolean"
        },
        "tags_to_ignore_sampling": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`.",
          "title": "Tags To Ignore Sampling"
        },
        "profile_nested_fields": {
          "default": false,
          "description": "Whether to profile complex types like structs, arrays and maps. ",
          "title": "Profile Nested Fields",
          "type": "boolean"
        },
        "nested_field_max_depth": {
          "default": 10,
          "description": "Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch).",
          "exclusiveMinimum": 0,
          "title": "Nested Field Max Depth",
          "type": "integer"
        },
        "query_timeout": {
          "default": 300,
          "description": "Time before cancelling Dremio profiling query",
          "title": "Query Timeout",
          "type": "integer"
        }
      },
      "title": "ProfileConfig",
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
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "incremental_properties": {
      "default": false,
      "description": "When enabled, emits dataset properties as incremental to existing dataset properties in DataHub. When disabled, re-states dataset properties on each run.",
      "title": "Incremental Properties",
      "type": "boolean"
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
      "description": "Stateful ingestion config with stale-entity removal support."
    },
    "hostname": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Hostname or IP Address of the Dremio server",
      "title": "Hostname"
    },
    "port": {
      "default": 9047,
      "description": "Port of the Dremio REST API",
      "title": "Port",
      "type": "integer"
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
      "description": "Dremio username",
      "title": "Username"
    },
    "authentication_method": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "PAT",
      "description": "Authentication method: 'password' or 'PAT' (Personal Access Token)",
      "title": "Authentication Method"
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
      "description": "Dremio password or Personal Access Token",
      "title": "Password"
    },
    "tls": {
      "default": true,
      "description": "Whether the Dremio REST API port is encrypted",
      "title": "Tls",
      "type": "boolean"
    },
    "disable_certificate_verification": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Disable TLS certificate verification",
      "title": "Disable Certificate Verification"
    },
    "path_to_certificates": {
      "default": "/home/runner/work/datahub/datahub/metadata-ingestion/venv/lib/python3.10/site-packages/certifi/cacert.pem",
      "description": "Path to SSL certificates",
      "title": "Path To Certificates",
      "type": "string"
    },
    "is_dremio_cloud": {
      "default": false,
      "description": "Whether this is a Dremio Cloud instance",
      "title": "Is Dremio Cloud",
      "type": "boolean"
    },
    "dremio_cloud_region": {
      "default": "US",
      "description": "Dremio Cloud region ('US' or 'EU')",
      "enum": [
        "US",
        "EU"
      ],
      "title": "Dremio Cloud Region",
      "type": "string"
    },
    "dremio_cloud_project_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "ID of Dremio Cloud Project. Found in Project Settings in the Dremio Cloud UI",
      "title": "Dremio Cloud Project Id"
    },
    "domain": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Domain for all source objects.",
      "title": "Domain"
    },
    "include_system_tables": {
      "default": true,
      "description": "Whether to include system tables and schemas (INFORMATION_SCHEMA, SYS) in ingestion. ",
      "title": "Include System Tables",
      "type": "boolean"
    },
    "source_mappings": {
      "anyOf": [
        {
          "items": {
            "$ref": "#/$defs/DremioSourceMapping"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Mappings from Dremio sources to DataHub platforms and datasets.",
      "title": "Source Mappings"
    },
    "source_type_mappings": {
      "additionalProperties": {
        "$ref": "#/$defs/DremioSourceTypeOverride"
      },
      "description": "Register additional Dremio source-type strings on top of the built-in mapping, for Dremio ARP connectors or source types not yet known to DataHub. Keys are the Dremio `type` field (case-insensitive); values declare the DataHub platform and optional category. Example: `{\"MYORG_KAFKA\": {\"platform\": \"kafka\", \"category\": \"database\"}}`.",
      "title": "Source Type Mappings",
      "type": "object"
    },
    "schema_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for schemas to filter"
    },
    "dataset_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for tables and views to filter in ingestion. Specify regex to match the entire table name in dremio.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'dremio.public.customer.*'"
    },
    "profile_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for tables to profile"
    },
    "profiling": {
      "$ref": "#/$defs/ProfileConfig",
      "default": {
        "method": "sqlalchemy",
        "enabled": false,
        "operation_config": {
          "lower_freq_profile_enabled": false,
          "profile_date_of_month": null,
          "profile_day_of_week": null
        },
        "limit": null,
        "offset": null,
        "profile_table_level_only": false,
        "include_field_null_count": true,
        "include_field_distinct_count": true,
        "include_field_min_value": true,
        "include_field_max_value": true,
        "include_field_mean_value": true,
        "include_field_median_value": false,
        "include_field_stddev_value": true,
        "include_field_quantiles": false,
        "include_field_distinct_value_frequencies": false,
        "include_field_histogram": false,
        "include_field_sample_values": true,
        "max_workers": 20,
        "report_dropped_profiles": false,
        "turn_off_expensive_profiling_metrics": false,
        "field_sample_values_limit": 20,
        "max_number_of_fields_to_profile": null,
        "profile_if_updated_since_days": null,
        "profile_table_size_limit": 5,
        "profile_table_row_limit": 5000000,
        "profile_table_row_count_estimate_only": false,
        "query_combiner_enabled": true,
        "catch_exceptions": true,
        "partition_profiling_enabled": true,
        "partition_datetime": null,
        "use_sampling": true,
        "sample_size": 10000,
        "profile_external_tables": false,
        "tags_to_ignore_sampling": null,
        "profile_nested_fields": false,
        "nested_field_max_depth": 10,
        "query_timeout": 300
      },
      "description": "Configuration for profiling"
    },
    "max_workers": {
      "default": 20,
      "description": "Number of worker threads to use for parallel processing",
      "title": "Max Workers",
      "type": "integer"
    },
    "batch_size": {
      "default": 10000,
      "description": "Number of rows to fetch per page when reading Dremio's system tables (datasets, columns, view definitions, and queries) via LIMIT/OFFSET. Dremio's system tables can be slow, so a larger batch reduces the number of round-trips. Set to 0 to fetch as much as possible per page. Values are clamped to a safety ceiling of 1,000,000 rows per page to keep pagination reliable. A larger batch also raises the amount Dremio must materialize per job, so lower this value if Dremio runs out of memory during extraction.",
      "minimum": 0,
      "title": "Batch Size",
      "type": "integer"
    },
    "include_query_lineage": {
      "default": false,
      "description": "Whether to include query-based lineage information.",
      "title": "Include Query Lineage",
      "type": "boolean"
    },
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, lineage aspects are emitted as PATCH operations rather than full overwrites. This preserves any lineage edges that were manually added in DataHub between runs. Disable if you want each run to fully replace lineage. PATCH emission requires a GMS that supports patch aspects.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "enable_stateful_time_window": {
      "default": false,
      "description": "Enable stateful time window tracking for query lineage/usage extraction. When enabled, subsequent runs will skip time windows already fully processed, avoiding redundant API calls. Requires stateful_ingestion to be configured.",
      "title": "Enable Stateful Time Window",
      "type": "boolean"
    },
    "ingest_owner": {
      "default": true,
      "description": "Ingest Owner from source. This will override Owner info entered from UI",
      "title": "Ingest Owner",
      "type": "boolean"
    }
  },
  "title": "DremioSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Stateful Ingestion

Enabling `stateful_ingestion` unlocks three incremental capabilities that reduce API load on repeated runs:

| Feature                   | Config key                                | What it does                                                                                                                                                          |
| ------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stale entity removal      | `stateful_ingestion.enabled: true`        | Removes entities from DataHub that no longer exist in Dremio                                                                                                          |
| Time-window deduplication | `enable_stateful_time_window: true`       | Advances the query lineage start time to the previous run's end time, so `SYS.JOBS_RECENT` is never re-processed                                                      |
| Incremental profiling     | `profiling.profile_if_updated_since_days` | Skips re-profiling tables that DataHub profiled within the configured window (compared against last-profiled time, since Dremio has no table modification timestamps) |

```yaml
stateful_ingestion:
  enabled: true

enable_stateful_time_window: true # only process new job history each run

profiling:
  enabled: true
  profile_if_updated_since_days: 1 # re-profile at most once per day
```

#### Preserving Manual Edits Between Runs

By default, each ingestion run overwrites the full `DatasetProperties` aspect, which resets any
descriptions or custom properties edited in the DataHub UI. Set `incremental_properties: true` to
emit properties as PATCH operations instead, so only the fields the connector knows about are
updated and your manual edits are preserved.

This is particularly useful for Dremio, which often acts as a semantic layer on top of raw sources
that teams re-document in DataHub.

```yaml
incremental_properties: true
```

Set `incremental_lineage: true` to emit lineage as PATCH operations, so manually-curated
lineage edges added in the DataHub UI are not removed on the next run. Defaults to `false`
(full-overwrite) for consistency with the standard `IncrementalLineageConfigMixin` used by other
connectors; PATCH emission requires a GMS that supports patch aspects.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.dremio.dremio_source.DremioSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/dremio/dremio_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Dremio, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
