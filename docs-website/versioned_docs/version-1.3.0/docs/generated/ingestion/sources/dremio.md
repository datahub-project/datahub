---
sidebar_position: 18
title: Dremio
slug: /generated/ingestion/sources/dremio
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/dremio.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dremio
### Concept Mapping

Here's a table for **Concept Mapping** between Dremio and DataHub to provide a clear overview of how entities and concepts in Dremio are mapped to corresponding entities in DataHub:

| Source Concept             | DataHub Concept | Notes                                                      |
| -------------------------- | --------------- | ---------------------------------------------------------- |
| **Physical Dataset/Table** | `Dataset`       | Subtype: `Table`                                           |
| **Virtual Dataset/Views**  | `Dataset`       | Subtype: `View`                                            |
| **Spaces**                 | `Container`     | Mapped to DataHub’s `Container` aspect. Subtype: `Space`   |
| **Folders**                | `Container`     | Mapped as a `Container` in DataHub. Subtype: `Folder`      |
| **Sources**                | `Container`     | Represented as a `Container` in DataHub. Subtype: `Source` |
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
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. Supported for types - Table. |


This plugin integrates with Dremio to extract and ingest metadata into DataHub.
The following types of metadata are extracted:

- Metadata for Spaces, Folders, Sources, and Datasets:
    - Includes physical and virtual datasets, with detailed information about each dataset.
    - Extracts metadata about Dremio's organizational hierarchy: Spaces (top-level), Folders (sub-level), and Sources (external data connections).

- Schema and Column Information:
    - Column types and schema metadata associated with each physical and virtual dataset.
    - Extracts column-level metadata, such as names, data types, and descriptions, if available.

- Lineage Information:
    - Dataset-level and column-level lineage tracking:
        - Dataset-level lineage shows dependencies and relationships between physical and virtual datasets.
        - Column-level lineage tracks transformations applied to individual columns across datasets.
    - Lineage information helps trace the flow of data and transformations within Dremio.

- Ownership and Glossary Terms:
    - Metadata related to ownership of datasets, extracted from Dremio’s ownership model.
    - Glossary terms and business metadata associated with datasets, providing additional context to the data.
    - Note: Ownership information will only be available for the Cloud and Enterprise editions, it will not be available for the Community edition.

- Optional SQL Profiling (if enabled):
    - Table, row, and column statistics can be profiled and ingested via optional SQL queries.
    - Extracts statistics about tables and columns, such as row counts and data distribution, for better insight into the dataset structure.

### Setup

This integration pulls metadata directly from the Dremio APIs.

You'll need to have a Dremio instance up and running with access to the necessary datasets, and API access should be enabled with a valid token.

The API token should have the necessary permissions to **read metadata** and **retrieve lineage**.

#### Steps to Get the Required Information

1. **Generate an API Token**:

   - Log in to your Dremio instance.
   - Navigate to your user profile in the top-right corner.
   - Select **Generate API Token** to create an API token for programmatic access.

2. **Permissions**:

   - The token should have **read-only** or **admin** permissions that allow it to:
     - View all datasets (physical and virtual).
     - Access all spaces, folders, and sources.
     - Retrieve dataset and column-level lineage information.

3. **Verify External Data Source Permissions**:
   - If Dremio is connected to external data sources (e.g., AWS S3, relational databases), ensure that Dremio has access to the credentials required for querying those sources.

### CLI based Ingestion

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

    # Credentials with personal access token(recommended)
    authentication_method: PAT
    password: pass
    # OR Credentials with basic auth
    # authentication_method: password
    # username: user
    # password: pass

    #For cloud instance
    #is_dremio_cloud: True
    #dremio_cloud_project_id: <project_id>

    include_query_lineage: True

    ingest_owner: true

    #Optional
    source_mappings:
      - platform: s3
        source_name: samples

    #Optional
    schema_pattern:
      allow:
        - "<source_name>.<table_name>"

sink:
  # sink configs
```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">authentication_method</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Authentication method: 'password' or 'PAT' (Personal Access Token) <div className="default-line default-line-with-docs">Default: <span className="default-value">PAT</span></div> |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-main">disable_certificate_verification</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Disable TLS certificate verification <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Domain for all source objects. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">dremio_cloud_project_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | ID of Dremio Cloud Project. Found in Project Settings in the Dremio Cloud UI <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">dremio_cloud_region</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "US", "EU" <div className="default-line default-line-with-docs">Default: <span className="default-value">US</span></div> |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-main">hostname</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Hostname or IP Address of the Dremio server <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">include_query_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to include query-based lineage information. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_owner</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest Owner from source. This will override Owner info entered from UI <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">is_dremio_cloud</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether this is a Dremio Cloud instance <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use for parallel processing <div className="default-line default-line-with-docs">Default: <span className="default-value">70</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Dremio password or Personal Access Token <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">path_to_certificates</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to SSL certificates <div className="default-line default-line-with-docs">Default: <span className="default-value">/Users/ryanholstien/OSS/fork/second/oss-public/dat...</span></div> |
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
| <div className="path-line"><span className="path-main">usage</span></div> <div className="type-name-line"><span className="type-name">BaseUsageConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report read operational stats. Experimental. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_top_n_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest the top_n_queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">top_n_queries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of top queries to save to each table. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">usage.user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">ProfileConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
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
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">70</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Offset in documents to profile. By default, uses no offset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Time before cancelling Dremio profiling query <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

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
    "BaseUsageConfig": {
      "additionalProperties": false,
      "properties": {
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
        "top_n_queries": {
          "default": 10,
          "description": "Number of top queries to save to each table.",
          "exclusiveMinimum": 0,
          "title": "Top N Queries",
          "type": "integer"
        },
        "user_email_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "regex patterns for user emails to filter in usage."
        },
        "include_operational_stats": {
          "default": true,
          "description": "Whether to display operational stats.",
          "title": "Include Operational Stats",
          "type": "boolean"
        },
        "include_read_operational_stats": {
          "default": false,
          "description": "Whether to report read operational stats. Experimental.",
          "title": "Include Read Operational Stats",
          "type": "boolean"
        },
        "format_sql_queries": {
          "default": false,
          "description": "Whether to format sql queries",
          "title": "Format Sql Queries",
          "type": "boolean"
        },
        "include_top_n_queries": {
          "default": true,
          "description": "Whether to ingest the top_n_queries.",
          "title": "Include Top N Queries",
          "type": "boolean"
        }
      },
      "title": "BaseUsageConfig",
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
          "default": 70,
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "title": "Max Workers",
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
      "default": null
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
          "type": "string"
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
      "default": "/Users/ryanholstien/OSS/fork/second/oss-public/datahub/metadata-ingestion/venv/lib/python3.11/site-packages/certifi/cacert.pem",
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
    "usage": {
      "$ref": "#/$defs/BaseUsageConfig",
      "default": {
        "bucket_duration": "DAY",
        "end_time": "2025-10-06T21:15:08.194539Z",
        "start_time": "2025-10-05T00:00:00Z",
        "queries_character_limit": 24000,
        "top_n_queries": 10,
        "user_email_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "include_operational_stats": true,
        "include_read_operational_stats": false,
        "format_sql_queries": false,
        "include_top_n_queries": true
      },
      "description": "The usage config to use when generating usage statistics"
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
        "max_workers": 70,
        "query_timeout": 300
      },
      "description": "Configuration for profiling"
    },
    "max_workers": {
      "default": 70,
      "description": "Number of worker threads to use for parallel processing",
      "title": "Max Workers",
      "type": "integer"
    },
    "include_query_lineage": {
      "default": false,
      "description": "Whether to include query-based lineage information.",
      "title": "Include Query Lineage",
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


</TabItem>
</Tabs>

### Starter Receipe for Dremio Cloud Instance

```
source:
  type: dremio
  config:
    # Authentication details
    authentication_method: PAT        # Use Personal Access Token for authentication
    password: <your_api_token>        # Replace <your_api_token> with your Dremio Cloud API token
    is_dremio_cloud: True             # Set to True for Dremio Cloud instances
    dremio_cloud_project_id: <project_id>  # Provide the Project ID for Dremio Cloud

    # Enable query lineage tracking
    include_query_lineage: True

    #Optional
    source_mappings:
      - platform: s3
        source_name: samples

    # Optional
    schema_pattern:
      allow:
        - "<source_name>.<table_name>"

sink:
    # Define your sink configuration here

```

### Code Coordinates
- Class Name: `datahub.ingestion.source.dremio.dremio_source.DremioSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/dremio/dremio_source.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Dremio, feel free to ping us on [our Slack](https://datahub.com/slack).
