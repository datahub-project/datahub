# Delta Lake 

<!-- Set Support Status -->
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)

## Integration Details

This integration is extracting metadata from delta lake implementation via a [Delta Sharing Server](https://github.com/delta-io/delta-sharing). The implementation relies on the metadata supported by the delta sharing protocol. For more details see [here](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md).

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is extracted from (ie. logs, source API, manifest, etc.)   -->

### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrevant DataHub Concepts -->

| Source Concept | DataHub Concept | Notes |
| -- | -- | -- |
| | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) | |
| Table | [Dataset](docs/generated/metamodel/entities/dataset.md) | |
| Field | SchemaField | can be nested for structure, map or array types| 


### Supported Capabilities

<!-- This should be an auto-generated table of supported DataHub features/functionality -->
<!-- Each capability should link out to a feature guide -->

| Capability | Status | Notes |
| --- | :-: | --- |
| Data Domain | ❌ | Requires transformer |
| Dataset Profiling | ❌ | Currently not supported. This would have to extracted from the stats files |
| Extract Descriptions | ✅ | Enabled by default |
| Extract Ownership | ❌ | Not available in delta sharing interface |
| Partition Support | ✅ | Enabled by default |
| Platform Instance | ✅ | Enabled by default |

## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest metadata from Delta Lake, you will need:

* a delta sharing server (API)
* delta sharing python library (0.4.0+)

### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[delta-lake]'`


### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion. 

_For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes)._

#### `'acryl-datahub[source-name]'`

```yml
source:
  type: delta_lake
  config:
    url: https://url.to.api/
    token: ${token_passkey}
    share_credentials_version: 1
    share_pattern:
      allow:
        - "delta_sharing"
      ignoreCase: true
    schema_pattern:
      deny:
        - "zzz"
      ignoreCase: true
    table_pattern:
      allow:
        - "COVID_19_NYT"
        - "lending_club"
      ignoreCase: true

sink:
  # sink configs
```

<details>
  <summary>View All Recipe Configuartion Options</summary>
  
  | Field | Required | Default | Description |
  | --- | :-: | :-: | --- |
  | `url` | ✅ |  | URL to API |
  | `token` | ✅ | | API token passkey |
  | `share_credentials_version` | ✅ | 1| version of delta share credentials to use |
  | `share_pattern.allow` | ❌ | | List of regex patterns for shares to include in ingestion. |
  | `share_pattern.deny` | ❌ | | List of regex patterns for shares to exclude from ingestion. |
  | `share_pattern.ignoreCase` | ❌ | `True` | Whether to ignore case sensitivity during pattern matching |
  | `schema_pattern.allow` | ❌ | | List of regex patterns for schemas to include in ingestion. |
  | `schema_pattern.deny` | ❌ | | List of regex patterns for schemas to exclude from ingestion. |
  | `schema_pattenn.ignoreCase` | ❌ | `True` | Whether to ignore case sensitivity during pattern matching |
  | `table_pattern.allow` | ❌ | | List of regex patterns for tables to include in ingestion. |
  | `table_pattern.deny` | ❌ | | List of regex patterns for tables to exclude from ingestion. |
  | `table_pattenn.ignoreCase` | ❌ | `True` | Whether to ignore case sensitivity during pattern matching |
</details>

## Troubleshooting

### [Common Issue]