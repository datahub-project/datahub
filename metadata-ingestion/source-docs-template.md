# Source Name 

<!-- Set Support Status -->
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)

## Integration Details

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is etracted from (ie. logs, source API, manifest, etc.)   -->

### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Metadata Source Concepts to DataHub Concepts:

| Source Concept | DataHub Concept | Notes |
| -- | -- | -- |
| eg. Project | eg. Container | eg. This is automatically extracted from the source |
| eg. View | eg. Dataset | eg. This is modeled as a Dataset with a View subtype |
| ... | | | 


### Supported Capabilities

<!-- This should be an auto-generated table of supported DataHub features/functionality -->
<!-- Each capability should link out to a feature guide -->

| Capability | Status | Notes |
| -- | -- | -- |
| Data Container | ✅ | Enabled by default |
| Detect Deleted Entities | ✅ | Requires recipe configuration |
| Data Domain | ❌ | Requires transformer |
| Dataset Profiling | ✅ | Requires `acryl-datahub[source-usage]` |
| Dataset Usage | ✅ | Requires `acryl-datahub[source-usage]` |
| Extract Descriptions | ✅ | Enabled by default |
| Extract Lineage | ✅ | Enabled by default |
| Extract Ownership | ✅ | Enabled by default |
| Extract Tags | ❌ | Requires transformer |
| Partition Support | ❌ | Not applicable to source |
| Platform Instance | ❌ | Not applicable to source |

## Quickstart

### 1. Prerequisites

In order to ingest metadata from [Source Name], you will need:

* eg. Python version, source version, source access requirements
* eg. Steps to configure source access

### 2. Install the Plugin

run `pip install 'acryl-datahub[source_name]'`

### 3. Create an Ingestion Recipe

Use the following recipe to get started with ingestion! See [below](#recipe-configuartion-detials) for full configuration options.

_For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes)._

```yml
source:
  type: source_name
  config:
    # Required fields
    option1: value1

sink:
  # sink configs
```

#### Recipe Configuartion Details

| Field | Required | Default | Description |
| -- | -- | -- | -- |
| `field1` | ✅ | `default_value` | A required field with a default value |
| `field2` | | `default_value` | An optional field with a default value |
| `field3` | | | An optional field without a default value |

### 4. Troubleshooting

### [Common Issue]

[Provide description of common issues with this integration and steps to resolve]