# Source Name 

<!-- Set Support Status -->
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)

## Integration Details

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
| | [Dataset](docs/generated/metamodel/entities/dataset.md) | |
| | [Data Job](docs/generated/metamodel/entities/dataJob.md) | |
| | [Data Flow](docs/generated/metamodel/entities/dataFlow.md) | |
| | [Chart](docs/generated/metamodel/entities/chart.md) | |
| | [Dashboard](docs/generated/metamodel/entities/dashboard.md) | |
| | [User (a.k.a CorpUser)](docs/generated/metamodel/entities/corpuser.md) | |
| | CorpGroup | |
| | Domain | |
| | Container | | 
| | Tag | | 
| | GlossaryTerm | | 
| | GlossaryNode | | 
| | Assertion | | 
| | DataProcess | | 
| | MlFeature | | 
| | MlFeatureTable | | 
| | MlModel | | 
| | MlModelDeployment | | 
| | MlPrimaryKey | | 
| | SchemaField | | 
| | DataHubPolicy | | 
| | DataHubIngestionSource | | 
| | DataHubSecret | | 
| | DataHubExecutionRequest | | 
| | DataHubREtention | | 

### Supported Capabilities

<!-- This should be an auto-generated table of supported DataHub features/functionality -->
<!-- Each capability should link out to a feature guide -->

| Capability | Status | Notes |
| --- | :-: | --- |
| Data Container | ✅ | Enabled by default |
| Detect Deleted Entities | ✅ | Requires recipe configuration |
| Data Domain | ❌ | Requires transformer |
| Dataset Profiling | ✅ | Requires `acryl-datahub[source-usage-name]` |
| Dataset Usage | ✅ | Requires `acryl-datahub[source-usage-name]` |
| Extract Descriptions | ✅ | Enabled by default |
| Extract Lineage | ✅ | Enabled by default |
| Extract Ownership | ✅ | Enabled by default |
| Extract Tags | ❌ | Requires transformer |
| Partition Support | ❌ | Not applicable to source |
| Platform Instance | ❌ | Not applicable to source |
| ... | |

## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest metadata from [Source Name], you will need:

* eg. Python version, source version, source access requirements
* eg. Steps to configure source access
* ...

### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[source-name]'`

`pip install 'acryl-datahub[source-usage-name]'`

### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion. 

_For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes)._

#### `'acryl-datahub[source-name]'`

```yml
source:
  type: source_name
  config:
    # Required fields
    option1: value1

sink:
  # sink configs
```

<details>
  <summary>View All Recipe Configuartion Options</summary>
  
  | Field | Required | Default | Description |
  | --- | :-: | :-: | --- |
  | `field1` | ✅ | `default_value` | A required field with a default value |
  | `field2` | ❌ | `default_value` | An optional field with a default value |
  | `field3` | ❌ | | An optional field without a default value |
  | ... | | |
</details>

#### `'acryl-datahub[source-usage-name]'`

```yml
source:
  type: source-usage-name
  config:
    # Required Fields
    option1: value1

    # Options
    top_n_queries: 10

sink:
  # sink configs
```

<details>
  <summary>View All Recipe Configuartion Options</summary>
  
  | Field | Required | Default | Description |
  | --- | :-: | :-: | --- |
  | `field1` | ✅ | `default_value` | A required field with a default value |
  | `field2` | ❌ | `default_value` | An optional field with a default value |
  | `field3` | ❌ | | An optional field without a default value |
  | ... | | |
</details>

## Troubleshooting

### [Common Issue]

[Provide description of common issues with this integration and steps to resolve]
