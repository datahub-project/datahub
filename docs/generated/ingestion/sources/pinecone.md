


# Pinecone

## Overview

Pinecone is a managed vector database platform used to store, index, and query high-dimensional vector embeddings for AI and machine learning applications. Learn more in the [official Pinecone documentation](https://docs.pinecone.io/).

The DataHub integration for Pinecone extracts metadata about indexes, namespaces, and vector collections, including inferred schemas from vector metadata fields.

## Concept Mapping

| Source Concept    | DataHub Concept                | Notes                                                |
| ----------------- | ------------------------------ | ---------------------------------------------------- |
| Pinecone Account  | Platform Instance              | Organizes assets within the platform context.        |
| Index             | Container (PINECONE_INDEX)     | Top-level organizational unit storing vectors.       |
| Namespace         | Container (PINECONE_NAMESPACE) | Logical partition within an index.                   |
| Vector Collection | Dataset                        | Represents the collection of vectors in a namespace. |
| Metadata Fields   | SchemaField                    | Inferred from sampled vector metadata.               |


## Module `pinecone`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |

### Overview

The `pinecone` module ingests metadata from Pinecone vector database into DataHub. It extracts index configurations, namespace statistics, and infers schemas from vector metadata fields.

### Prerequisites

Before running ingestion, ensure you have a valid Pinecone API key with read access to your indexes.

#### Steps to Get the Required Information

1. Log in to the [Pinecone Console](https://app.pinecone.io/).
2. Navigate to **API Keys** in the left sidebar.
3. Copy an existing API key or create a new one with read permissions.

:::note

Schema inference samples vectors from each namespace to build a schema. This requires that your vectors have metadata fields attached. If vectors have no metadata, schema inference is skipped gracefully.

:::


### Install the Plugin
```shell
pip install 'acryl-datahub[pinecone]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: pinecone
  config:
    # Required: Pinecone API key
    api_key: "${PINECONE_API_KEY}"

    # Optional: Platform instance for multi-environment setups
    # platform_instance: "production"

    # Optional: Filter indexes by name pattern
    # index_pattern:
    #   allow:
    #     - "prod-.*"
    #   deny:
    #     - ".*-test"

    # Optional: Filter namespaces by name pattern
    # namespace_pattern:
    #   allow:
    #     - "customer-.*"

    # Optional: Schema inference settings
    # enable_schema_inference: true
    # schema_sampling_size: 100
    # max_metadata_fields: 100

    # Optional: Stateful ingestion for stale entity removal
    # stateful_ingestion:
    #   enabled: true
    #   remove_stale_metadata: true

sink:
  # config sinks

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_key</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Pinecone API key for authentication. Can be found in the Pinecone console under API Keys.  |
| <div className="path-line"><span className="path-main">enable_schema_inference</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to infer schemas from vector metadata. When enabled, samples vectors from each namespace to build a schema. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">environment</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Pinecone environment (for pod-based indexes). Not required for serverless indexes. Example: 'us-west1-gcp' <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">index_host_mapping</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional manual mapping of index names to host URLs. Useful if automatic host resolution fails. Example: {'my-index': 'my-index-abc123.svc.pinecone.io'} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">max_metadata_fields</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of metadata fields to include in the inferred schema. Limits schema size for namespaces with many metadata fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of parallel workers for processing indexes and namespaces. Increase for faster ingestion of many indexes. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">schema_sampling_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of vectors to sample per namespace for schema inference. Higher values provide more accurate schemas but increase ingestion time. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">index_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">index_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">namespace_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">namespace_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration for tracking processed entities and removing stale metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
  "description": "Configuration for Pinecone source.\n\nExtracts metadata from Pinecone vector database including:\n- Index configurations (dimension, metric, type)\n- Namespace information and statistics\n- Inferred schemas from vector metadata",
  "properties": {
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
      "description": "Stateful ingestion configuration for tracking processed entities and removing stale metadata."
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
    "api_key": {
      "description": "Pinecone API key for authentication. Can be found in the Pinecone console under API Keys.",
      "format": "password",
      "title": "Api Key",
      "type": "string",
      "writeOnly": true
    },
    "environment": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Pinecone environment (for pod-based indexes). Not required for serverless indexes. Example: 'us-west1-gcp'",
      "title": "Environment"
    },
    "index_host_mapping": {
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
      "description": "Optional manual mapping of index names to host URLs. Useful if automatic host resolution fails. Example: {'my-index': 'my-index-abc123.svc.pinecone.io'}",
      "title": "Index Host Mapping"
    },
    "index_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for indexes to filter in ingestion. Specify 'allow' patterns to include specific indexes, and 'deny' patterns to exclude indexes."
    },
    "namespace_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for namespaces to filter in ingestion. Specify 'allow' patterns to include specific namespaces, and 'deny' patterns to exclude namespaces."
    },
    "enable_schema_inference": {
      "default": true,
      "description": "Whether to infer schemas from vector metadata. When enabled, samples vectors from each namespace to build a schema.",
      "title": "Enable Schema Inference",
      "type": "boolean"
    },
    "schema_sampling_size": {
      "default": 100,
      "description": "Number of vectors to sample per namespace for schema inference. Higher values provide more accurate schemas but increase ingestion time.",
      "exclusiveMinimum": 0,
      "title": "Schema Sampling Size",
      "type": "integer"
    },
    "max_metadata_fields": {
      "default": 100,
      "description": "Maximum number of metadata fields to include in the inferred schema. Limits schema size for namespaces with many metadata fields.",
      "exclusiveMinimum": 0,
      "title": "Max Metadata Fields",
      "type": "integer"
    },
    "max_workers": {
      "default": 5,
      "description": "Maximum number of parallel workers for processing indexes and namespaces. Increase for faster ingestion of many indexes.",
      "exclusiveMinimum": 0,
      "title": "Max Workers",
      "type": "integer"
    }
  },
  "required": [
    "api_key"
  ],
  "title": "PineconeConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

- Schema inference is based on sampled vectors and may not capture all metadata fields present in the full dataset.
- The `describe_namespace()` API is only available for serverless indexes; pod-based indexes use `describe_index_stats()` for namespace discovery.
- Vector values themselves are not ingested, only metadata fields and statistics.

### Troubleshooting

If ingestion fails, validate your API key, check network connectivity to the Pinecone API, and review ingestion logs for source-specific errors. If schema inference is slow, reduce `schema_sampling_size` or set `enable_schema_inference: false`.


### Code Coordinates
- Class Name: `datahub.ingestion.source.pinecone.pinecone_source.PineconeSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/pinecone/pinecone_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Pinecone, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
