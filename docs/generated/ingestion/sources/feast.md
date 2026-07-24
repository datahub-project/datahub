


# Feast

## Overview

Feast is a machine learning platform. Learn more in the [official Feast documentation](https://feast.dev/).

The DataHub integration for Feast covers ML entities such as models, features, and related lineage metadata. It also captures table-level lineage and stateful deletion detection.

## Concept Mapping

- Entities as `MLPrimaryKey`
- Fields as `MLFeature`
- Feature views and on-demand feature views as `MLFeatureTable`
- Batch and stream source details as `Dataset`
- Column types associated with each entity and feature

Use this mapping to align feature-store metadata with existing ML entity governance patterns in DataHub.


## Module `feast`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |

### Overview

The `feast` module ingests metadata from Feast into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.


### Install the Plugin
```shell
pip install 'acryl-datahub[feast]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: feast
  config:
    # Coordinates
    path: "/path/to/repository/"
    # Options
    environment: "PROD"

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">path</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to Feast repository  |
| <div className="path-line"><span className="path-main">enable_owner_extraction</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If this is disabled, then we NEVER try to map owners. If this is enabled, then owner_mappings is REQUIRED to extract ownership. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">enable_tag_extraction</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If this is disabled, then we NEVER try to extract tags. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">environment</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Environment to use when constructing URNs <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">fs_yaml_file</span></div> <div className="type-name-line"><span className="type-name">One of string(path), null</span></div> | Path to the `feature_store.yaml` file used to configure the feature store <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">owner_mappings</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Mapping of owner names to owner types <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">owner_mappings.</span><span className="path-main">map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulIngestionConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "StatefulIngestionConfig": {
      "additionalProperties": false,
      "description": "Basic Stateful Ingestion Specific Configuration for any source.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        }
      },
      "title": "StatefulIngestionConfig",
      "type": "object"
    }
  },
  "properties": {
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulIngestionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful Ingestion Config"
    },
    "path": {
      "description": "Path to Feast repository",
      "title": "Path",
      "type": "string"
    },
    "fs_yaml_file": {
      "anyOf": [
        {
          "format": "path",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to the `feature_store.yaml` file used to configure the feature store",
      "title": "Fs Yaml File"
    },
    "environment": {
      "default": "PROD",
      "description": "Environment to use when constructing URNs",
      "title": "Environment",
      "type": "string"
    },
    "owner_mappings": {
      "anyOf": [
        {
          "items": {
            "additionalProperties": {
              "type": "string"
            },
            "type": "object"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Mapping of owner names to owner types",
      "title": "Owner Mappings"
    },
    "enable_owner_extraction": {
      "default": false,
      "description": "If this is disabled, then we NEVER try to map owners. If this is enabled, then owner_mappings is REQUIRED to extract ownership.",
      "title": "Enable Owner Extraction",
      "type": "boolean"
    },
    "enable_tag_extraction": {
      "default": false,
      "description": "If this is disabled, then we NEVER try to extract tags.",
      "title": "Enable Tag Extraction",
      "type": "boolean"
    }
  },
  "required": [
    "path"
  ],
  "title": "FeastRepositorySourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.feast.FeastRepositorySource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/feast.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Feast, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
