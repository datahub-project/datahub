


# DataHubApply

## Overview

DataHub Apply is a DataHub utility or metadata-focused integration. Learn more in the [official DataHub Apply documentation](https://datahub.com/docs/).

The DataHub integration for DataHub Apply covers metadata entities and operational objects relevant to this connector. It applies metadata from YAML/JSON files to existing DataHub entities.

## Concept Mapping

| Source Concept                             | DataHub Concept                                      | Notes                                                    |
| ------------------------------------------ | ---------------------------------------------------- | -------------------------------------------------------- |
| Apply operation input                      | Metadata Change Proposal (MCP) updates               | Input drives metadata updates rather than discovery.     |
| Asset target list                          | Dataset / Container (and other supported entities)   | Targets are selected explicitly in recipe configuration. |
| Ownership / domain / tag / term assignment | Ownership, Domain, GlobalTags, GlossaryTerms aspects | Applied directly to existing DataHub entities.           |


## Module `datahub-apply`
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
Capability metadata is not explicitly declared for this module. Refer to module documentation and configuration sections below.

### Overview

The `datahub-apply` module applies metadata changes directly to existing DataHub entities. It is useful for programmatic curation tasks such as bulk ownership, domain, tag, and glossary-term updates.

### Prerequisites

- Access to a DataHub instance with permissions to update target entities.
- Valid authentication configuration for the ingestion run.
- Existing target entities in DataHub for each configured apply operation.


### Install the Plugin
```shell
pip install 'acryl-datahub[datahub-apply]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: datahub-apply
  config:
    owner_apply:
      - owner_urn: "urn:li:corpuser:datahub"
        assets:
          - "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">domain_apply</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | List to apply domains to assets <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">domain_apply.</span><span className="path-main">DomainApplyConfig</span></div> <div className="type-name-line"><span className="type-name">DomainApplyConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain_apply.DomainApplyConfig.</span><span className="path-main">domain_urn</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-prefix">domain_apply.DomainApplyConfig.</span><span className="path-main">assets</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of assets to apply domain hierarchically. Currently only containers and datasets are supported  |
| <div className="path-line"><span className="path-prefix">domain_apply.DomainApplyConfig.assets.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">owner_apply</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | List to apply owners to assets <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">owner_apply.</span><span className="path-main">OwnerApplyConfig</span></div> <div className="type-name-line"><span className="type-name">OwnerApplyConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">owner_apply.OwnerApplyConfig.</span><span className="path-main">owner_urn</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-prefix">owner_apply.OwnerApplyConfig.</span><span className="path-main">assets</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of assets to apply owner hierarchically. Currently only containers and datasets are supported  |
| <div className="path-line"><span className="path-prefix">owner_apply.OwnerApplyConfig.assets.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">tag_apply</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | List to apply tags to assets <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">tag_apply.</span><span className="path-main">TagApplyConfig</span></div> <div className="type-name-line"><span className="type-name">TagApplyConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">tag_apply.TagApplyConfig.</span><span className="path-main">tag_urn</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-prefix">tag_apply.TagApplyConfig.</span><span className="path-main">assets</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of assets to apply tag hierarchically. Currently only containers and datasets are supported  |
| <div className="path-line"><span className="path-prefix">tag_apply.TagApplyConfig.assets.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">term_apply</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | List to apply terms to assets <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">term_apply.</span><span className="path-main">TermApplyConfig</span></div> <div className="type-name-line"><span className="type-name">TermApplyConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">term_apply.TermApplyConfig.</span><span className="path-main">term_urn</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-prefix">term_apply.TermApplyConfig.</span><span className="path-main">assets</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of assets to apply term hierarchically. Currently only containers and datasets are supported  |
| <div className="path-line"><span className="path-prefix">term_apply.TermApplyConfig.assets.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "DomainApplyConfig": {
      "additionalProperties": false,
      "properties": {
        "assets": {
          "description": "List of assets to apply domain hierarchically. Currently only containers and datasets are supported",
          "items": {
            "type": "string"
          },
          "title": "Assets",
          "type": "array"
        },
        "domain_urn": {
          "default": "",
          "title": "Domain Urn",
          "type": "string"
        }
      },
      "title": "DomainApplyConfig",
      "type": "object"
    },
    "OwnerApplyConfig": {
      "additionalProperties": false,
      "properties": {
        "assets": {
          "description": "List of assets to apply owner hierarchically. Currently only containers and datasets are supported",
          "items": {
            "type": "string"
          },
          "title": "Assets",
          "type": "array"
        },
        "owner_urn": {
          "default": "",
          "title": "Owner Urn",
          "type": "string"
        }
      },
      "title": "OwnerApplyConfig",
      "type": "object"
    },
    "TagApplyConfig": {
      "additionalProperties": false,
      "properties": {
        "assets": {
          "description": "List of assets to apply tag hierarchically. Currently only containers and datasets are supported",
          "items": {
            "type": "string"
          },
          "title": "Assets",
          "type": "array"
        },
        "tag_urn": {
          "default": "",
          "title": "Tag Urn",
          "type": "string"
        }
      },
      "title": "TagApplyConfig",
      "type": "object"
    },
    "TermApplyConfig": {
      "additionalProperties": false,
      "properties": {
        "assets": {
          "description": "List of assets to apply term hierarchically. Currently only containers and datasets are supported",
          "items": {
            "type": "string"
          },
          "title": "Assets",
          "type": "array"
        },
        "term_urn": {
          "default": "",
          "title": "Term Urn",
          "type": "string"
        }
      },
      "title": "TermApplyConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "domain_apply": {
      "anyOf": [
        {
          "items": {
            "$ref": "#/$defs/DomainApplyConfig"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "List to apply domains to assets",
      "title": "Domain Apply"
    },
    "tag_apply": {
      "anyOf": [
        {
          "items": {
            "$ref": "#/$defs/TagApplyConfig"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "List to apply tags to assets",
      "title": "Tag Apply"
    },
    "term_apply": {
      "anyOf": [
        {
          "items": {
            "$ref": "#/$defs/TermApplyConfig"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "List to apply terms to assets",
      "title": "Term Apply"
    },
    "owner_apply": {
      "anyOf": [
        {
          "items": {
            "$ref": "#/$defs/OwnerApplyConfig"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "List to apply owners to assets",
      "title": "Owner Apply"
    }
  },
  "title": "DataHubApplyConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features. This module focuses on applying metadata updates rather than extracting metadata from external systems.

### Limitations

- This module does not discover source metadata; it only applies configured updates to existing DataHub entities.
- Incorrect URNs or selectors can lead to partial updates or no-ops.

### Troubleshooting

- Validate target URNs and entity existence before running large apply jobs.
- Start with a small scoped recipe to verify permissions and expected update behavior.
- Review ingestion logs for validation or authorization errors returned by DataHub APIs.


### Code Coordinates
- Class Name: `datahub.ingestion.source.apply.datahub_apply.DataHubApplySource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/apply/datahub_apply.py)


:::tip Questions?

If you've got any questions on configuring ingestion for DataHubApply, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
