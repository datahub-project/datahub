


# Demo Data

## Overview

Demo Data is a DataHub utility or metadata-focused integration. Learn more in the [official Demo Data documentation](https://datahub.com/docs/).

The DataHub integration for Demo Data covers metadata entities and operational objects relevant to this connector. It ingests pre-built sample metadata to populate a DataHub instance for demonstration purposes.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `demo-data`

### Important Capabilities
Capability metadata is not explicitly declared for this module. Refer to module documentation and configuration sections below.

### Overview

The `demo-data` source loads curated data packs into DataHub. By default it loads the **bootstrap** sample data (datasets, dashboards, users, and tags) with original timestamps.

It also supports loading named packs from the DataHub registry (e.g. `showcase-ecommerce`, `covid-bigquery`) or custom URLs, with optional time-shifting to make timestamps appear recent.

Use this source for demos, testing, or bootstrapping a DataHub instance with realistic metadata.

### Prerequisites

A running DataHub instance. No external credentials or network access beyond the pack URL is required.


### Install the Plugin
```shell
pip install 'acryl-datahub[demo-data]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
# Zero-config: load bootstrap sample data
source:
  type: demo-data
  config: {}

# Or load a specific data pack with time-shifting:
# source:
#   type: demo-data
#   config:
#     pack_name: "showcase-ecommerce"
#     no_time_shift: false

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">as_of</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | ISO 8601 datetime to use as the time-shift target (default: current time). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">no_cache</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Force re-download even if the pack is cached. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">no_time_shift</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If true, load with original timestamps (no time-shifting). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">pack_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Name of a data pack from the registry (e.g. 'bootstrap', 'showcase-ecommerce'). <div className="default-line default-line-with-docs">Default: <span className="default-value">bootstrap</span></div> |
| <div className="path-line"><span className="path-main">pack_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | HTTP(S) URL to an MCP/MCE JSON file. Use instead of pack_name for custom packs. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">trust_community</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Allow loading community-contributed packs without warning. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">trust_custom</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Allow loading from unverified URLs without warning. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "additionalProperties": false,
  "description": "Configuration for the Demo Data source.\n\nWith no configuration, loads the \"bootstrap\" pack with original timestamps.",
  "properties": {
    "pack_name": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "bootstrap",
      "description": "Name of a data pack from the registry (e.g. 'bootstrap', 'showcase-ecommerce').",
      "title": "Pack Name"
    },
    "pack_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "HTTP(S) URL to an MCP/MCE JSON file. Use instead of pack_name for custom packs.",
      "title": "Pack Url"
    },
    "no_time_shift": {
      "default": true,
      "description": "If true, load with original timestamps (no time-shifting).",
      "title": "No Time Shift",
      "type": "boolean"
    },
    "as_of": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "ISO 8601 datetime to use as the time-shift target (default: current time).",
      "title": "As Of"
    },
    "trust_community": {
      "default": false,
      "description": "Allow loading community-contributed packs without warning.",
      "title": "Trust Community",
      "type": "boolean"
    },
    "trust_custom": {
      "default": false,
      "description": "Allow loading from unverified URLs without warning.",
      "title": "Trust Custom",
      "type": "boolean"
    },
    "no_cache": {
      "default": false,
      "description": "Force re-download even if the pack is cached.",
      "title": "No Cache",
      "type": "boolean"
    }
  },
  "title": "DemoDataConfig",
  "type": "object"
}
```





### Capabilities

- Loads any named pack from the DataHub registry (`bootstrap`, `showcase-ecommerce`, `covid-bigquery`)
- Loads custom packs from arbitrary HTTP(S) URLs via `pack_url`
- Time-shifts timestamps so ingested metadata appears current (set `no_time_shift: false`)
- SHA256 integrity verification for registry packs
- Local caching to avoid repeated downloads

### Limitations

- Data packs are read-only collections of MCPs; they cannot be modified before loading.
- Time-shifting adjusts all temporal fields by a fixed offset — relative ordering is preserved but absolute times may not match real-world events.
- Custom URL packs (`pack_url`) bypass SHA256 verification unless the pack includes a checksum.

### Troubleshooting

- **"Pack not found"**: Verify the pack name with `datahub datapack list`. Pack names are case-sensitive.
- **Trust errors**: Community and custom packs require explicit opt-in via `trust_community: true` or `trust_custom: true`.
- **Download failures**: Check network connectivity to the pack URL. Use `no_cache: true` to force a fresh download if a cached file is corrupted.


### Code Coordinates
- Class Name: `datahub.ingestion.source.demo_data.DemoDataSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/demo_data.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Demo Data, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
