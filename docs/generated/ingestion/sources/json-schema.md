


# JSON Schemas

## Overview

Json Schema is a storage and lakehouse platform. Learn more in the [official Json Schema documentation](https://json-schema.org/).

The DataHub integration for Json Schema covers file/lakehouse metadata entities such as datasets, paths, and containers. It also captures stateful deletion detection.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `json-schema`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Extracts descriptions at top level and field level. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | With stateful ingestion enabled, will remove entities from DataHub if they are no longer present in the source. |
| Extract Ownership | ❌ | Does not currently support extracting ownership. |
| Extract Tags | ❌ | Does not currently support extracting tags. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Supports platform instance via config. |
| Schema Metadata | ✅ | Extracts schemas, following references. |

### Overview

The `json-schema` module ingests metadata from Json Schema into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.


### Install the Plugin
```shell
pip install 'acryl-datahub[json-schema]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
pipeline_name: json_schema_ingestion
source:
  type: json-schema
  config:
    path: <path_to_json_file_or_directory or url> # e.g. https://json.schemastore.org/petstore-v1.0.json
    platform: <choose a platform that you want schemas to live under> # e.g. schemaregistry
    # platform_instance: <add a platform_instance if there are multiple schema repositories>
    stateful_ingestion:
      enabled: true # recommended to have this turned on

# sink configs if needed
```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">path</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">One of string(file-path), string(directory-path), string(uri)</span></div> | Set this to a single file-path or a directory-path (for recursive traversal) or a remote url. e.g. https://json.schemastore.org/petstore-v1.0.json  |
| <div className="path-line"><span className="path-main">platform</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Set this to a platform that you want all schemas to live under. e.g. schemaregistry / schemarepo etc.  |
| <div className="path-line"><span className="path-main">dataset_name_strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "BASENAME", "SCHEMA_ID", "TITLE"  |
| <div className="path-line"><span className="path-main">http_headers</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | HTTP headers to include when fetching remote schemas (e.g. for authentication). Applied to $ref resolution and remote URL downloads. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">use_id_as_base_uri</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, uses the `$id` field in the json schema as the base uri for following references. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">dataset_name_replace_pattern</span></div> <div className="type-name-line"><span className="type-name">One of URIReplacePattern, null</span></div> | Apply a string match-replace on the computed display name. Runs after dataset_name_strategy is applied. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">dataset_name_replace_pattern.</span><span className="path-main">match</span>&nbsp;<abbr title="Required if dataset_name_replace_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern to match on uri-s as part of reference resolution. See replace field  |
| <div className="path-line"><span className="path-prefix">dataset_name_replace_pattern.</span><span className="path-main">replace</span>&nbsp;<abbr title="Required if dataset_name_replace_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern to replace with as part of reference resolution. See match field  |
| <div className="path-line"><span className="path-main">uri_replace_pattern</span></div> <div className="type-name-line"><span className="type-name">One of URIReplacePattern, null</span></div> | Use this if URI-s need to be modified during reference resolution. Simple string match - replace capabilities are supported. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">uri_replace_pattern.</span><span className="path-main">match</span>&nbsp;<abbr title="Required if uri_replace_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern to match on uri-s as part of reference resolution. See replace field  |
| <div className="path-line"><span className="path-prefix">uri_replace_pattern.</span><span className="path-main">replace</span>&nbsp;<abbr title="Required if uri_replace_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern to replace with as part of reference resolution. See match field  |
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
    "DatasetNameStrategy": {
      "description": "Controls how the display name for ingested JSON schema datasets is computed.",
      "enum": [
        "BASENAME",
        "SCHEMA_ID",
        "TITLE"
      ],
      "title": "DatasetNameStrategy",
      "type": "string"
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
    },
    "URIReplacePattern": {
      "additionalProperties": false,
      "properties": {
        "match": {
          "description": "Pattern to match on uri-s as part of reference resolution. See replace field",
          "title": "Match",
          "type": "string"
        },
        "replace": {
          "description": "Pattern to replace with as part of reference resolution. See match field",
          "title": "Replace",
          "type": "string"
        }
      },
      "required": [
        "match",
        "replace"
      ],
      "title": "URIReplacePattern",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
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
    "path": {
      "anyOf": [
        {
          "format": "file-path",
          "type": "string"
        },
        {
          "format": "directory-path",
          "type": "string"
        },
        {
          "format": "uri",
          "minLength": 1,
          "type": "string"
        }
      ],
      "description": "Set this to a single file-path or a directory-path (for recursive traversal) or a remote url. e.g. https://json.schemastore.org/petstore-v1.0.json",
      "title": "Path"
    },
    "platform": {
      "description": "Set this to a platform that you want all schemas to live under. e.g. schemaregistry / schemarepo etc.",
      "title": "Platform",
      "type": "string"
    },
    "use_id_as_base_uri": {
      "default": false,
      "description": "When enabled, uses the `$id` field in the json schema as the base uri for following references.",
      "title": "Use Id As Base Uri",
      "type": "boolean"
    },
    "uri_replace_pattern": {
      "anyOf": [
        {
          "$ref": "#/$defs/URIReplacePattern"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Use this if URI-s need to be modified during reference resolution. Simple string match - replace capabilities are supported."
    },
    "dataset_name_strategy": {
      "$ref": "#/$defs/DatasetNameStrategy",
      "default": "BASENAME",
      "description": "Controls how the display name for datasets is computed. BASENAME (default) uses the last path segment. SCHEMA_ID uses the full $id URI. TITLE uses the 'title' field from the schema."
    },
    "dataset_name_replace_pattern": {
      "anyOf": [
        {
          "$ref": "#/$defs/URIReplacePattern"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Apply a string match-replace on the computed display name. Runs after dataset_name_strategy is applied."
    },
    "http_headers": {
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
      "description": "HTTP headers to include when fetching remote schemas (e.g. for authentication). Applied to $ref resolution and remote URL downloads.",
      "title": "Http Headers"
    }
  },
  "required": [
    "path",
    "platform"
  ],
  "title": "JsonSchemaSourceConfig",
  "type": "object"
}
```





#### Configuration Notes

- You must provide a `platform` field. Most organizations have custom project names for their schema repositories, so you can pick whatever name makes sense. For example, you might want to call your schema platform **schemaregistry**. After picking a custom platform, you can use the [put platform](../../../../docs/cli.md#put-platform) command to register your custom platform into DataHub.

### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Dataset Display Name Control

By default, the display name for each ingested schema is derived from the last path segment of the schema identifier (e.g. a schema with `$id: "https://example.com/schemas/domain/1.0"` gets the display name `1.0`). This can be insufficient when many schemas share similar version-based names.

The `dataset_name_strategy` config controls how display names are computed:

- `BASENAME` (default): Last path segment of the `$id` or filename. Current behavior, no breaking change.
- `SCHEMA_ID`: Uses the full `$id` URI as the display name. Falls back to BASENAME if no `$id` is present.
- `TITLE`: Uses the `title` field from the JSON schema. Falls back to BASENAME if no title is present.

Additionally, `dataset_name_replace_pattern` applies a simple string match-replace on the computed display name, regardless of which strategy is used.

These settings only affect the display name shown in the DataHub UI (`DatasetProperties.name`). The dataset URN is not affected.

Example: use the full schema `$id` as the display name, stripping the host prefix:

```yaml
source:
  type: json-schema
  config:
    path: /schemas
    platform: schemaregistry
    dataset_name_strategy: SCHEMA_ID
    dataset_name_replace_pattern:
      match: "https://dev.schema.example.com/"
      replace: ""
```

Example: use the schema `title` field as the display name:

```yaml
source:
  type: json-schema
  config:
    path: /schemas
    platform: schemaregistry
    dataset_name_strategy: TITLE
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.schema.json_schema.JsonSchemaSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/schema/json_schema.py)


:::tip Questions?

If you've got any questions on configuring ingestion for JSON Schemas, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
