---
sidebar_position: 20
title: JSON Schemas
slug: /generated/ingestion/sources/json-schema
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/json-schema.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# JSON Schemas

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                                                                                                          |
| ---------------------------------------------------------------------------------------------------------- | ------ | -------------------------------------------------------------------------------------------------------------- |
| Descriptions                                                                                               | ✅     | Extracts descriptions at top level and field level                                                             |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | With stateful ingestion enabled, will remove entities from DataHub if they are no longer present in the source |
| Extract Ownership                                                                                          | ❌     | Does not currently support extracting ownership                                                                |
| Extract Tags                                                                                               | ❌     | Does not currently support extracting tags                                                                     |
| [Platform Instance](../../../platform-instances.md)                                                        | ✅     | Supports platform instance via config                                                                          |
| Schema Metadata                                                                                            | ✅     | Extracts schemas, following references                                                                         |

This source extracts metadata from a single JSON Schema or multiple JSON Schemas rooted at a particular path.
It performs reference resolution based on the `$ref` keyword.

Metadata mapping:

- Schemas are mapped to Datasets with sub-type Schema
- The name of the Schema (Dataset) is inferred from the `$id` property and if that is missing, the file name.
- Browse paths are minted based on the path

### CLI based Ingestion

#### Install the Plugin

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

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                                                    | Description                                                                                                                                                                                                                                  |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">path</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">One of string(file-path), string(directory-path), string(uri)</span></div>                                    | Set this to a single file-path or a directory-path (for recursive traversal) or a remote url. e.g. https://json.schemastore.org/petstore-v1.0.json                                                                                           |
| <div className="path-line"><span className="path-main">platform</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                       | Set this to a platform that you want all schemas to live under. e.g. schemaregistry / schemarepo etc.                                                                                                                                        |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                    | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                               |
| <div className="path-line"><span className="path-main">use_id_as_base_uri</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                  | When enabled, uses the `$id` field in the json schema as the base uri for following references. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                             |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                  | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                         |
| <div className="path-line"><span className="path-main">uri_replace_pattern</span></div> <div className="type-name-line"><span className="type-name">URIReplacePattern</span></div>                                                                                                       | Use this if URI-s need to be modified during reference resolution. Simple string match - replace capabilities are supported.                                                                                                                 |
| <div className="path-line"><span className="path-prefix">uri_replace_pattern.</span><span className="path-main">match</span>&nbsp;<abbr title="Required if uri_replace_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>   | Pattern to match on uri-s as part of reference resolution. See replace field                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">uri_replace_pattern.</span><span className="path-main">replace</span>&nbsp;<abbr title="Required if uri_replace_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern to replace with as part of reference resolution. See match field                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                                                                       | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                     | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                           |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                       | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "JsonSchemaSourceConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
  "type": "object",
  "properties": {
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "stateful_ingestion": {
      "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
    },
    "path": {
      "title": "Path",
      "description": "Set this to a single file-path or a directory-path (for recursive traversal) or a remote url. e.g. https://json.schemastore.org/petstore-v1.0.json",
      "anyOf": [
        {
          "type": "string",
          "format": "file-path"
        },
        {
          "type": "string",
          "format": "directory-path"
        },
        {
          "type": "string",
          "minLength": 1,
          "maxLength": 65536,
          "format": "uri"
        }
      ]
    },
    "platform": {
      "title": "Platform",
      "description": "Set this to a platform that you want all schemas to live under. e.g. schemaregistry / schemarepo etc.",
      "type": "string"
    },
    "use_id_as_base_uri": {
      "title": "Use Id As Base Uri",
      "description": "When enabled, uses the `$id` field in the json schema as the base uri for following references.",
      "default": false,
      "type": "boolean"
    },
    "uri_replace_pattern": {
      "title": "Uri Replace Pattern",
      "description": "Use this if URI-s need to be modified during reference resolution. Simple string match - replace capabilities are supported.",
      "allOf": [
        {
          "$ref": "#/definitions/URIReplacePattern"
        }
      ]
    }
  },
  "required": [
    "path",
    "platform"
  ],
  "additionalProperties": false,
  "definitions": {
    "DynamicTypedStateProviderConfig": {
      "title": "DynamicTypedStateProviderConfig",
      "type": "object",
      "properties": {
        "type": {
          "title": "Type",
          "description": "The type of the state provider to use. For DataHub use `datahub`",
          "type": "string"
        },
        "config": {
          "title": "Config",
          "description": "The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19)."
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": false
    },
    "StatefulStaleMetadataRemovalConfig": {
      "title": "StatefulStaleMetadataRemovalConfig",
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "The type of the ingestion state provider registered with datahub.",
          "default": false,
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "title": "Remove Stale Metadata",
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "URIReplacePattern": {
      "title": "URIReplacePattern",
      "type": "object",
      "properties": {
        "match": {
          "title": "Match",
          "description": "Pattern to match on uri-s as part of reference resolution. See replace field",
          "type": "string"
        },
        "replace": {
          "title": "Replace",
          "description": "Pattern to replace with as part of reference resolution. See match field",
          "type": "string"
        }
      },
      "required": [
        "match",
        "replace"
      ],
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

#### Configuration Notes

- You must provide a `platform` field. Most organizations have custom project names for their schema repositories, so you can pick whatever name makes sense. For example, you might want to call your schema platform **schemaregistry**. After picking a custom platform, you can use the [put platform](../../../../docs/cli.md#put-platform) command to register your custom platform into DataHub.

### Code Coordinates

- Class Name: `datahub.ingestion.source.schema.json_schema.JsonSchemaSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/schema/json_schema.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for JSON Schemas, feel free to ping us on [our Slack](https://slack.datahubproject.io).
