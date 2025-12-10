---
sidebar_position: 44
title: MongoDB
slug: /generated/ingestion/sources/mongodb
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/mongodb.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MongoDB
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Database. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |


This plugin extracts the following:

- Databases and associated metadata
- Collections in each database and schemas for each collection (via schema inference)

By default, schema inference samples 1,000 documents from each collection. Setting `schemaSamplingSize: null` will scan the entire collection.
Moreover, setting `useRandomSampling: False` will sample the first documents found without random selection, which may be faster for large collections.

Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

Really large schemas will be further truncated to a maximum of 300 schema fields. This is configurable using the `maxSchemaSize` parameter.



### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "mongodb"
  config:
    # Coordinates
    connect_uri: "mongodb://localhost"

    # Credentials
    username: admin
    password: password
    authMechanism: "DEFAULT"

    # Options
    enableSchemaInference: True
    useRandomSampling: True
    maxSchemaSize: 300

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
| <div className="path-line"><span className="path-main">authMechanism</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | MongoDB authentication mechanism. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | MongoDB connection URI. <div className="default-line default-line-with-docs">Default: <span className="default-value">mongodb://localhost</span></div> |
| <div className="path-line"><span className="path-main">enableSchemaInference</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to infer schemas.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">hostingEnvironment</span></div> <div className="type-name-line"><span className="type-name">One of Enum, null</span></div> | Hosting environment of MongoDB, default is SELF_HOSTED, currently support `SELF_HOSTED`, `ATLAS`, `AWS_DOCUMENTDB` <div className="default-line default-line-with-docs">Default: <span className="default-value">SELF&#95;HOSTED</span></div> |
| <div className="path-line"><span className="path-main">maxDocumentSize</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> |  <div className="default-line ">Default: <span className="default-value">16793600</span></div> |
| <div className="path-line"><span className="path-main">maxSchemaSize</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum number of fields to include in the schema. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Additional options to pass to `pymongo.MongoClient()`. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | MongoDB password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">schemaSamplingSize</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number of documents to use when inferring schema size. If set to `null`, all documents will be scanned. <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">useRandomSampling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If documents for schema inference should be randomly selected. If `False`, documents will be selected from start. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | MongoDB username. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">collection_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">collection_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">database_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
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
    "HostingEnvironment": {
      "enum": [
        "SELF_HOSTED",
        "ATLAS",
        "AWS_DOCUMENTDB"
      ],
      "title": "HostingEnvironment",
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
    }
  },
  "additionalProperties": false,
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
      "default": null
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
    "connect_uri": {
      "default": "mongodb://localhost",
      "description": "MongoDB connection URI.",
      "title": "Connect Uri",
      "type": "string"
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
      "description": "MongoDB username.",
      "title": "Username"
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
      "description": "MongoDB password.",
      "title": "Password"
    },
    "authMechanism": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "MongoDB authentication mechanism.",
      "title": "Authmechanism"
    },
    "options": {
      "additionalProperties": true,
      "default": {},
      "description": "Additional options to pass to `pymongo.MongoClient()`.",
      "title": "Options",
      "type": "object"
    },
    "enableSchemaInference": {
      "default": true,
      "description": "Whether to infer schemas. ",
      "title": "Enableschemainference",
      "type": "boolean"
    },
    "schemaSamplingSize": {
      "anyOf": [
        {
          "exclusiveMinimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 1000,
      "description": "Number of documents to use when inferring schema size. If set to `null`, all documents will be scanned.",
      "title": "Schemasamplingsize"
    },
    "useRandomSampling": {
      "default": true,
      "description": "If documents for schema inference should be randomly selected. If `False`, documents will be selected from start.",
      "title": "Userandomsampling",
      "type": "boolean"
    },
    "maxSchemaSize": {
      "anyOf": [
        {
          "exclusiveMinimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 300,
      "description": "Maximum number of fields to include in the schema.",
      "title": "Maxschemasize"
    },
    "maxDocumentSize": {
      "anyOf": [
        {
          "exclusiveMinimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 16793600,
      "description": "",
      "title": "Maxdocumentsize"
    },
    "hostingEnvironment": {
      "anyOf": [
        {
          "$ref": "#/$defs/HostingEnvironment"
        },
        {
          "type": "null"
        }
      ],
      "default": "SELF_HOSTED",
      "description": "Hosting environment of MongoDB, default is SELF_HOSTED, currently support `SELF_HOSTED`, `ATLAS`, `AWS_DOCUMENTDB`"
    },
    "database_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for databases to filter in ingestion."
    },
    "collection_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for collections to filter in ingestion."
    }
  },
  "title": "MongoDBConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.mongodb.MongoDBSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/mongodb.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for MongoDB, feel free to ping us on [our Slack](https://datahub.com/slack).
