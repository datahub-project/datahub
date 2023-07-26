---
sidebar_position: 29
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

| Capability      | Status | Notes              |
| --------------- | ------ | ------------------ |
| Schema Metadata | ✅     | Enabled by default |

This plugin extracts the following:

- Databases and associated metadata
- Collections in each database and schemas for each collection (via schema inference)

By default, schema inference samples 1,000 documents from each collection. Setting `schemaSamplingSize: null` will scan the entire collection.
Moreover, setting `useRandomSampling: False` will sample the first documents found without random selection, which may be faster for large collections.

Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

Really large schemas will be further truncated to a maximum of 300 schema fields. This is configurable using the `maxSchemaSize` parameter.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[mongodb]'
```

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

| Field                                                                                                                                                                                                                    | Description                                                                                                                                                                                                                                                                            |
| :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">authMechanism</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                        | MongoDB authentication mechanism.                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                          | MongoDB connection URI. <div className="default-line default-line-with-docs">Default: <span className="default-value">mongodb://localhost</span></div>                                                                                                                                 |
| <div className="path-line"><span className="path-main">enableSchemaInference</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                               | Whether to infer schemas. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                              |
| <div className="path-line"><span className="path-main">maxDocumentSize</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                     | <div className="default-line ">Default: <span className="default-value">16793600</span></div>                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">maxSchemaSize</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                       | Maximum number of fields to include in the schema. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div>                                                                                                                      |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                              | Additional options to pass to `pymongo.MongoClient()`. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                         |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                             | MongoDB password.                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">schemaSamplingSize</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                  | Number of documents to use when inferring schema size. If set to `0`, all documents will be scanned. <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div>                                                                   |
| <div className="path-line"><span className="path-main">useRandomSampling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                   | If documents for schema inference should be randomly selected. If `False`, documents will be selected from start. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                      |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                             | MongoDB username.                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                  | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                   |
| <div className="path-line"><span className="path-main">collection_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                         | regex patterns for collections to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">collection_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div> |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">collection_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>  |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">collection_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>  | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                            |
| <div className="path-line"><span className="path-main">database_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                           | regex patterns for databases to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>   |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>   |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>    |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>    | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                            |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "MongoDBConfig",
  "description": "Any source that produces dataset urns in a single environment should inherit this class",
  "type": "object",
  "properties": {
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "connect_uri": {
      "title": "Connect Uri",
      "description": "MongoDB connection URI.",
      "default": "mongodb://localhost",
      "type": "string"
    },
    "username": {
      "title": "Username",
      "description": "MongoDB username.",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "MongoDB password.",
      "type": "string"
    },
    "authMechanism": {
      "title": "Authmechanism",
      "description": "MongoDB authentication mechanism.",
      "type": "string"
    },
    "options": {
      "title": "Options",
      "description": "Additional options to pass to `pymongo.MongoClient()`.",
      "default": {},
      "type": "object"
    },
    "enableSchemaInference": {
      "title": "Enableschemainference",
      "description": "Whether to infer schemas. ",
      "default": true,
      "type": "boolean"
    },
    "schemaSamplingSize": {
      "title": "Schemasamplingsize",
      "description": "Number of documents to use when inferring schema size. If set to `0`, all documents will be scanned.",
      "default": 1000,
      "exclusiveMinimum": 0,
      "type": "integer"
    },
    "useRandomSampling": {
      "title": "Userandomsampling",
      "description": "If documents for schema inference should be randomly selected. If `False`, documents will be selected from start.",
      "default": true,
      "type": "boolean"
    },
    "maxSchemaSize": {
      "title": "Maxschemasize",
      "description": "Maximum number of fields to include in the schema.",
      "default": 300,
      "exclusiveMinimum": 0,
      "type": "integer"
    },
    "maxDocumentSize": {
      "title": "Maxdocumentsize",
      "default": 16793600,
      "exclusiveMinimum": 0,
      "type": "integer"
    },
    "database_pattern": {
      "title": "Database Pattern",
      "description": "regex patterns for databases to filter in ingestion.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "collection_pattern": {
      "title": "Collection Pattern",
      "description": "regex patterns for collections to filter in ingestion.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    }
  },
  "additionalProperties": false,
  "definitions": {
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.mongodb.MongoDBSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/mongodb.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for MongoDB, feel free to ping us on [our Slack](https://slack.datahubproject.io).
