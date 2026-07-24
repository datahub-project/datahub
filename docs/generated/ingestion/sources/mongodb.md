


# MongoDB

## Overview

Mongodb is a data platform used to store and query analytical or operational data. Learn more in the [official Mongodb documentation](https://www.mongodb.com/).

The DataHub integration for Mongodb covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures stateful deletion detection.

The connector also supports Amazon DocumentDB through its MongoDB-compatible API.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `mongodb`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Database. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |

### Overview

The `mongodb` module ingests metadata from Mongodb into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

#### Required Permissions

The ingestion user must have the `read` role on each database to be ingested, or
`readAnyDatabase` to ingest all accessible databases:

```js
// Grant read access to a specific database
db.grantRolesToUser("datahub_ingest", [{ role: "read", db: "your_database" }]);
// Or grant read access to all databases
db.grantRolesToUser("datahub_ingest", [
  { role: "readAnyDatabase", db: "admin" },
]);
```

:::tip Note on system collections

MongoDB `system.*` collections (such as `system.profile` and `system.views`) are excluded
from ingestion by default via the `excludeSystemCollections` option (enabled by default).
This means the `read` role is sufficient for all standard ingestion use cases. If you
disable `excludeSystemCollections`, ingestion of `system.profile` requires the `dbAdmin`
role — see [Config Details](#config-details) for more information.

If you are upgrading from a version that ingested system collections by default and have
MongoDB profiling enabled, you may encounter an authorization error during ingestion. As a
workaround, you can explicitly deny system collections in your recipe:

```yaml
source:
  type: mongodb
  config:
    collection_pattern:
      deny:
        - ".*\\.system\\..*"
```

:::

#### Authentication

The `authMechanism` config field maps directly to PyMongo's
[authentication mechanisms](https://pymongo.readthedocs.io/en/stable/examples/authentication.html).
Supported values:

| `authMechanism` | Use Case                                                                                                                                                                                 | Required Fields                            |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `DEFAULT`       | SCRAM auth (MongoDB default). Automatically negotiates SCRAM-SHA-256 or SCRAM-SHA-1 based on server version.                                                                             | `username`, `password`                     |
| `SCRAM-SHA-256` | Explicit SCRAM-SHA-256 (MongoDB 4.0+).                                                                                                                                                   | `username`, `password`                     |
| `SCRAM-SHA-1`   | Explicit SCRAM-SHA-1.                                                                                                                                                                    | `username`, `password`                     |
| `MONGODB-AWS`   | AWS IAM authentication for MongoDB Atlas or AWS DocumentDB. Credentials are resolved from the environment (env vars, EC2 instance profile, ECS task role, EKS pod identity) via `boto3`. | See below                                  |
| `MONGODB-X509`  | X.509 certificate authentication.                                                                                                                                                        | TLS options via `connect_uri` or `options` |

##### Username/Password (DEFAULT, SCRAM)

The simplest configuration - supply `username` and `password` directly:

```yaml
source:
  type: mongodb
  config:
    connect_uri: "mongodb://host:27017"
    username: "${MONGODB_USER}"
    password: "${MONGODB_PASSWORD}"
    authMechanism: "DEFAULT"
```

##### AWS IAM Authentication (MONGODB-AWS)

Set `authMechanism: "MONGODB-AWS"` to authenticate using AWS IAM
credentials. The connector resolves credentials automatically via
`boto3`'s standard credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_SESSION_TOKEN`)
2. Shared credentials/config files (`~/.aws/credentials`)
3. EC2 instance metadata / instance profile
4. ECS container credentials
5. EKS Pod Identity / IRSA (IAM Roles for Service Accounts)

```yaml
source:
  type: mongodb
  config:
    connect_uri: "mongodb+srv://cluster.example.mongodb.net"
    authMechanism: "MONGODB-AWS"
    hostingEnvironment: "ATLAS" # or "AWS_DOCUMENTDB"
```

To supply explicit AWS credentials instead of using the credential chain:

```yaml
source:
  type: mongodb
  config:
    connect_uri: "mongodb://docdb-cluster.region.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/path/to/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    username: "${AWS_ACCESS_KEY_ID}"
    password: "${AWS_SECRET_ACCESS_KEY}"
    authMechanism: "MONGODB-AWS"
    hostingEnvironment: "AWS_DOCUMENTDB"
    platform: documentdb
```


### Install the Plugin
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
    # See the connector docs for all supported authMechanism values,
    # including MONGODB-AWS for IAM authentication.
    username: "${MONGODB_USER}"
    password: "${MONGODB_PASSWORD}"
    authMechanism: "DEFAULT"

    # Options
    enableSchemaInference: True
    useRandomSampling: True
    maxSchemaSize: 300

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">authMechanism</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | MongoDB authentication mechanism. Supported values: DEFAULT, SCRAM-SHA-1, SCRAM-SHA-256, MONGODB-AWS, MONGODB-X509. When using MONGODB-AWS, credentials are resolved via boto3. See https://pymongo.readthedocs.io/en/stable/examples/authentication.html <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | MongoDB connection URI. <div className="default-line default-line-with-docs">Default: <span className="default-value">mongodb://localhost</span></div> |
| <div className="path-line"><span className="path-main">enableSchemaInference</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to infer schemas.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">excludeSystemCollections</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to exclude MongoDB system collections (those starting with 'system.') from ingestion. System collections such as system.profile and system.views are internal MongoDB collections that do not contain user data. Ingesting them produces noisy or incorrect metadata. Set to False only if you explicitly need to ingest system collections and have granted the appropriate database roles (dbAdmin for system.profile). Default: True. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">hostingEnvironment</span></div> <div className="type-name-line"><span className="type-name">One of Enum, null</span></div> | Hosting environment of MongoDB, default is SELF_HOSTED, currently support `SELF_HOSTED`, `ATLAS`, `AWS_DOCUMENTDB` <div className="default-line default-line-with-docs">Default: <span className="default-value">SELF&#95;HOSTED</span></div> |
| <div className="path-line"><span className="path-main">maxDocumentSize</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> |  <div className="default-line ">Default: <span className="default-value">16793600</span></div> |
| <div className="path-line"><span className="path-main">maxSchemaSize</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum number of fields to include in the schema. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Additional options to pass to `pymongo.MongoClient()`. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | MongoDB password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "mongodb", "documentdb" <div className="default-line default-line-with-docs">Default: <span className="default-value">mongodb</span></div> |
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
          "format": "password",
          "type": "string",
          "writeOnly": true
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
      "description": "MongoDB authentication mechanism. Supported values: DEFAULT, SCRAM-SHA-1, SCRAM-SHA-256, MONGODB-AWS, MONGODB-X509. When using MONGODB-AWS, credentials are resolved via boto3. See https://pymongo.readthedocs.io/en/stable/examples/authentication.html",
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
    "platform": {
      "default": "mongodb",
      "description": "Data platform to emit entities under. Use `documentdb` to surface AWS DocumentDB clusters as their own platform instead of `mongodb`. Requires `hostingEnvironment` to be `AWS_DOCUMENTDB`.",
      "enum": [
        "mongodb",
        "documentdb"
      ],
      "title": "Platform",
      "type": "string"
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
    },
    "excludeSystemCollections": {
      "default": true,
      "description": "Whether to exclude MongoDB system collections (those starting with 'system.') from ingestion. System collections such as system.profile and system.views are internal MongoDB collections that do not contain user data. Ingesting them produces noisy or incorrect metadata. Set to False only if you explicitly need to ingest system collections and have granted the appropriate database roles (dbAdmin for system.profile). Default: True.",
      "title": "Excludesystemcollections",
      "type": "boolean"
    }
  },
  "title": "MongoDBConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Emitting as the DocumentDB platform

By default the connector emits all ingested entities under the `mongodb` data platform, regardless of whether the underlying source is MongoDB or AWS DocumentDB. To surface DocumentDB clusters as their own platform, set `platform: documentdb`. This requires `hostingEnvironment` to be `AWS_DOCUMENTDB`; any other hosting environment is rejected at config validation time.

Switching an existing recipe to `platform: documentdb` will generate new `documentdb` dataset and container URNs; the previously emitted `mongodb` URNs will need to be cleaned up via stateful ingestion (if enabled) or manually soft-deleted.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.mongodb.MongoDBSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/mongodb.py)


:::tip Questions?

If you've got any questions on configuring ingestion for MongoDB, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
