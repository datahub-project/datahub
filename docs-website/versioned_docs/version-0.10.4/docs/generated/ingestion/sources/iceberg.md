---
sidebar_position: 19
title: Iceberg
slug: /generated/ingestion/sources/iceberg
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/iceberg.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Iceberg

![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                                                                                                             |
| ---------------------------------------------------------------------------------------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------- |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md)                           | ✅     | Optionally enabled via configuration.                                                                             |
| Descriptions                                                                                               | ✅     | Enabled by default.                                                                                               |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Enabled via stateful ingestion                                                                                    |
| [Domains](../../../domains.md)                                                                             | ❌     | Currently not supported.                                                                                          |
| Extract Ownership                                                                                          | ✅     | Optionally enabled via configuration by specifying which Iceberg table property holds user or group ownership.    |
| Partition Support                                                                                          | ❌     | Currently not supported.                                                                                          |
| [Platform Instance](../../../platform-instances.md)                                                        | ✅     | Optionally enabled via configuration, an Iceberg instance represents the datalake name where the table is stored. |

## Integration Details

The DataHub Iceberg source plugin extracts metadata from [Iceberg tables](https://iceberg.apache.org/spec/) stored in a distributed or local file system.
Typically, Iceberg tables are stored in a distributed file system like S3 or Azure Data Lake Storage (ADLS) and registered in a catalog. There are various catalog
implementations like Filesystem-based, RDBMS-based or even REST-based catalogs. This Iceberg source plugin relies on the
[Iceberg python_legacy library](https://github.com/apache/iceberg/tree/master/python_legacy) and its support for catalogs is limited at the moment.
A new version of the [Iceberg Python library](https://github.com/apache/iceberg/tree/master/python) is currently in development and should fix this.
Because of this limitation, this source plugin **will only ingest HadoopCatalog-based tables that have a `version-hint.text` metadata file**.

Ingestion of tables happens in 2 steps:

1. Discover Iceberg tables stored in file system.
2. Load discovered tables using Iceberg python_legacy library

The current implementation of the Iceberg source plugin will only discover tables stored in a local file system or in ADLS. Support for S3 could
be added fairly easily.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[iceberg]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: "iceberg"
  config:
    env: PROD
    adls:
      # Will be translated to https://{account_name}.dfs.core.windows.net
      account_name: my_adls_account
      # Can use sas_token or account_key
      sas_token: "${SAS_TOKEN}"
      # account_key: "${ACCOUNT_KEY}"
      container_name: warehouse
      base_path: iceberg
    platform_instance: my_iceberg_catalog
    table_pattern:
      allow:
        - marketing.*
    profiling:
      enabled: true

sink:
  # sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                             | Description                                                                                                                                                                                                                                                                       |
| :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">group_ownership_property</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                      | Iceberg table property to look for a `CorpGroup` owner. Can only hold a single group value. If property has no value, no owner information will be emitted.                                                                                                                       |
| <div className="path-line"><span className="path-main">localfs</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                       | Local path to crawl for Iceberg tables. This is one filesystem type supported by this source and **only one can be configured**.                                                                                                                                                  |
| <div className="path-line"><span className="path-main">max_path_depth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                                               | Maximum folder depth to crawl for Iceberg tables. Folders deeper than this value will be silently ignored. <div className="default-line default-line-with-docs">Default: <span className="default-value">2</span></div>                                                           |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                             | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">user_ownership_property</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                       | Iceberg table property to look for a `CorpUser` owner. Can only hold a single user value. If property has no value, no owner information will be emitted. <div className="default-line default-line-with-docs">Default: <span className="default-value">owner</span></div>        |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                           | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                              |
| <div className="path-line"><span className="path-main">adls</span></div> <div className="type-name-line"><span className="type-name">AdlsSourceConfig</span></div>                                                                                                | [Azure Data Lake Storage](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) to crawl for Iceberg tables. This is one filesystem type supported by this source and **only one can be configured**.                                              |
| <div className="path-line"><span className="path-prefix">adls.</span><span className="path-main">account_name</span>&nbsp;<abbr title="Required if adls is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>   | Name of the Azure storage account. See [Microsoft official documentation on how to create a storage account.](https://docs.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)                                                                              |
| <div className="path-line"><span className="path-prefix">adls.</span><span className="path-main">container_name</span>&nbsp;<abbr title="Required if adls is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Azure storage account container name.                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">adls.</span><span className="path-main">account_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                         | Azure storage account access key that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**                                                                                                                             |
| <div className="path-line"><span className="path-prefix">adls.</span><span className="path-main">base_path</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                           | Base folder in hierarchical namespaces to start from. <div className="default-line default-line-with-docs">Default: <span className="default-value">/</span></div>                                                                                                                |
| <div className="path-line"><span className="path-prefix">adls.</span><span className="path-main">client_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                           | Azure client (Application) ID required when a `client_secret` is used as a credential.                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">adls.</span><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | Azure client secret that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">adls.</span><span className="path-main">sas_token</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                           | Azure storage account Shared Access Signature (SAS) token that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**                                                                                                    |
| <div className="path-line"><span className="path-prefix">adls.</span><span className="path-main">tenant_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                           | Azure tenant (Directory) ID required when a `client_secret` is used as a credential.                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                       | Regex patterns for tables to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                               |                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                |                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                       |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">IcebergProfilingConfig</span></div>                                                                                     | <div className="default-line ">Default: <span className="default-value">&#123;&#x27;enabled&#x27;: False, &#x27;include_field_null_count&#x27;: Tru...</span></div>                                                                                                               |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                       | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                       | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                          |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                       | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                          |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                      | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                       |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                                                | Iceberg Stateful Ingestion Config.                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                              | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                      |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "IcebergSourceConfig",
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
      "title": "Stateful Ingestion",
      "description": "Iceberg Stateful Ingestion Config.",
      "allOf": [
        {
          "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
        }
      ]
    },
    "adls": {
      "title": "Adls",
      "description": "[Azure Data Lake Storage](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) to crawl for Iceberg tables.  This is one filesystem type supported by this source and **only one can be configured**.",
      "allOf": [
        {
          "$ref": "#/definitions/AdlsSourceConfig"
        }
      ]
    },
    "localfs": {
      "title": "Localfs",
      "description": "Local path to crawl for Iceberg tables. This is one filesystem type supported by this source and **only one can be configured**.",
      "type": "string"
    },
    "max_path_depth": {
      "title": "Max Path Depth",
      "description": "Maximum folder depth to crawl for Iceberg tables.  Folders deeper than this value will be silently ignored.",
      "default": 2,
      "type": "integer"
    },
    "table_pattern": {
      "title": "Table Pattern",
      "description": "Regex patterns for tables to filter in ingestion.",
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
    "user_ownership_property": {
      "title": "User Ownership Property",
      "description": "Iceberg table property to look for a `CorpUser` owner.  Can only hold a single user value.  If property has no value, no owner information will be emitted.",
      "default": "owner",
      "type": "string"
    },
    "group_ownership_property": {
      "title": "Group Ownership Property",
      "description": "Iceberg table property to look for a `CorpGroup` owner.  Can only hold a single group value.  If property has no value, no owner information will be emitted.",
      "type": "string"
    },
    "profiling": {
      "title": "Profiling",
      "default": {
        "enabled": false,
        "include_field_null_count": true,
        "include_field_min_value": true,
        "include_field_max_value": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/IcebergProfilingConfig"
        }
      ]
    }
  },
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
    "AdlsSourceConfig": {
      "title": "AdlsSourceConfig",
      "description": "Common Azure credentials config.\n\nhttps://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python",
      "type": "object",
      "properties": {
        "base_path": {
          "title": "Base Path",
          "description": "Base folder in hierarchical namespaces to start from.",
          "default": "/",
          "type": "string"
        },
        "container_name": {
          "title": "Container Name",
          "description": "Azure storage account container name.",
          "type": "string"
        },
        "account_name": {
          "title": "Account Name",
          "description": "Name of the Azure storage account.  See [Microsoft official documentation on how to create a storage account.](https://docs.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)",
          "type": "string"
        },
        "account_key": {
          "title": "Account Key",
          "description": "Azure storage account access key that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
          "type": "string"
        },
        "sas_token": {
          "title": "Sas Token",
          "description": "Azure storage account Shared Access Signature (SAS) token that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
          "type": "string"
        },
        "client_secret": {
          "title": "Client Secret",
          "description": "Azure client secret that can be used as a credential. **An account key, a SAS token or a client secret is required for authentication.**",
          "type": "string"
        },
        "client_id": {
          "title": "Client Id",
          "description": "Azure client (Application) ID required when a `client_secret` is used as a credential.",
          "type": "string"
        },
        "tenant_id": {
          "title": "Tenant Id",
          "description": "Azure tenant (Directory) ID required when a `client_secret` is used as a credential.",
          "type": "string"
        }
      },
      "required": [
        "container_name",
        "account_name"
      ],
      "additionalProperties": false
    },
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
    },
    "IcebergProfilingConfig": {
      "title": "IcebergProfilingConfig",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "Whether profiling should be done.",
          "default": false,
          "type": "boolean"
        },
        "include_field_null_count": {
          "title": "Include Field Null Count",
          "description": "Whether to profile for the number of nulls for each column.",
          "default": true,
          "type": "boolean"
        },
        "include_field_min_value": {
          "title": "Include Field Min Value",
          "description": "Whether to profile for the min value of numeric columns.",
          "default": true,
          "type": "boolean"
        },
        "include_field_max_value": {
          "title": "Include Field Max Value",
          "description": "Whether to profile for the max value of numeric columns.",
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

### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrelevant DataHub Concepts -->

| Source Concept                                                                                                                          | DataHub Concept                                                        | Notes                                                                                                                                                                                                                                                                                                          |
| --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `iceberg`                                                                                                                               | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md)     |                                                                                                                                                                                                                                                                                                                |
| Table                                                                                                                                   | [Dataset](docs/generated/metamodel/entities/dataset.md)                | Each Iceberg table maps to a Dataset named using the parent folders. If a table is stored under `my/namespace/table`, the dataset name will be `my.namespace.table`. If a [Platform Instance](/docs/platform-instances/) is configured, it will be used as a prefix: `<platform_instance>.my.namespace.table`. |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties)                                                | [User (a.k.a CorpUser)](docs/generated/metamodel/entities/corpuser.md) | The value of a table property can be used as the name of a CorpUser owner. This table property name can be configured with the source option `user_ownership_property`.                                                                                                                                        |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties)                                                | CorpGroup                                                              | The value of a table property can be used as the name of a CorpGroup owner. This table property name can be configured with the source option `group_ownership_property`.                                                                                                                                      |
| Table parent folders (excluding [warehouse catalog location](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties)) | Container                                                              | Available in a future release                                                                                                                                                                                                                                                                                  |
| [Table schema](https://iceberg.apache.org/spec/#schemas-and-data-types)                                                                 | SchemaField                                                            | Maps to the fields defined within the Iceberg table schema definition.                                                                                                                                                                                                                                         |

## Troubleshooting

### [Common Issue]

[Provide description of common issues with this integration and steps to resolve]

### Code Coordinates

- Class Name: `datahub.ingestion.source.iceberg.iceberg.IcebergSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/iceberg/iceberg.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Iceberg, feel free to ping us on [our Slack](https://slack.datahubproject.io).
