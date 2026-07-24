


# Metabase

## Overview

Metabase is a business intelligence and analytics platform. Learn more in the [official Metabase documentation](https://www.metabase.com/).

The DataHub integration for Metabase covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table-level lineage and stateful deletion detection.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `metabase`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Supported by default. |

### Overview

The `metabase` module ingests metadata from Metabase into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

#### Compatibility

Metabase version [v0.48.3](https://www.metabase.com/start/oss/)

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.


### Install the Plugin
```shell
pip install 'acryl-datahub[metabase]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: metabase
  config:
    connect_uri: "http://localhost:3000"
    username: "user@example.com"
    password: "${METABASE_PASSWORD}"

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Metabase API key. If provided, the username and password will be ignored. Recommended method. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Metabase host URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">localhost:3000</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">database_alias_map</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | Database name map to use when constructing dataset URN. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">database_id_to_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Custom mappings between metabase database id and DataHub platform instance <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Default schema name to use when schema is not provided in an SQL query <div className="default-line default-line-with-docs">Default: <span className="default-value">public</span></div> |
| <div className="path-line"><span className="path-main">display_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | optional URL to use in links (if `connect_uri` is only for ingestion) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">engine_platform_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Custom mappings between metabase database engines and DataHub platforms <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">exclude_other_user_collections</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Flag that if true, exclude other user collections <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Metabase password, used when an API key is not provided. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A holder for platform -> platform_instance mappings to generate correct dataset urns <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Metabase username, used when an API key is not provided. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
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
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
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
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance_map": {
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
      "description": "A holder for platform -> platform_instance mappings to generate correct dataset urns",
      "title": "Platform Instance Map"
    },
    "connect_uri": {
      "default": "localhost:3000",
      "description": "Metabase host URL.",
      "title": "Connect Uri",
      "type": "string"
    },
    "display_uri": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "optional URL to use in links (if `connect_uri` is only for ingestion)",
      "title": "Display Uri"
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
      "description": "Metabase username, used when an API key is not provided.",
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
      "description": "Metabase password, used when an API key is not provided.",
      "title": "Password"
    },
    "api_key": {
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
      "description": "Metabase API key. If provided, the username and password will be ignored. Recommended method.",
      "title": "Api Key"
    },
    "database_alias_map": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Database name map to use when constructing dataset URN.",
      "title": "Database Alias Map"
    },
    "engine_platform_map": {
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
      "description": "Custom mappings between metabase database engines and DataHub platforms",
      "title": "Engine Platform Map"
    },
    "database_id_to_instance_map": {
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
      "description": "Custom mappings between metabase database id and DataHub platform instance",
      "title": "Database Id To Instance Map"
    },
    "default_schema": {
      "default": "public",
      "description": "Default schema name to use when schema is not provided in an SQL query",
      "title": "Default Schema",
      "type": "string"
    },
    "exclude_other_user_collections": {
      "default": false,
      "description": "Flag that if true, exclude other user collections",
      "title": "Exclude Other User Collections",
      "type": "boolean"
    }
  },
  "title": "MetabaseConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Collection

The connector uses Metabase collection APIs to discover collection hierarchy and list dashboards within each collection:

- [`/api/collection`](https://www.metabase.com/docs/latest/api/collection)
- [`/api/collection/{id}/items?models=dashboard`](https://www.metabase.com/docs/latest/api/collection#get-apicollectioniditems)

#### Dashboard

Dashboard details are extracted from:

- [`/api/dashboard/{id}`](https://www.metabase.com/docs/latest/api/dashboard)

This includes titles, descriptions, ownership context, and dashboard URL metadata.

#### Chart

Chart metadata is extracted from:

- [`/api/card/{id}`](https://www.metabase.com/docs/latest/api-documentation.html#card)

This captures chart definitions, query relationships, and chart-level attributes used for DataHub Chart entities.

#### Database Mapping

Metabase databases will be mapped to a DataHub platform based on the engine listed in the
[api/database](https://www.metabase.com/docs/latest/api-documentation.html#database) response. This mapping can be
customized by using the `engine_platform_map` config option. For example, to map databases using the `athena` engine to
the underlying datasets in the `glue` platform, the following snippet can be used:

```yml
engine_platform_map:
  athena: glue
```

DataHub will try to determine database name from Metabase [api/database](https://www.metabase.com/docs/latest/api-documentation.html#database)
payload. However, the name can be overridden from `database_alias_map` for a given database connected to Metabase.

If several platform instances with the same platform (e.g. from several distinct clickhouse clusters) are present in DataHub,
the mapping between database id in Metabase and platform instance in DataHub may be configured with the following map:

```yml
database_id_to_instance_map:
  "42": platform_instance_in_datahub
```

The key in this map must be string, not integer although Metabase API provides `id` as number.
If `database_id_to_instance_map` is not specified, `platform_instance_map` is used for platform instance mapping. If none of the above are specified, platform instance is not used when constructing `urn` when searching for dataset relations.

If needed it is possible to exclude collections from other users by setting the following configuration:

```yaml
exclude_other_user_collections: true
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.metabase.MetabaseSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/metabase.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Metabase, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
