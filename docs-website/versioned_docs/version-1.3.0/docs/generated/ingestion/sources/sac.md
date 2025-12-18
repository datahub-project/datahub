---
sidebar_position: 63
title: SAP Analytics Cloud
slug: /generated/ingestion/sources/sac
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/sac.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SAP Analytics Cloud
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default (only for Import Data Models). |
| Table-Level Lineage | ✅ | Enabled by default (only for Live Data Models). |


## Configuration Notes

1. Refer to [Manage OAuth Clients](https://help.sap.com/docs/SAP_ANALYTICS_CLOUD/00f68c2e08b941f081002fd3691d86a7/4f43b54398fc4acaa5efa32badfe3df6.html) to create an OAuth client in SAP Analytics Cloud. The OAuth client is required to have the following properties:

   - Purpose: API Access
   - Access:
     - Story Listing
     - Data Import Service
   - Authorization Grant: Client Credentials

2. Maintain connection mappings (optional):

To map individual connections in SAP Analytics Cloud to platforms, platform instances and environments, the `connection_mapping` configuration can be used within the recipe:

```yaml
connection_mapping:
  MY_BW_CONNECTION:
    platform: bw
    platform_instance: PROD_BW
    env: PROD
  MY_HANA_CONNECTION:
    platform: hana
    platform_instance: PROD_HANA
    env: PROD
```

The key in the connection mapping dictionary represents the name of the connection created in SAP Analytics Cloud.

## Concept mapping

| SAP Analytics Cloud | DataHub     |
| ------------------- | ----------- |
| `Story`             | `Dashboard` |
| `Application`       | `Dashboard` |
| `Live Data Model`   | `Dataset`   |
| `Import Data Model` | `Dataset`   |
| `Model`             | `Dataset`   |

## Limitations

- Only models which are used in a Story or an Application will be ingested because there is no dedicated API to retrieve models (only for Stories and Applications).
- Browse Paths for models cannot be created because the folder where the models are saved is not returned by the API.
- Schema metadata is only ingested for Import Data Models because there is no possibility to get the schema metadata of the other model types.
- Lineages for Import Data Models cannot be ingested because the API is not providing any information about it.
- Currently, only SAP BW and SAP HANA are supported for ingesting the upstream lineages of Live Data Models - a warning is logged for all other connection types, please feel free to open an [issue on GitHub](https://github.com/datahub-project/datahub/issues/new/choose) with the warning message to have this fixed.
- For some models (e.g., builtin models) it cannot be detected whether the models are Live Data or Import Data Models. Therefore, these models will be ingested only with the `Story` subtype.

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
    type: sac
    config:
        stateful_ingestion:
            enabled: true

        tenant_url: # Your SAP Analytics Cloud tenant URL, e.g. https://company.eu10.sapanalytics.cloud or https://company.eu10.hcs.cloud.sap
        token_url: # The Token URL of your SAP Analytics Cloud tenant, e.g. https://company.eu10.hana.ondemand.com/oauth/token.

        # Add secret in Secrets Tab with relevant names for each variable
        client_id: "${SAC_CLIENT_ID}" # Your SAP Analytics Cloud client id
        client_secret: "${SAC_CLIENT_SECRET}" # Your SAP Analytics Cloud client secret

        # ingest stories
        ingest_stories: true

        # ingest applications
        ingest_applications: true

        resource_id_pattern:
            allow:
                - .*

        resource_name_pattern:
            allow:
                - .*

        folder_pattern:
            allow:
                - .*

        connection_mapping:
            MY_BW_CONNECTION:
                platform: bw
                platform_instance: PROD_BW
                env: PROD
            MY_HANA_CONNECTION:
                platform: hana
                platform_instance: PROD_HANA
                env: PROD

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">client_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Client ID for the OAuth authentication  |
| <div className="path-line"><span className="path-main">client_secret</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Client secret for the OAuth authentication  |
| <div className="path-line"><span className="path-main">tenant_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | URL of the SAP Analytics Cloud tenant  |
| <div className="path-line"><span className="path-main">token_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | URL of the OAuth token endpoint of the SAP Analytics Cloud tenant  |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_applications</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Controls whether Analytic Applications should be ingested <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_import_data_model_schema_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Controls whether schema metadata of Import Data Models should be ingested (ingesting schema metadata of Import Data Models significantly increases overall ingestion time) <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_stories</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Controls whether Stories should be ingested <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">query_name_template</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Template for generating dataset urns of consumed queries, the placeholder {query} can be used within the template for inserting the name of the query <div className="default-line default-line-with-docs">Default: <span className="default-value">QUERY/&#123;name&#125;</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">connection_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,ConnectionMappingConfig)</span></div> |   |
| <div className="path-line"><span className="path-prefix">connection_mapping.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that this connection mapping belongs to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">connection_mapping.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The platform that this connection mapping belongs to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_mapping.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that this connection mapping belongs to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">folder_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">folder_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">resource_id_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">resource_id_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">resource_name_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">resource_name_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion related configs <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "ConnectionMappingConfig": {
      "additionalProperties": false,
      "properties": {
        "env": {
          "default": "PROD",
          "description": "The environment that this connection mapping belongs to",
          "title": "Env",
          "type": "string"
        },
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The platform that this connection mapping belongs to",
          "title": "Platform"
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
          "description": "The instance of the platform that this connection mapping belongs to",
          "title": "Platform Instance"
        }
      },
      "title": "ConnectionMappingConfig",
      "type": "object"
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
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
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
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion related configs"
    },
    "tenant_url": {
      "description": "URL of the SAP Analytics Cloud tenant",
      "title": "Tenant Url",
      "type": "string"
    },
    "token_url": {
      "description": "URL of the OAuth token endpoint of the SAP Analytics Cloud tenant",
      "title": "Token Url",
      "type": "string"
    },
    "client_id": {
      "description": "Client ID for the OAuth authentication",
      "title": "Client Id",
      "type": "string"
    },
    "client_secret": {
      "description": "Client secret for the OAuth authentication",
      "format": "password",
      "title": "Client Secret",
      "type": "string",
      "writeOnly": true
    },
    "ingest_stories": {
      "default": true,
      "description": "Controls whether Stories should be ingested",
      "title": "Ingest Stories",
      "type": "boolean"
    },
    "ingest_applications": {
      "default": true,
      "description": "Controls whether Analytic Applications should be ingested",
      "title": "Ingest Applications",
      "type": "boolean"
    },
    "ingest_import_data_model_schema_metadata": {
      "default": true,
      "description": "Controls whether schema metadata of Import Data Models should be ingested (ingesting schema metadata of Import Data Models significantly increases overall ingestion time)",
      "title": "Ingest Import Data Model Schema Metadata",
      "type": "boolean"
    },
    "resource_id_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Patterns for selecting resource ids that are to be included"
    },
    "resource_name_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Patterns for selecting resource names that are to be included"
    },
    "folder_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Patterns for selecting folders that are to be included"
    },
    "connection_mapping": {
      "additionalProperties": {
        "$ref": "#/$defs/ConnectionMappingConfig"
      },
      "default": {},
      "description": "Custom mappings for connections",
      "title": "Connection Mapping",
      "type": "object"
    },
    "query_name_template": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "QUERY/{name}",
      "description": "Template for generating dataset urns of consumed queries, the placeholder {query} can be used within the template for inserting the name of the query",
      "title": "Query Name Template"
    }
  },
  "required": [
    "tenant_url",
    "token_url",
    "client_id",
    "client_secret"
  ],
  "title": "SACSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.sac.sac.SACSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sac/sac.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for SAP Analytics Cloud, feel free to ping us on [our Slack](https://datahub.com/slack).
