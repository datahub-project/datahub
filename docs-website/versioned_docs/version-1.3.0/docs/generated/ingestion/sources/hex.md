---
sidebar_position: 29
title: Hex
slug: /generated/ingestion/sources/hex
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/hex.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Hex
This connector ingests [Hex](https://hex.tech/) assets into DataHub.

### Concept Mapping

| Hex Concept | DataHub Concept                                                                           | Notes               |
| ----------- | ----------------------------------------------------------------------------------------- | ------------------- |
| `"hex"`     | [Data Platform](/docs/generated/metamodel/entities/dataplatform/) |                     |
| Workspace   | [Container](/docs/generated/metamodel/entities/container/)        |                     |
| Project     | [Dashboard](/docs/generated/metamodel/entities/dashboard/)        | Subtype `Project`   |
| Component   | [Dashboard](/docs/generated/metamodel/entities/dashboard/)        | Subtype `Component` |
| Collection  | [Tag](/docs/generated/metamodel/entities/Tag/)                    |                     |

Other Hex concepts are not mapped to DataHub entities yet.

### Limitations

Currently, the [Hex API](https://learn.hex.tech/docs/api/api-reference) has some limitations that affect the completeness of the extracted metadata:

1. **Projects and Components Relationship**: The API does not support fetching the many-to-many relationship between Projects and their Components.

2. **Metadata Access**: There is no direct method to retrieve metadata for Collections, Status, or Categories. This information is only available indirectly through references within Projects and Components.

Please keep these limitations in mind when working with the Hex connector.

For the **Dataset - Hex Project lineage**, the connector relies on the
[_Hex query metadata_](https://learn.hex.tech/docs/explore-data/cells/sql-cells/sql-cells-introduction#query-metadata) feature.
Therefore, in order to extract lineage information, the required setup must include:

- A separated warehouse ingestor (_eg_ BigQuery, Snowflake, Redshift, ...) with `use_queries_v2` enabled in order to fetch Queries.
  This will ingest the queries into DataHub as `Query` entities and the ones triggered by Hex will include the corresponding _Hex query metadata_.
- A DataHub server with version >= SaaS `0.3.10` or > OSS `1.0.0` so the `Query` entities are properly indexed by source (Hex in this case) and so fetched and processed by the Hex ingestor in order to emit the Dataset - Project lineage.

Please note:

- Lineage is only captured for scheduled executions of the Project.
- In cases where queries are handled by [`hextoolkit`](https://learn.hex.tech/tutorials/connect-to-data/using-the-hextoolkit), _Hex query metadata_ is not injected, which prevents capturing lineage.
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| Dataset Usage | ✅ | Supported by default. Supported for types - Project. |
| Descriptions | ✅ | Supported by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Supported by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |


### Prerequisites

#### Workspace name

Workspace name is required to fetch the data from Hex. You can find the workspace name in the URL of your Hex home page.

```
https://app.hex.tech/<workspace_name>"
```

_Eg_: In https://app.hex.tech/acryl-partnership, `acryl-partnership` is the workspace name.

#### Authentication

To authenticate with Hex, you will need to provide your Hex API Bearer token.
You can obtain your API key by following the instructions on the [Hex documentation](https://learn.hex.tech/docs/api/api-overview).

Either PAT (Personal Access Token) or Workspace Token can be used as API Bearer token:

- (Recommended) If Workspace Token, a read-only token would be enough for ingestion.
- If PAT, ingestion will be done with the user's permissions.

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: hex
  config:
    workspace_name: # Hex workspace name. You can find this name in your Hex home page URL: https://app.hex.tech/<workspace_name>
    token: # Your PAT or Workspace token

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
| <div className="path-line"><span className="path-main">token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Hex API token; either PAT or Workflow token - https://learn.hex.tech/docs/api/api-overview#authentication  |
| <div className="path-line"><span className="path-main">workspace_name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Hex workspace name. You can find this name in your Hex home page URL: https://app.hex.tech/<workspace_name>  |
| <div className="path-line"><span className="path-main">base_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Hex API base URL. For most Hex users, this will be https://app.hex.tech/api/v1. Single-tenant app users should replace this with the URL they use to access Hex. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://app.hex.tech/api/v1</span></div> |
| <div className="path-line"><span className="path-main">categories_as_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit Hex Category as tags <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">collections_as_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit Hex Collections as tags <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">datahub_page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of items to fetch per DataHub API call. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">include_components</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include Hex Components in the ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include Hex lineage, being fetched from DataHub. See "Limitations" section in the docs for more details about the limitations of this feature. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">lineage_end_time</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), null</span></div> | Latest date of lineage to consider. Default: Current time in UTC. You can specify absolute time like '2023-01-01' or relative time like '-1 day' or '-1d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">lineage_start_time</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), null</span></div> | Earliest date of lineage to consider. Default: 1 day before lineage end time. You can specify absolute time like '2023-01-01' or relative time like '-7 days' or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of items to fetch per Hex API call. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">patch_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit metadata as patch events <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">set_ownership_from_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Set ownership identity from owner/creator email <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">status_as_tag</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit Hex Status as tags <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">component_title_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">component_title_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_title_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">project_title_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Configuration for stateful ingestion and stale metadata removal. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
      "description": "Configuration for stateful ingestion and stale metadata removal."
    },
    "workspace_name": {
      "description": "Hex workspace name. You can find this name in your Hex home page URL: https://app.hex.tech/<workspace_name>",
      "title": "Workspace Name",
      "type": "string"
    },
    "token": {
      "description": "Hex API token; either PAT or Workflow token - https://learn.hex.tech/docs/api/api-overview#authentication",
      "format": "password",
      "title": "Token",
      "type": "string",
      "writeOnly": true
    },
    "base_url": {
      "default": "https://app.hex.tech/api/v1",
      "description": "Hex API base URL. For most Hex users, this will be https://app.hex.tech/api/v1. Single-tenant app users should replace this with the URL they use to access Hex.",
      "title": "Base Url",
      "type": "string"
    },
    "include_components": {
      "default": true,
      "description": "Include Hex Components in the ingestion",
      "title": "Include Components",
      "type": "boolean"
    },
    "page_size": {
      "default": 100,
      "description": "Number of items to fetch per Hex API call.",
      "title": "Page Size",
      "type": "integer"
    },
    "patch_metadata": {
      "default": false,
      "description": "Emit metadata as patch events",
      "title": "Patch Metadata",
      "type": "boolean"
    },
    "collections_as_tags": {
      "default": true,
      "description": "Emit Hex Collections as tags",
      "title": "Collections As Tags",
      "type": "boolean"
    },
    "status_as_tag": {
      "default": true,
      "description": "Emit Hex Status as tags",
      "title": "Status As Tag",
      "type": "boolean"
    },
    "categories_as_tags": {
      "default": true,
      "description": "Emit Hex Category as tags",
      "title": "Categories As Tags",
      "type": "boolean"
    },
    "project_title_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex pattern for project titles to filter in ingestion."
    },
    "component_title_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex pattern for component titles to filter in ingestion."
    },
    "set_ownership_from_email": {
      "default": true,
      "description": "Set ownership identity from owner/creator email",
      "title": "Set Ownership From Email",
      "type": "boolean"
    },
    "include_lineage": {
      "default": true,
      "description": "Include Hex lineage, being fetched from DataHub. See \"Limitations\" section in the docs for more details about the limitations of this feature.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "lineage_start_time": {
      "anyOf": [
        {
          "format": "date-time",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Earliest date of lineage to consider. Default: 1 day before lineage end time. You can specify absolute time like '2023-01-01' or relative time like '-7 days' or '-7d'.",
      "title": "Lineage Start Time"
    },
    "lineage_end_time": {
      "anyOf": [
        {
          "format": "date-time",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Latest date of lineage to consider. Default: Current time in UTC. You can specify absolute time like '2023-01-01' or relative time like '-1 day' or '-1d'.",
      "title": "Lineage End Time"
    },
    "datahub_page_size": {
      "default": 100,
      "description": "Number of items to fetch per DataHub API call.",
      "title": "Datahub Page Size",
      "type": "integer"
    }
  },
  "required": [
    "workspace_name",
    "token"
  ],
  "title": "HexSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.hex.hex.HexSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/hex/hex.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Hex, feel free to ping us on [our Slack](https://datahub.com/slack).
