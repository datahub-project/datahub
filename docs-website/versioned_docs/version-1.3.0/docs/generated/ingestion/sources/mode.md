---
sidebar_position: 43
title: Mode
slug: /generated/ingestion/sources/mode
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/mode.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Mode
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| Column-level Lineage | ✅ | Supported by default. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Supported by default. |



This plugin extracts Charts, Reports, and associated metadata from a given Mode workspace. This plugin is in beta and has only been tested
on PostgreSQL database.

### Report

[/api/{account}/reports/{report}](https://mode.com/developer/api-reference/analytics/reports/) endpoint is used to
retrieve the following report information.

- Title and description
- Last edited by
- Owner
- Link to the Report in Mode for exploration
- Associated charts within the report

### Chart

[/api/{workspace}/reports/{report}/queries/{query}/charts'](https://mode.com/developer/api-reference/analytics/charts/#getChart) endpoint is used to
retrieve the following information.

- Title and description
- Last edited by
- Owner
- Link to the chart in Metabase
- Datasource and lineage information from Report queries.

The following properties for a chart are ingested in DataHub.

#### Chart Information
| Name      | Description                            |
|-----------|----------------------------------------|
| `Filters` | Filters applied to the chart           |
| `Metrics` | Fields or columns used for aggregation |
| `X`       | Fields used in X-axis                  |
| `X2`      | Fields used in second X-axis           |
| `Y`       | Fields used in Y-axis                  |
| `Y2`      | Fields used in second Y-axis           |


#### Table Information
| Name      | Description                  |
|-----------|------------------------------|
| `Columns` | Column names in a table      |
| `Filters` | Filters applied to the table |



#### Pivot Table Information
| Name      | Description                            |
|-----------|----------------------------------------|
| `Columns` | Column names in a table                |
| `Filters` | Filters applied to the table           |
| `Metrics` | Fields or columns used for aggregation |
| `Rows`    | Row names in a table                   |


### Authentication

See Mode's [Authentication documentation](https://mode.com/developer/api-reference/authentication/) on how to generate an API `token` and `password`.

Mode does not support true "service accounts", so you must use a user account for authentication.
Depending on your requirements, you may want to create a dedicated user account for usage with DataHub ingestion.

### Permissions

DataHub ingestion requires the user to have the following permissions:

- Have at least the "Member" role.
- For each Connection, have at least"View" access.

  To check Connection permissions, navigate to "Workspace Settings" → "Manage Connections". For each connection in the list, click on the connection → "Permissions". If the default workspace access is "View" or "Query", you're all set for that connection. If it's "Restricted", you'll need to individually grant your ingestion user View access.

- For each Space, have at least "View" access.

  To check Collection permissions, navigate to the "My Collections" page as an Admin user. For each collection with Workspace Access set to "Restricted" access, the ingestion user must be manually granted the "Viewer" access in the "Manage Access" dialog. Collections with "All Members can View/Edit" do not need to be manually granted access.

Note that if the ingestion user has "Admin" access, then it will automatically have "View" access for all connections and collections.

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: mode
  config:
    # Coordinates
    connect_uri: http://app.mode.com

    # Credentials
    token: token
    password: pass

    # Options
    workspace: "datahub"
    default_schema: "public"
    owner_username_instead_of_email: False
    api_options:
      retry_backoff_multiplier: 2
      max_retry_interval: 10
      max_attempts: 5

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
| <div className="path-line"><span className="path-main">password</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | When creating workspace API key this is the 'Secret'.  |
| <div className="path-line"><span className="path-main">token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | When creating workspace API key this is the 'Key ID'.  |
| <div className="path-line"><span className="path-main">workspace</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The Mode workspace username. If you navigate to Workspace Settings > Details, the url will be `https://app.mode.com/organizations/<workspace-username>`. This is distinct from the workspace's display name, and should be all lowercase.  |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Mode host URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://app.mode.com</span></div> |
| <div className="path-line"><span className="path-main">exclude_restricted</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Exclude restricted collections <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_embed_url</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to Ingest embed URL for Reports <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">owner_username_instead_of_email</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Use username for owner URN instead of Email <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A holder for platform -> platform_instance mappings to generate correct dataset urns <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tag_measures_and_dimensions</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Tag measures and dimensions in the schema <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">api_options</span></div> <div className="type-name-line"><span className="type-name">ModeAPIConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">api_options.</span><span className="path-main">max_attempts</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of attempts to retry before failing <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">api_options.</span><span className="path-main">max_retry_interval</span></div> <div className="type-name-line"><span className="type-name">One of integer, number</span></div> | Maximum interval to wait when retrying <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">api_options.</span><span className="path-main">retry_backoff_multiplier</span></div> <div className="type-name-line"><span className="type-name">One of integer, number</span></div> | Multiplier for exponential backoff when waiting to retry <div className="default-line default-line-with-docs">Default: <span className="default-value">2</span></div> |
| <div className="path-line"><span className="path-prefix">api_options.</span><span className="path-main">timeout</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Timout setting, how long to wait for the Mode rest api to send data before giving up <div className="default-line default-line-with-docs">Default: <span className="default-value">40</span></div> |
| <div className="path-line"><span className="path-main">space_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">space_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
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
    "ModeAPIConfig": {
      "additionalProperties": false,
      "properties": {
        "retry_backoff_multiplier": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "number"
            }
          ],
          "default": 2,
          "description": "Multiplier for exponential backoff when waiting to retry",
          "title": "Retry Backoff Multiplier"
        },
        "max_retry_interval": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "number"
            }
          ],
          "default": 10,
          "description": "Maximum interval to wait when retrying",
          "title": "Max Retry Interval"
        },
        "max_attempts": {
          "default": 5,
          "description": "Maximum number of attempts to retry before failing",
          "title": "Max Attempts",
          "type": "integer"
        },
        "timeout": {
          "default": 40,
          "description": "Timout setting, how long to wait for the Mode rest api to send data before giving up",
          "title": "Timeout",
          "type": "integer"
        }
      },
      "title": "ModeAPIConfig",
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
    "connect_uri": {
      "default": "https://app.mode.com",
      "description": "Mode host URL.",
      "title": "Connect Uri",
      "type": "string"
    },
    "token": {
      "description": "When creating workspace API key this is the 'Key ID'.",
      "title": "Token",
      "type": "string"
    },
    "password": {
      "description": "When creating workspace API key this is the 'Secret'.",
      "format": "password",
      "title": "Password",
      "type": "string",
      "writeOnly": true
    },
    "exclude_restricted": {
      "default": false,
      "description": "Exclude restricted collections",
      "title": "Exclude Restricted",
      "type": "boolean"
    },
    "workspace": {
      "description": "The Mode workspace username. If you navigate to Workspace Settings > Details, the url will be `https://app.mode.com/organizations/<workspace-username>`. This is distinct from the workspace's display name, and should be all lowercase.",
      "title": "Workspace",
      "type": "string"
    },
    "space_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "^Personal$"
        ],
        "ignoreCase": true
      },
      "description": "Regex patterns for mode spaces to filter in ingestion (Spaces named as 'Personal' are filtered by default.) Specify regex to only match the space name. e.g. to only ingest space named analytics, use the regex 'analytics'"
    },
    "owner_username_instead_of_email": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Use username for owner URN instead of Email",
      "title": "Owner Username Instead Of Email"
    },
    "api_options": {
      "$ref": "#/$defs/ModeAPIConfig",
      "default": {
        "retry_backoff_multiplier": 2,
        "max_retry_interval": 10,
        "max_attempts": 5,
        "timeout": 40
      },
      "description": "Retry/Wait settings for Mode API to avoid \"Too many Requests\" error. See Mode API Options below"
    },
    "ingest_embed_url": {
      "default": true,
      "description": "Whether to Ingest embed URL for Reports",
      "title": "Ingest Embed Url",
      "type": "boolean"
    },
    "tag_measures_and_dimensions": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Tag measures and dimensions in the schema",
      "title": "Tag Measures And Dimensions"
    }
  },
  "required": [
    "token",
    "password",
    "workspace"
  ],
  "title": "ModeConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.mode.ModeSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/mode.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Mode, feel free to ping us on [our Slack](https://datahub.com/slack).
