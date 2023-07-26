---
sidebar_position: 28
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

| Capability                                          | Status | Notes              |
| --------------------------------------------------- | ------ | ------------------ |
| [Platform Instance](../../../platform-instances.md) | ✅     | Enabled by default |

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
| --------- | -------------------------------------- |
| `Filters` | Filters applied to the chart           |
| `Metrics` | Fields or columns used for aggregation |
| `X`       | Fields used in X-axis                  |
| `X2`      | Fields used in second X-axis           |
| `Y`       | Fields used in Y-axis                  |
| `Y2`      | Fields used in second Y-axis           |

#### Table Information

| Name      | Description                  |
| --------- | ---------------------------- |
| `Columns` | Column names in a table      |
| `Filters` | Filters applied to the table |

#### Pivot Table Information

| Name      | Description                            |
| --------- | -------------------------------------- |
| `Columns` | Column names in a table                |
| `Filters` | Filters applied to the table           |
| `Metrics` | Fields or columns used for aggregation |
| `Rows`    | Row names in a table                   |

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[mode]'
```

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

| Field                                                                                                                                                                                                                                         | Description                                                                                                                                                                                                                                                                          |
| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">password</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div>                                  | Mode password for authentication.                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                               | Mode user token.                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                               | Mode host URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://app.mode.com</span></div>                                                                                                                                       |
| <div className="path-line"><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                            | Default schema to use when schema is not provided in an SQL query <div className="default-line default-line-with-docs">Default: <span className="default-value">public</span></div>                                                                                                  |
| <div className="path-line"><span className="path-main">owner_username_instead_of_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                          | Use username for owner URN instead of Email <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                          |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                                            |                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">workspace</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                 |                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                       | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                 |
| <div className="path-line"><span className="path-main">api_options</span></div> <div className="type-name-line"><span className="type-name">ModeAPIConfig</span></div>                                                                        | Retry/Wait settings for Mode API to avoid "Too many Requests" error. See Mode API Options below <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;retry_backoff_multiplier&#x27;: 2, &#x27;max_retry_interva...</span></div> |
| <div className="path-line"><span className="path-prefix">api_options.</span><span className="path-main">max_attempts</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                            | Maximum number of attempts to retry before failing <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div>                                                                                                                      |
| <div className="path-line"><span className="path-prefix">api_options.</span><span className="path-main">max_retry_interval</span></div> <div className="type-name-line"><span className="type-name">One of integer, number</span></div>       | Maximum interval to wait when retrying <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div>                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">api_options.</span><span className="path-main">retry_backoff_multiplier</span></div> <div className="type-name-line"><span className="type-name">One of integer, number</span></div> | Multiplier for exponential backoff when waiting to retry <div className="default-line default-line-with-docs">Default: <span className="default-value">2</span></div>                                                                                                                |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "ModeConfig",
  "description": "Any non-Dataset source that produces lineage to Datasets should inherit this class.\ne.g. Orchestrators, Pipelines, BI Tools etc.",
  "type": "object",
  "properties": {
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance_map": {
      "title": "Platform Instance Map",
      "description": "A holder for platform -> platform_instance mappings to generate correct dataset urns",
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    },
    "connect_uri": {
      "title": "Connect Uri",
      "description": "Mode host URL.",
      "default": "https://app.mode.com",
      "type": "string"
    },
    "token": {
      "title": "Token",
      "description": "Mode user token.",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "Mode password for authentication.",
      "type": "string",
      "writeOnly": true,
      "format": "password"
    },
    "workspace": {
      "title": "Workspace",
      "type": "string"
    },
    "default_schema": {
      "title": "Default Schema",
      "description": "Default schema to use when schema is not provided in an SQL query",
      "default": "public",
      "type": "string"
    },
    "owner_username_instead_of_email": {
      "title": "Owner Username Instead Of Email",
      "description": "Use username for owner URN instead of Email",
      "default": true,
      "type": "boolean"
    },
    "api_options": {
      "title": "Api Options",
      "description": "Retry/Wait settings for Mode API to avoid \"Too many Requests\" error. See Mode API Options below",
      "default": {
        "retry_backoff_multiplier": 2,
        "max_retry_interval": 10,
        "max_attempts": 5
      },
      "allOf": [
        {
          "$ref": "#/definitions/ModeAPIConfig"
        }
      ]
    }
  },
  "required": [
    "token",
    "password"
  ],
  "additionalProperties": false,
  "definitions": {
    "ModeAPIConfig": {
      "title": "ModeAPIConfig",
      "type": "object",
      "properties": {
        "retry_backoff_multiplier": {
          "title": "Retry Backoff Multiplier",
          "description": "Multiplier for exponential backoff when waiting to retry",
          "default": 2,
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "number"
            }
          ]
        },
        "max_retry_interval": {
          "title": "Max Retry Interval",
          "description": "Maximum interval to wait when retrying",
          "default": 10,
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "number"
            }
          ]
        },
        "max_attempts": {
          "title": "Max Attempts",
          "description": "Maximum number of attempts to retry before failing",
          "default": 5,
          "type": "integer"
        }
      },
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

See Mode's [Authentication documentation](https://mode.com/developer/api-reference/authentication/) on how to generate `token` and `password`.

### Code Coordinates

- Class Name: `datahub.ingestion.source.mode.ModeSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/mode.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Mode, feel free to ping us on [our Slack](https://slack.datahubproject.io).
