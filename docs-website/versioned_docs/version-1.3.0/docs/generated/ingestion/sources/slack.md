---
sidebar_position: 66
title: Slack
slug: /generated/ingestion/sources/slack
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/slack.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Slack
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |



### CLI based Ingestion

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">bot_token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Bot token for the Slack workspace. Needs `users:read`, `users:read.email`, `users.profile:read`, and `team:read` scopes.  |
| <div className="path-line"><span className="path-main">api_requests_per_min</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of API requests per minute. Low-level config. Do not tweak unless you are facing any issues. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">channel_min_members</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Ingest channels with at least this many members. <div className="default-line default-line-with-docs">Default: <span className="default-value">2</span></div> |
| <div className="path-line"><span className="path-main">channels_iteration_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Limit the number of channels to be ingested in a iteration. Low-level config. Do not tweak unless you are facing any issues. <div className="default-line default-line-with-docs">Default: <span className="default-value">200</span></div> |
| <div className="path-line"><span className="path-main">enrich_user_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, will enrich provisioned DataHub users' metadata with information from Slack. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_public_channels</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest public channels. If set to true needs `channels:read` scope. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ingest_users</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest users. When set to true, will ingest all users in the Slack workspace (as platform resources) to simplify user enrichment after they are provisioned on DataHub. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">should_ingest_archived_channels</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest archived channels. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulIngestionConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "StatefulIngestionConfig": {
      "additionalProperties": false,
      "description": "Basic Stateful Ingestion Specific Configuration for any source.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        }
      },
      "title": "StatefulIngestionConfig",
      "type": "object"
    }
  },
  "properties": {
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulIngestionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful Ingestion Config"
    },
    "bot_token": {
      "description": "Bot token for the Slack workspace. Needs `users:read`, `users:read.email`, `users.profile:read`, and `team:read` scopes.",
      "format": "password",
      "title": "Bot Token",
      "type": "string",
      "writeOnly": true
    },
    "enrich_user_metadata": {
      "default": true,
      "description": "When enabled, will enrich provisioned DataHub users' metadata with information from Slack.",
      "title": "Enrich User Metadata",
      "type": "boolean"
    },
    "ingest_users": {
      "default": true,
      "description": "Whether to ingest users. When set to true, will ingest all users in the Slack workspace (as platform resources) to simplify user enrichment after they are provisioned on DataHub.",
      "title": "Ingest Users",
      "type": "boolean"
    },
    "api_requests_per_min": {
      "default": 10,
      "description": "Number of API requests per minute. Low-level config. Do not tweak unless you are facing any issues.",
      "title": "Api Requests Per Min",
      "type": "integer"
    },
    "ingest_public_channels": {
      "default": false,
      "description": "Whether to ingest public channels. If set to true needs `channels:read` scope.",
      "title": "Ingest Public Channels",
      "type": "boolean"
    },
    "channels_iteration_limit": {
      "default": 200,
      "description": "Limit the number of channels to be ingested in a iteration. Low-level config. Do not tweak unless you are facing any issues.",
      "title": "Channels Iteration Limit",
      "type": "integer"
    },
    "channel_min_members": {
      "default": 2,
      "description": "Ingest channels with at least this many members.",
      "title": "Channel Min Members",
      "type": "integer"
    },
    "should_ingest_archived_channels": {
      "default": false,
      "description": "Whether to ingest archived channels.",
      "title": "Should Ingest Archived Channels",
      "type": "boolean"
    }
  },
  "required": [
    "bot_token"
  ],
  "title": "SlackSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.slack.slack.SlackSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/slack/slack.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Slack, feel free to ping us on [our Slack](https://datahub.com/slack).
