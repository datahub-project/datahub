


# Slack

## Overview

Slack is a documentation or collaboration platform. Learn more in the [official Slack documentation](https://slack.com/).

The DataHub integration for Slack covers document/workspace entities and hierarchy context for knowledge assets. It also captures stateful deletion detection.

## Concept Mapping

| Source Concept                  | DataHub Concept                                       | Notes                                                         |
| ------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------- |
| Workspace scope                 | Platform Instance / Container                         | Organizes Slack metadata context.                             |
| Channel / conversation metadata | Dataset or Document-style entities (module dependent) | Represented based on connector modeling choices.              |
| Users and memberships           | CorpUser / CorpGroup style metadata                   | Used for ownership and collaboration context where supported. |


## Module `slack`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |

### Overview

The `slack` module ingests Slack metadata into DataHub to improve collaboration context and discovery across communication artifacts.

### Prerequisites

- Slack API access token with permissions for the metadata you plan to ingest.
- Network connectivity from the ingestion runtime to Slack APIs.
- Access scope validation in the target Slack workspace.


### Install the Plugin
```shell
pip install 'acryl-datahub[slack]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: slack
  config:
    token: "${SLACK_BOT_TOKEN}"

sink:
  # sink configs

```

### Config Details

                
#### Options


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




#### Schema


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





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported Slack ingestion features.

### Limitations

- Available metadata is constrained by Slack API scopes and workspace permissions.
- Some advanced metadata may require elevated scopes or enterprise Slack configurations.

### Troubleshooting

- Verify token scopes and workspace authorization first.
- Confirm API reachability and rate-limit behavior from the ingestion runtime.
- Review ingestion logs for Slack API errors and missing scope diagnostics.


### Code Coordinates
- Class Name: `datahub.ingestion.source.slack.slack.SlackSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/slack/slack.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Slack, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
