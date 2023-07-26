---
sidebar_position: 48
title: Superset
slug: /generated/ingestion/sources/superset
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/superset.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Superset

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                                     |
| ---------------------------------------------------------------------------------------------------------- | ------ | ----------------------------------------- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Optionally enabled via stateful_ingestion |

This plugin extracts the following:

- Charts, dashboards, and associated metadata

See documentation for superset's /security/login at https://superset.apache.org/docs/rest-api for more details on superset's login api.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[superset]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: superset
  config:
    # Coordinates
    connect_uri: http://localhost:8088

    # Credentials
    username: user
    password: pass
    provider: ldap

sink:
  # sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                              | Description                                                                                                                                                                                                                                  |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                    | Superset host URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">http://localhost:8088</span></div>                                                                                          |
| <div className="path-line"><span className="path-main">database_alias</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                                        |                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">display_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                    | optional URL to use in links (if `connect_uri` is only for ingestion)                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                        | <div className="default-line ">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                                            |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                       | Superset password.                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">provider</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                       | Superset provider. <div className="default-line default-line-with-docs">Default: <span className="default-value">db</span></div>                                                                                                             |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                       | Superset username.                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                            | Environment to use in namespace when constructing URNs <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                       |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                 | Superset Stateful Ingestion Config.                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                           |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "SupersetConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
  "type": "object",
  "properties": {
    "stateful_ingestion": {
      "title": "Stateful Ingestion",
      "description": "Superset Stateful Ingestion Config.",
      "allOf": [
        {
          "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
        }
      ]
    },
    "connect_uri": {
      "title": "Connect Uri",
      "description": "Superset host URL.",
      "default": "http://localhost:8088",
      "type": "string"
    },
    "display_uri": {
      "title": "Display Uri",
      "description": "optional URL to use in links (if `connect_uri` is only for ingestion)",
      "type": "string"
    },
    "username": {
      "title": "Username",
      "description": "Superset username.",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "Superset password.",
      "type": "string"
    },
    "provider": {
      "title": "Provider",
      "description": "Superset provider.",
      "default": "db",
      "type": "string"
    },
    "options": {
      "title": "Options",
      "default": {},
      "type": "object"
    },
    "env": {
      "title": "Env",
      "description": "Environment to use in namespace when constructing URNs",
      "default": "PROD",
      "type": "string"
    },
    "database_alias": {
      "title": "Database Alias",
      "description": "Can be used to change mapping for database names in superset to what you have in datahub",
      "default": {},
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
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
    }
  }
}
```

</TabItem>
</Tabs>

If you were using `database_alias` in one of your other ingestions to rename your databases to something else based on business needs you can rename them in superset also

```yml
source:
  type: superset
  config:
    # Coordinates
    connect_uri: http://localhost:8088

    # Credentials
    username: user
    password: pass
    provider: ldap
    database_alias:
      example_name_1: business_name_1
      example_name_2: business_name_2

sink:
  # sink configs
```

### Code Coordinates

- Class Name: `datahub.ingestion.source.superset.SupersetSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/superset.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Superset, feel free to ping us on [our Slack](https://slack.datahubproject.io).
