---
sidebar_position: 23
title: LDAP
slug: /generated/ingestion/sources/ldap
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/ldap.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# LDAP

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

This plugin extracts the following:

- People
- Names, emails, titles, and manager information for each person
- List of groups

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[ldap]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: "ldap"
  config:
    # Coordinates
    ldap_server: ldap://localhost

    # Credentials
    ldap_user: "cn=admin,dc=example,dc=org"
    ldap_password: "admin"

    # Options
    base_dn: "dc=example,dc=org"

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
| <div className="path-line"><span className="path-main">base_dn</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                  | LDAP DN.                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">ldap_password</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                            | LDAP password.                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">ldap_server</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                              | LDAP server URL.                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">ldap_user</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                | LDAP user.                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">attrs_list</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                              |                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">custom_props_list</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                       |                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">drop_missing_first_last_name</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                  | If set to true, any users without first and last names will be dropped. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                      |
| <div className="path-line"><span className="path-main">filter</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                         | LDAP extractor filter. <div className="default-line default-line-with-docs">Default: <span className="default-value">(objectClass=\*)</span></div>                                                                                           |
| <div className="path-line"><span className="path-main">group_attrs_map</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                | <div className="default-line ">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                                            |
| <div className="path-line"><span className="path-main">manager_filter_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                        | Use LDAP extractor filter to search managers. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                |
| <div className="path-line"><span className="path-main">manager_pagination_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                    | Use pagination while search for managers (enabled by default). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                               |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                     | Size of each page to fetch when extracting metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div>                                                                           |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                              | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                               |
| <div className="path-line"><span className="path-main">user_attrs_map</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                 | <div className="default-line ">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                                            |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                            | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                         |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                 | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                           |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "LDAPSourceConfig",
  "description": "Config used by the LDAP Source.",
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
      "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
    },
    "ldap_server": {
      "title": "Ldap Server",
      "description": "LDAP server URL.",
      "type": "string"
    },
    "ldap_user": {
      "title": "Ldap User",
      "description": "LDAP user.",
      "type": "string"
    },
    "ldap_password": {
      "title": "Ldap Password",
      "description": "LDAP password.",
      "type": "string"
    },
    "base_dn": {
      "title": "Base Dn",
      "description": "LDAP DN.",
      "type": "string"
    },
    "filter": {
      "title": "Filter",
      "description": "LDAP extractor filter.",
      "default": "(objectClass=*)",
      "type": "string"
    },
    "attrs_list": {
      "title": "Attrs List",
      "description": "Retrieved attributes list",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "custom_props_list": {
      "title": "Custom Props List",
      "description": "A list of custom attributes to extract from the LDAP provider.",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "drop_missing_first_last_name": {
      "title": "Drop Missing First Last Name",
      "description": "If set to true, any users without first and last names will be dropped.",
      "default": true,
      "type": "boolean"
    },
    "page_size": {
      "title": "Page Size",
      "description": "Size of each page to fetch when extracting metadata.",
      "default": 20,
      "type": "integer"
    },
    "manager_filter_enabled": {
      "title": "Manager Filter Enabled",
      "description": "Use LDAP extractor filter to search managers.",
      "default": true,
      "type": "boolean"
    },
    "manager_pagination_enabled": {
      "title": "Manager Pagination Enabled",
      "description": "Use pagination while search for managers (enabled by default).",
      "default": true,
      "type": "boolean"
    },
    "user_attrs_map": {
      "title": "User Attrs Map",
      "default": {},
      "type": "object"
    },
    "group_attrs_map": {
      "title": "Group Attrs Map",
      "default": {},
      "type": "object"
    }
  },
  "required": [
    "ldap_server",
    "ldap_user",
    "ldap_password",
    "base_dn"
  ],
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

### Code Coordinates

- Class Name: `datahub.ingestion.source.ldap.LDAPSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/ldap.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for LDAP, feel free to ping us on [our Slack](https://slack.datahubproject.io).
