---
sidebar_position: 36
title: LDAP
slug: /generated/ingestion/sources/ldap
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/ldap.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# LDAP
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |


This plugin extracts the following:
- People
- Names, emails, titles, and manager information for each person
- List of groups


### CLI based Ingestion

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

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">base_dn</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | LDAP DN.  |
| <div className="path-line"><span className="path-main">ldap_password</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | LDAP password.  |
| <div className="path-line"><span className="path-main">ldap_server</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | LDAP server URL.  |
| <div className="path-line"><span className="path-main">ldap_user</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | LDAP user.  |
| <div className="path-line"><span className="path-main">drop_missing_first_last_name</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If set to true, any users without first and last names will be dropped. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">filter</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | LDAP extractor filter. <div className="default-line default-line-with-docs">Default: <span className="default-value">(objectClass=&#42;)</span></div> |
| <div className="path-line"><span className="path-main">group_attrs_map</span></div> <div className="type-name-line"><span className="type-name">object</span></div> |  <div className="default-line ">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">manager_filter_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use LDAP extractor filter to search managers. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">manager_pagination_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | [deprecated] Use pagination_enabled  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Size of each page to fetch when extracting metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-main">pagination_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use pagination while do search query (enabled by default). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">use_email_as_username</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use email for users' usernames instead of username (disabled by default).             If enabled, the user and group urn would be having email as the id part of the urn. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">user_attrs_map</span></div> <div className="type-name-line"><span className="type-name">object</span></div> |  <div className="default-line ">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">attrs_list</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Retrieved attributes list <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">attrs_list.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">custom_props_list</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | A list of custom attributes to extract from the LDAP provider. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">custom_props_list.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
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
  "description": "Config used by the LDAP Source.",
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
      "default": null
    },
    "ldap_server": {
      "description": "LDAP server URL.",
      "title": "Ldap Server",
      "type": "string"
    },
    "ldap_user": {
      "description": "LDAP user.",
      "title": "Ldap User",
      "type": "string"
    },
    "ldap_password": {
      "description": "LDAP password.",
      "title": "Ldap Password",
      "type": "string"
    },
    "base_dn": {
      "description": "LDAP DN.",
      "title": "Base Dn",
      "type": "string"
    },
    "filter": {
      "default": "(objectClass=*)",
      "description": "LDAP extractor filter.",
      "title": "Filter",
      "type": "string"
    },
    "attrs_list": {
      "anyOf": [
        {
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Retrieved attributes list",
      "title": "Attrs List"
    },
    "custom_props_list": {
      "anyOf": [
        {
          "items": {
            "type": "string"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "A list of custom attributes to extract from the LDAP provider.",
      "title": "Custom Props List"
    },
    "drop_missing_first_last_name": {
      "default": true,
      "description": "If set to true, any users without first and last names will be dropped.",
      "title": "Drop Missing First Last Name",
      "type": "boolean"
    },
    "page_size": {
      "default": 20,
      "description": "Size of each page to fetch when extracting metadata.",
      "title": "Page Size",
      "type": "integer"
    },
    "manager_filter_enabled": {
      "default": true,
      "description": "Use LDAP extractor filter to search managers.",
      "title": "Manager Filter Enabled",
      "type": "boolean"
    },
    "manager_pagination_enabled": {
      "default": true,
      "description": "[deprecated] Use pagination_enabled ",
      "title": "Manager Pagination Enabled",
      "type": "boolean"
    },
    "pagination_enabled": {
      "default": true,
      "description": "Use pagination while do search query (enabled by default).",
      "title": "Pagination Enabled",
      "type": "boolean"
    },
    "use_email_as_username": {
      "default": false,
      "description": "Use email for users' usernames instead of username (disabled by default).             If enabled, the user and group urn would be having email as the id part of the urn.",
      "title": "Use Email As Username",
      "type": "boolean"
    },
    "user_attrs_map": {
      "additionalProperties": true,
      "default": {},
      "title": "User Attrs Map",
      "type": "object"
    },
    "group_attrs_map": {
      "additionalProperties": true,
      "default": {},
      "title": "Group Attrs Map",
      "type": "object"
    }
  },
  "required": [
    "ldap_server",
    "ldap_user",
    "ldap_password",
    "base_dn"
  ],
  "title": "LDAPSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.ldap.LDAPSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/ldap.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for LDAP, feel free to ping us on [our Slack](https://datahub.com/slack).
