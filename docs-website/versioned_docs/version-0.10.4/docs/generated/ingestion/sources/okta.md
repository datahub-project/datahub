---
sidebar_position: 32
title: Okta
slug: /generated/ingestion/sources/okta
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/okta.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Okta

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                                     |
| ---------------------------------------------------------------------------------------------------------- | ------ | ----------------------------------------- |
| Descriptions                                                                                               | ✅     | Optionally enabled via configuration      |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Optionally enabled via stateful_ingestion |

This plugin extracts the following:

- Users
- Groups
- Group Membership

from your Okta instance.

Note that any users ingested from this connector will not be able to log into DataHub unless you have Okta OIDC SSO
enabled. You can, however, have these users ingested into DataHub before they log in for the first time if you would
like to take actions like adding them to a group or assigning them a role.

For instructions on how to do configure Okta OIDC SSO, please read the documentation
[here](/docs/authentication/guides/sso/configure-oidc-react-okta).

### Extracting DataHub Users

#### Usernames

Usernames serve as unique identifiers for users on DataHub. This connector extracts usernames using the
"login" field of an [Okta User Profile](https://developer.okta.com/docs/reference/api/users/#profile-object).
By default, the 'login' attribute, which contains an email, is parsed to extract the text before the "@" and map that to the DataHub username.

If this is not how you wish to map to DataHub usernames, you can provide a custom mapping using the configurations options detailed below. Namely, `okta_profile_to_username_attr`
and `okta_profile_to_username_regex`. e.g. if you want to map emails to urns then you may use the following configuration:

```yaml
okta_profile_to_username_attr: "email"
okta_profile_to_username_regex: ".*"
```

#### Profiles

This connector also extracts basic user profile information from Okta. The following fields of the Okta User Profile are extracted
and mapped to the DataHub `CorpUserInfo` aspect:

- display name
- first name
- last name
- email
- title
- department
- country code

### Extracting DataHub Groups

#### Group Names

Group names serve as unique identifiers for groups on DataHub. This connector extracts group names using the "name" attribute of an Okta Group Profile.
By default, a URL-encoded version of the full group name is used as the unique identifier (CorpGroupKey) and the raw "name" attribute is mapped
as the display name that will appear in DataHub's UI.

If this is not how you wish to map to DataHub group names, you can provide a custom mapping using the configurations options detailed below. Namely, `okta_profile_to_group_name_attr`
and `okta_profile_to_group_name_regex`.

#### Profiles

This connector also extracts basic group information from Okta. The following fields of the Okta Group Profile are extracted and mapped to the
DataHub `CorpGroupInfo` aspect:

- name
- description

### Extracting Group Membership

This connector additional extracts the edges between Users and Groups that are stored in Okta. It maps them to the `GroupMembership` aspect
associated with DataHub users (CorpUsers).

### Filtering and Searching

You can also choose to ingest a subset of users or groups to Datahub by adding flags for filtering or searching. For
users, set either the `okta_users_filter` or `okta_users_search` flag (only one can be set at a time). For groups, set
either the `okta_groups_filter` or `okta_groups_search` flag. Note that these are not regular expressions. See [below](#config-details) for full configuration
options.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[okta]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: okta
  config:
    # Coordinates
    okta_domain: "dev-35531955.okta.com"

    # Credentials
    okta_api_token: "11be4R_M2MzDqXawbTHfKGpKee0kuEOfX1RCQSRx99"

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
| <div className="path-line"><span className="path-main">okta_api_token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                           | An API token generated for the DataHub application inside your Okta Developer Console. e.g. 00be4R_M2MzDqXawbWgfKGpKee0kuEOfX1RCQSRx00                                                                                                       |
| <div className="path-line"><span className="path-main">okta_domain</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                              | The location of your Okta Domain, without a protocol. Can be found in Okta Developer console. e.g. dev-33231928.okta.com                                                                                                                     |
| <div className="path-line"><span className="path-main">delay_seconds</span></div> <div className="type-name-line"><span className="type-name">One of number, integer</span></div>                                                  | Number of seconds to wait between calls to Okta's REST APIs. (Okta rate limits). Defaults to 10ms. <div className="default-line default-line-with-docs">Default: <span className="default-value">0.01</span></div>                           |
| <div className="path-line"><span className="path-main">include_deprovisioned_users</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                   | Whether to ingest users in the DEPROVISIONED state from Okta. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                               |
| <div className="path-line"><span className="path-main">include_suspended_users</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                       | Whether to ingest users in the SUSPENDED state from Okta. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                   |
| <div className="path-line"><span className="path-main">ingest_group_membership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                       | Whether group membership should be ingested into DataHub. ingest_groups must be True if this is True. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                        |
| <div className="path-line"><span className="path-main">ingest_groups</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                 | Whether groups should be ingested into DataHub. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                              |
| <div className="path-line"><span className="path-main">ingest_users</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                  | Whether users should be ingested into DataHub. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                               |
| <div className="path-line"><span className="path-main">mask_group_id</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                 | <div className="default-line ">Default: <span className="default-value">True</span></div>                                                                                                                                                    |
| <div className="path-line"><span className="path-main">mask_user_id</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                  | <div className="default-line ">Default: <span className="default-value">True</span></div>                                                                                                                                                    |
| <div className="path-line"><span className="path-main">okta_groups_filter</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                             | Okta filter expression (not regex) for ingesting groups. Only one of `okta_groups_filter` and `okta_groups_search` can be set. See (https://developer.okta.com/docs/reference/api/groups/#filters) for more info.                            |
| <div className="path-line"><span className="path-main">okta_groups_search</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                             | Okta search expression (not regex) for ingesting groups. Only one of `okta_groups_filter` and `okta_groups_search` can be set. See (https://developer.okta.com/docs/reference/api/groups/#list-groups-with-search) for more info.            |
| <div className="path-line"><span className="path-main">okta_profile_to_group_name_attr</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                | Which Okta Group Profile attribute to use as input to DataHub group name mapping. <div className="default-line default-line-with-docs">Default: <span className="default-value">name</span></div>                                            |
| <div className="path-line"><span className="path-main">okta_profile_to_group_name_regex</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                               | A regex used to parse the DataHub group name from the attribute specified in `okta_profile_to_group_name_attr`. <div className="default-line default-line-with-docs">Default: <span className="default-value">(.\*)</span></div>             |
| <div className="path-line"><span className="path-main">okta_profile_to_username_attr</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                  | Which Okta User Profile attribute to use as input to DataHub username mapping. Common values used are - login, email. <div className="default-line default-line-with-docs">Default: <span className="default-value">email</span></div>       |
| <div className="path-line"><span className="path-main">okta_profile_to_username_regex</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                 | A regex used to parse the DataHub username from the attribute specified in `okta_profile_to_username_attr`. <div className="default-line default-line-with-docs">Default: <span className="default-value">(.\*)</span></div>                 |
| <div className="path-line"><span className="path-main">okta_users_filter</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                              | Okta filter expression (not regex) for ingesting users. Only one of `okta_users_filter` and `okta_users_search` can be set. See (https://developer.okta.com/docs/reference/api/users/#list-users-with-a-filter) for more info.               |
| <div className="path-line"><span className="path-main">okta_users_search</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                              | Okta search expression (not regex) for ingesting users. Only one of `okta_users_filter` and `okta_users_search` can be set. See (https://developer.okta.com/docs/reference/api/users/#list-users-with-search) for more info.                 |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                     | The number of entities requested from Okta's REST APIs in one request. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div>                                                        |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                 | Okta Stateful Ingestion Config.                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                           |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "OktaConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
  "type": "object",
  "properties": {
    "stateful_ingestion": {
      "title": "Stateful Ingestion",
      "description": "Okta Stateful Ingestion Config.",
      "allOf": [
        {
          "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
        }
      ]
    },
    "okta_domain": {
      "title": "Okta Domain",
      "description": "The location of your Okta Domain, without a protocol. Can be found in Okta Developer console. e.g. dev-33231928.okta.com",
      "type": "string"
    },
    "okta_api_token": {
      "title": "Okta Api Token",
      "description": "An API token generated for the DataHub application inside your Okta Developer Console. e.g. 00be4R_M2MzDqXawbWgfKGpKee0kuEOfX1RCQSRx00",
      "type": "string"
    },
    "ingest_users": {
      "title": "Ingest Users",
      "description": "Whether users should be ingested into DataHub.",
      "default": true,
      "type": "boolean"
    },
    "ingest_groups": {
      "title": "Ingest Groups",
      "description": "Whether groups should be ingested into DataHub.",
      "default": true,
      "type": "boolean"
    },
    "ingest_group_membership": {
      "title": "Ingest Group Membership",
      "description": "Whether group membership should be ingested into DataHub. ingest_groups must be True if this is True.",
      "default": true,
      "type": "boolean"
    },
    "okta_profile_to_username_attr": {
      "title": "Okta Profile To Username Attr",
      "description": "Which Okta User Profile attribute to use as input to DataHub username mapping. Common values used are - login, email.",
      "default": "email",
      "type": "string"
    },
    "okta_profile_to_username_regex": {
      "title": "Okta Profile To Username Regex",
      "description": "A regex used to parse the DataHub username from the attribute specified in `okta_profile_to_username_attr`.",
      "default": "(.*)",
      "type": "string"
    },
    "okta_profile_to_group_name_attr": {
      "title": "Okta Profile To Group Name Attr",
      "description": "Which Okta Group Profile attribute to use as input to DataHub group name mapping.",
      "default": "name",
      "type": "string"
    },
    "okta_profile_to_group_name_regex": {
      "title": "Okta Profile To Group Name Regex",
      "description": "A regex used to parse the DataHub group name from the attribute specified in `okta_profile_to_group_name_attr`.",
      "default": "(.*)",
      "type": "string"
    },
    "include_deprovisioned_users": {
      "title": "Include Deprovisioned Users",
      "description": "Whether to ingest users in the DEPROVISIONED state from Okta.",
      "default": false,
      "type": "boolean"
    },
    "include_suspended_users": {
      "title": "Include Suspended Users",
      "description": "Whether to ingest users in the SUSPENDED state from Okta.",
      "default": false,
      "type": "boolean"
    },
    "page_size": {
      "title": "Page Size",
      "description": "The number of entities requested from Okta's REST APIs in one request.",
      "default": 100,
      "type": "integer"
    },
    "delay_seconds": {
      "title": "Delay Seconds",
      "description": "Number of seconds to wait between calls to Okta's REST APIs. (Okta rate limits). Defaults to 10ms.",
      "default": 0.01,
      "anyOf": [
        {
          "type": "number"
        },
        {
          "type": "integer"
        }
      ]
    },
    "okta_users_filter": {
      "title": "Okta Users Filter",
      "description": "Okta filter expression (not regex) for ingesting users. Only one of `okta_users_filter` and `okta_users_search` can be set. See (https://developer.okta.com/docs/reference/api/users/#list-users-with-a-filter) for more info.",
      "type": "string"
    },
    "okta_users_search": {
      "title": "Okta Users Search",
      "description": "Okta search expression (not regex) for ingesting users. Only one of `okta_users_filter` and `okta_users_search` can be set. See (https://developer.okta.com/docs/reference/api/users/#list-users-with-search) for more info.",
      "type": "string"
    },
    "okta_groups_filter": {
      "title": "Okta Groups Filter",
      "description": "Okta filter expression (not regex) for ingesting groups. Only one of `okta_groups_filter` and `okta_groups_search` can be set. See (https://developer.okta.com/docs/reference/api/groups/#filters) for more info.",
      "type": "string"
    },
    "okta_groups_search": {
      "title": "Okta Groups Search",
      "description": "Okta search expression (not regex) for ingesting groups. Only one of `okta_groups_filter` and `okta_groups_search` can be set. See (https://developer.okta.com/docs/reference/api/groups/#list-groups-with-search) for more info.",
      "type": "string"
    },
    "mask_group_id": {
      "title": "Mask Group Id",
      "default": true,
      "type": "boolean"
    },
    "mask_user_id": {
      "title": "Mask User Id",
      "default": true,
      "type": "boolean"
    }
  },
  "required": [
    "okta_domain",
    "okta_api_token"
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

As a prerequisite, you should create a DataHub Application within the Okta Developer Console with full permissions to read your organization's Users and Groups.

## Compatibility

Validated against Okta API Versions:

- `2021.07.2`

Validated against load:

- User Count: `1000`
- Group Count: `100`
- Group Membership Edges: `1000` (1 per User)
- Run Time (Wall Clock): `2min 7sec`

### Code Coordinates

- Class Name: `datahub.ingestion.source.identity.okta.OktaSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/identity/okta.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Okta, feel free to ping us on [our Slack](https://slack.datahubproject.io).
