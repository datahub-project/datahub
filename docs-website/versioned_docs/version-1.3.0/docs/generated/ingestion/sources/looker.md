---
sidebar_position: 37
title: Looker
slug: /generated/ingestion/sources/looker
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/looker.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Looker
There are 2 sources that provide integration with Looker

<table>
<tr><td>Source Module</td><td>Documentation</td></tr><tr>
<td>

`looker`

</td>
<td>



This plugin extracts the following:
- Looker dashboards, dashboard elements (charts) and explores
- Names, descriptions, URLs, chart types, input explores for the charts
- Schemas and input views for explores
- Owners of dashboards

:::note
To get complete Looker metadata integration (including Looker views and lineage to the underlying warehouse tables), you must ALSO use the `lookml` module.
:::
 [Read more...](#module-looker)


</td>
</tr>
<tr>
<td>

`lookml`

</td>
<td>



This plugin extracts the following:
- LookML views from model files in a project
- Name, upstream table names, metadata for dimensions, measures, and dimension groups attached as tags
- If API integration is enabled (recommended), resolves table and view names by calling the Looker API, otherwise supports offline resolution of these names.

:::note
To get complete Looker metadata integration (including Looker dashboards and charts and lineage to the underlying Looker views, you must ALSO use the `looker` source module.
:::
 [Read more...](#module-lookml)


</td>
</tr>
</table>



## Module `looker`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - LookML Model, Folder. |
| Column-level Lineage | ✅ | Enabled by default, configured using `extract_column_level_lineage`. |
| Dataset Usage | ✅ | Enabled by default, configured using `extract_usage_history`. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default, configured using `extract_owners`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Use the `platform_instance` field. |
| Table-Level Lineage | ✅ | Supported by default. |
| Test Connection | ✅ | Enabled by default. |


This plugin extracts the following:
- Looker dashboards, dashboard elements (charts) and explores
- Names, descriptions, URLs, chart types, input explores for the charts
- Schemas and input views for explores
- Owners of dashboards

:::note
To get complete Looker metadata integration (including Looker views and lineage to the underlying warehouse tables), you must ALSO use the `lookml` module.
:::

### Prerequisites

#### Set up the right permissions

You need to provide the following permissions for ingestion to work correctly.

```
access_data
explore
manage_models
see_datagroups
see_lookml
see_lookml_dashboards
see_looks
see_pdts
see_queries
see_schedules
see_sql
see_system_activity
see_user_dashboards
see_users
```

Here is an example permission set after configuration.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/looker_datahub_permission_set.png"/>
</p>

#### Get an API key

You need to get an API key for the account with the above privileges to perform ingestion. See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.

### Ingestion through UI

The following video shows you how to get started with ingesting Looker metadata through the UI.

:::note

You will need to run `lookml` ingestion through the CLI after you have ingested Looker metadata through the UI. Otherwise you will not be able to see Looker Views and their lineage to your warehouse tables.

:::

<div
  style={{
    position: "relative",
    paddingBottom: "57.692307692307686%",
    height: 0
  }}
>
  <iframe
    src="https://www.loom.com/embed/b8b9654e02714d20a44122cc1bffc1bb"
    frameBorder={0}
    webkitallowfullscreen=""
    mozallowfullscreen=""
    allowFullScreen=""
    style={{
      position: "absolute",
      top: 0,
      left: 0,
      width: "100%",
      height: "100%"
    }}
  />
</div>

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "looker"
  config:
    # Coordinates
    base_url: "https://<company>.cloud.looker.com"

    # Credentials
    client_id: ${LOOKER_CLIENT_ID}
    client_secret: ${LOOKER_CLIENT_SECRET}

    # Liquid variables
    # liquid_variables:
    #   _user_attributes:
    #     looker_env: "dev"
    #     dev_database_prefix: "employee"
    #     dev_schema_prefix: "public"
    #   dw_eff_dt_date:
    #     _is_selected: true
    #   source_region: "ap-south-1"
    #   db: "test-db"

    # LookML Constants
    # lookml_constants:
    #   star_award_winner_year: "public.winner_2025"
# sink configs

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">base_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.  |
| <div className="path-line"><span className="path-main">client_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Looker API client id.  |
| <div className="path-line"><span className="path-main">client_secret</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Looker API client secret.  |
| <div className="path-line"><span className="path-main">emit_used_explores_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, only explores that are used by a Dashboard/Look will be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">external_base_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional URL to use when constructing external URLs to Looker if the `base_url` is not the correct one to use. For example, `https://looker-public.company.com`. If not provided, the external base URL will default to `base_url`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, extracts column-level lineage from Views and Explores <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_embed_urls</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Produce URLs used to render Looker Explores as Previews inside of DataHub UI. Embeds must be enabled inside of Looker to use this feature. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_independent_looks</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract looks which are not part of any Dashboard. To enable this flag the stateful_ingestion should also be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_owners</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, extracts ownership from Looker directly. When disabled, ownership is left empty for dashboards and charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_usage_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest usage statistics for dashboards. Setting this to True will query looker system activity explores to fetch historical dashboard usage. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_usage_history_for_interval</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Used only if extract_usage_history is set to True. Interval to extract looker dashboard usage history for. See https://docs.looker.com/reference/filter-expressions#date_and_time. <div className="default-line default-line-with-docs">Default: <span className="default-value">30 days</span></div> |
| <div className="path-line"><span className="path-main">include_deleted</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to include deleted dashboards and looks. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_platform_instance_in_urns</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, platform instance will be added in dashboard and chart urn. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of retries for Looker API calls <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">max_threads</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Max parallelism for Looker API calls. Defaults to cpuCount or 40  |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">skip_personal_folders</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to skip ingestion of dashboards in personal folders. Setting this to True will only ingest dashboards in the Shared folder space. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">strip_user_ids_from_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, converts Looker user emails of the form name@domain.com to urn:li:corpuser:name when assigning ownership <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">tag_measures_and_dimensions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">chart_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">chart_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">dashboard_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dashboard_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">explore_browse_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div> |   |
| <div className="path-line"><span className="path-prefix">explore_browse_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if explore_browse_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">explore_naming_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div> |   |
| <div className="path-line"><span className="path-prefix">explore_naming_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if explore_naming_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">folder_path_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">folder_path_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">transport_options</span></div> <div className="type-name-line"><span className="type-name">One of TransportOptionsConfig, null</span></div> | Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">transport_options.</span><span className="path-main">headers</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">transport_options.</span><span className="path-main">timeout</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">integer</span></div> |   |
| <div className="path-line"><span className="path-main">view_browse_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerViewNamingPattern</span></div> |   |
| <div className="path-line"><span className="path-prefix">view_browse_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if view_browse_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">view_naming_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerViewNamingPattern</span></div> |   |
| <div className="path-line"><span className="path-prefix">view_naming_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if view_naming_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
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
    "LookerNamingPattern": {
      "additionalProperties": false,
      "properties": {
        "pattern": {
          "title": "Pattern",
          "type": "string"
        }
      },
      "required": [
        "pattern"
      ],
      "title": "LookerNamingPattern",
      "type": "object"
    },
    "LookerViewNamingPattern": {
      "additionalProperties": false,
      "properties": {
        "pattern": {
          "title": "Pattern",
          "type": "string"
        }
      },
      "required": [
        "pattern"
      ],
      "title": "LookerViewNamingPattern",
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
    },
    "TransportOptionsConfig": {
      "additionalProperties": false,
      "properties": {
        "timeout": {
          "title": "Timeout",
          "type": "integer"
        },
        "headers": {
          "additionalProperties": {
            "type": "string"
          },
          "title": "Headers",
          "type": "object"
        }
      },
      "required": [
        "timeout",
        "headers"
      ],
      "title": "TransportOptionsConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
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
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
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
      "description": ""
    },
    "explore_naming_pattern": {
      "$ref": "#/$defs/LookerNamingPattern",
      "default": {
        "pattern": "{model}.explore.{name}"
      },
      "description": "Pattern for providing dataset names to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name']"
    },
    "explore_browse_pattern": {
      "$ref": "#/$defs/LookerNamingPattern",
      "default": {
        "pattern": "/Explore/{model}"
      },
      "description": "Pattern for providing browse paths to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name']"
    },
    "view_naming_pattern": {
      "$ref": "#/$defs/LookerViewNamingPattern",
      "default": {
        "pattern": "{project}.view.{name}"
      },
      "description": "Pattern for providing dataset names to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name', 'file_path', 'folder_path']"
    },
    "view_browse_pattern": {
      "$ref": "#/$defs/LookerViewNamingPattern",
      "default": {
        "pattern": "/Develop/{project}/{folder_path}"
      },
      "description": "Pattern for providing browse paths to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name', 'file_path', 'folder_path']"
    },
    "tag_measures_and_dimensions": {
      "default": true,
      "description": "When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column.",
      "title": "Tag Measures And Dimensions",
      "type": "boolean"
    },
    "extract_column_level_lineage": {
      "default": true,
      "description": "When enabled, extracts column-level lineage from Views and Explores",
      "title": "Extract Column Level Lineage",
      "type": "boolean"
    },
    "client_id": {
      "description": "Looker API client id.",
      "title": "Client Id",
      "type": "string"
    },
    "client_secret": {
      "description": "Looker API client secret.",
      "title": "Client Secret",
      "type": "string"
    },
    "base_url": {
      "description": "Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.",
      "title": "Base Url",
      "type": "string"
    },
    "transport_options": {
      "anyOf": [
        {
          "$ref": "#/$defs/TransportOptionsConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client"
    },
    "max_retries": {
      "default": 3,
      "description": "Number of retries for Looker API calls",
      "title": "Max Retries",
      "type": "integer"
    },
    "dashboard_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Patterns for selecting dashboard ids that are to be included"
    },
    "chart_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Patterns for selecting chart ids that are to be included"
    },
    "include_deleted": {
      "default": false,
      "description": "Whether to include deleted dashboards and looks.",
      "title": "Include Deleted",
      "type": "boolean"
    },
    "extract_owners": {
      "default": true,
      "description": "When enabled, extracts ownership from Looker directly. When disabled, ownership is left empty for dashboards and charts.",
      "title": "Extract Owners",
      "type": "boolean"
    },
    "strip_user_ids_from_email": {
      "default": false,
      "description": "When enabled, converts Looker user emails of the form name@domain.com to urn:li:corpuser:name when assigning ownership",
      "title": "Strip User Ids From Email",
      "type": "boolean"
    },
    "skip_personal_folders": {
      "default": false,
      "description": "Whether to skip ingestion of dashboards in personal folders. Setting this to True will only ingest dashboards in the Shared folder space.",
      "title": "Skip Personal Folders",
      "type": "boolean"
    },
    "max_threads": {
      "description": "Max parallelism for Looker API calls. Defaults to cpuCount or 40",
      "title": "Max Threads",
      "type": "integer"
    },
    "external_base_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Optional URL to use when constructing external URLs to Looker if the `base_url` is not the correct one to use. For example, `https://looker-public.company.com`. If not provided, the external base URL will default to `base_url`.",
      "title": "External Base Url"
    },
    "extract_usage_history": {
      "default": true,
      "description": "Whether to ingest usage statistics for dashboards. Setting this to True will query looker system activity explores to fetch historical dashboard usage.",
      "title": "Extract Usage History",
      "type": "boolean"
    },
    "extract_usage_history_for_interval": {
      "default": "30 days",
      "description": "Used only if extract_usage_history is set to True. Interval to extract looker dashboard usage history for. See https://docs.looker.com/reference/filter-expressions#date_and_time.",
      "title": "Extract Usage History For Interval",
      "type": "string"
    },
    "extract_embed_urls": {
      "default": true,
      "description": "Produce URLs used to render Looker Explores as Previews inside of DataHub UI. Embeds must be enabled inside of Looker to use this feature.",
      "title": "Extract Embed Urls",
      "type": "boolean"
    },
    "extract_independent_looks": {
      "default": false,
      "description": "Extract looks which are not part of any Dashboard. To enable this flag the stateful_ingestion should also be enabled.",
      "title": "Extract Independent Looks",
      "type": "boolean"
    },
    "emit_used_explores_only": {
      "default": true,
      "description": "When enabled, only explores that are used by a Dashboard/Look will be ingested.",
      "title": "Emit Used Explores Only",
      "type": "boolean"
    },
    "include_platform_instance_in_urns": {
      "default": false,
      "description": "When enabled, platform instance will be added in dashboard and chart urn.",
      "title": "Include Platform Instance In Urns",
      "type": "boolean"
    },
    "folder_path_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Allow or deny dashboards from specific folders using their fully qualified paths. For example: \ndeny: \n - Shared/deprecated \nThis pattern will deny the ingestion of all dashboards and looks within the Shared/deprecated folder. \nallow: \n - Shared/sales \nThis pattern will allow only the ingestion of dashboards within the Shared/sales folder. \nTo get the correct path from Looker, take the folder hierarchy shown in the UI and join it with slashes. For example, Shared -> Customer Reports -> Sales becomes Shared/Customer Reports/Sales. Dashboards will only be ingested if they're allowed by both this config and dashboard_pattern."
    }
  },
  "required": [
    "client_id",
    "client_secret",
    "base_url"
  ],
  "title": "LookerDashboardSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.looker.looker_source.LookerDashboardSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/looker/looker_source.py)



## Module `lookml`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - LookML Project. |
| Column-level Lineage | ✅ | Enabled by default, configured using `extract_column_level_lineage`. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Use the `platform_instance` and `connection_to_platform_map` fields. |
| Table-Level Lineage | ✅ | Supported by default. |


This plugin extracts the following:
- LookML views from model files in a project
- Name, upstream table names, metadata for dimensions, measures, and dimension groups attached as tags
- If API integration is enabled (recommended), resolves table and view names by calling the Looker API, otherwise supports offline resolution of these names.

:::note
To get complete Looker metadata integration (including Looker dashboards and charts and lineage to the underlying Looker views, you must ALSO use the `looker` source module.
:::

### Prerequisites

#### [Recommended] Create a GitHub Deploy Key

To use LookML ingestion through the UI, or automate github checkout through the cli, you must set up a GitHub deploy key for your Looker GitHub repository. Read [this](https://docs.github.com/en/developers/overview/managing-deploy-keys#deploy-keys) document for how to set up deploy keys for your Looker git repo.

In a nutshell, there are three steps:

1. Generate a private-public ssh key pair. This will typically generate two files, e.g. looker_datahub_deploy_key (this is the private key) and looker_datahub_deploy_key.pub (this is the public key). Do not add a passphrase.
   ![Image](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/gitssh/ssh-key-generation.png)

2. Add the public key to your Looker git repo as a deploy key with read access (no need to provision write access). Follow the guide [here](https://docs.github.com/en/developers/overview/managing-deploy-keys#deploy-keys) for that.
   ![Image](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/gitssh/git-deploy-key.png)

3. Make note of the private key file, you will need to paste the contents of the file into the **GitHub Deploy Key** field later while setting up [ingestion using the UI](#ui-based-ingestion-recommended-for-ease-of-use).

### Setup your connection mapping

The connection mapping enables DataHub to accurately generate lineage to your upstream warehouse.
It maps Looker connection names to the platform and database that they're pointing to.

There's two ways to configure this:

1. Provide Looker **admin** API credentials, and we'll automatically map lineage correctly. Details on how to do this are below.
2. Manually populate the `connection_to_platform_map` and `project_name` configuration fields. See the starter recipe for an example of what this should look like.

#### [Optional] Create an API key with admin privileges

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.
You need to ensure that the API key is attached to a user that has Admin privileges.

If you don't want to provide admin API credentials, you can manually populate the `connection_to_platform_map` and `project_name` in the ingestion configuration.

### Ingestion Options

You have 3 options for controlling where your ingestion of LookML is run.

- The DataHub UI (recommended for the easiest out-of-the-box experience)
- As a GitHub Action (recommended to ensure that you have the freshest metadata pushed on change)
- Using the CLI (scheduled via an orchestrator like Airflow)

Read on to learn more about these options.

### UI-based Ingestion [Recommended for ease of use]

To ingest LookML metadata through the UI, you must set up a GitHub deploy key using the instructions in the section [above](#recommended-create-a-github-deploy-key). Once that is complete, you can follow the on-screen instructions to set up a LookML source using the Ingestion page.
The following video shows you how to ingest LookML metadata through the UI and find the relevant information from your Looker account.

<div style={{ position: "relative", paddingBottom: "56.25%", height: 0 }}>
  <iframe
    src="https://www.loom.com/embed/c66dd625de7f48b39005e0eb9c345f5a"
    frameBorder={0}
    webkitallowfullscreen=""
    mozallowfullscreen=""
    allowFullScreen=""
    style={{
      position: "absolute",
      top: 0,
      left: 0,
      width: "100%",
      height: "100%"
    }}
  />
</div>

### GitHub Action based Ingestion [Recommended for push-based integration]

You can set up ingestion using a GitHub Action to push metadata whenever your main Looker GitHub repo changes.
The following sample GitHub action file can be modified to emit LookML metadata whenever there is a change to your repository. This ensures that metadata is already fresh and up to date.

#### Sample GitHub Action

Drop this file into your `.github/workflows` directory inside your Looker GitHub repo.
You need to set up the following secrets in your GitHub repository to get this workflow to work:

- DATAHUB_GMS_HOST: The endpoint where your DataHub host is running
- DATAHUB_TOKEN: An authentication token provisioned for DataHub ingestion
- LOOKER_BASE_URL: The base url where your Looker assets are hosted (e.g. <https://acryl.cloud.looker.com>)
- LOOKER_CLIENT_ID: A provisioned Looker Client ID
- LOOKER_CLIENT_SECRET: A provisioned Looker Client Secret

```yml
name: lookml metadata upload
on:
  # Note that this action only runs on pushes to your main branch. If you want to also
  # run on pull requests, we'd recommend running datahub ingest with the `--dry-run` flag.
  push:
    branches:
      - main
  release:
    types: [published, edited]
  workflow_dispatch:

jobs:
  lookml-metadata-upload:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - name: Run LookML ingestion
        run: |
          pip install 'acryl-datahub[lookml,datahub-rest]'
          cat << EOF > lookml_ingestion.yml
          # LookML ingestion configuration.
          # This is a full ingestion recipe, and supports all config options that the LookML source supports.
          source:
            type: "lookml"
            config:
              base_folder: ${{ github.workspace }}
              parse_table_names_from_sql: true
              git_info:
                repo: ${{ github.repository }}
                branch: ${{ github.ref }}
              # Options
              #connection_to_platform_map:
              #  connection-name:
              #    platform: platform-name (e.g. snowflake)
              #    default_db: default-db-name (e.g. DEMO_PIPELINE)
              api:
                client_id: ${LOOKER_CLIENT_ID}
                client_secret: ${LOOKER_CLIENT_SECRET}
                base_url: ${LOOKER_BASE_URL}
          sink:
            type: datahub-rest
            config:
              server: ${DATAHUB_GMS_URL}
              token: ${DATAHUB_GMS_TOKEN}
          EOF
          datahub ingest -c lookml_ingestion.yml
        env:
          DATAHUB_GMS_URL: ${{ secrets.DATAHUB_GMS_URL }}
          DATAHUB_GMS_TOKEN: ${{ secrets.DATAHUB_GMS_TOKEN }}
          LOOKER_BASE_URL: ${{ secrets.LOOKER_BASE_URL }}
          LOOKER_CLIENT_ID: ${{ secrets.LOOKER_CLIENT_ID }}
          LOOKER_CLIENT_SECRET: ${{ secrets.LOOKER_CLIENT_SECRET }}
```

If you want to ingest lookml using the **datahub** cli directly, read on for instructions and configuration details.

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "lookml"
  config:
    # GitHub Coordinates: Used to check out the repo locally and add github links on the dataset's entity page.
    git_info:
      repo: org/repo-name
      deploy_key_file: ${LOOKER_DEPLOY_KEY_FILE} # file containing the private ssh key for a deploy key for the looker git repo

    # Coordinates
    # base_folder: /path/to/model/files ## Optional if you are not able to provide a GitHub deploy key

    # Options
    api:
      # Coordinates for your looker instance
      base_url: "https://YOUR_INSTANCE.cloud.looker.com"

      # Credentials for your Looker connection (https://docs.looker.com/reference/api-and-integration/api-auth)
      client_id: ${LOOKER_CLIENT_ID}
      client_secret: ${LOOKER_CLIENT_SECRET}

    # Alternative to API section above if you want a purely file-based ingestion with no api calls to Looker or if you want to provide platform_instance ids for your connections
    # project_name: PROJECT_NAME # See (https://docs.looker.com/data-modeling/getting-started/how-project-works) to understand what is your project name
    # connection_to_platform_map:
    #   connection_name_1:
    #     platform: snowflake # bigquery, hive, etc
    #     default_db: DEFAULT_DATABASE. # the default database configured for this connection
    #     default_schema: DEFAULT_SCHEMA # the default schema configured for this connection
    #     platform_instance: snow_warehouse # optional
    #     platform_env: PROD  # optional
    #   connection_name_2:
    #     platform: bigquery # snowflake, hive, etc
    #     default_db: DEFAULT_DATABASE. # the default database configured for this connection
    #     default_schema: DEFAULT_SCHEMA # the default schema configured for this connection
    #     platform_instance: bq_warehouse # optional
    #     platform_env: DEV  # optional
# Default sink is datahub-rest and doesn't need to be configured
# See https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub for customization options


```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">base_folder</span></div> <div className="type-name-line"><span className="type-name">One of string(directory-path), null</span></div> | Required if not providing github configuration and deploy keys. A pointer to a local directory (accessible to the ingestion system) where the root of the LookML repo has been checked out (typically via a git clone). This is typically the root folder where the `*.model.lkml` and `*.view.lkml` files are stored. e.g. If you have checked out your LookML repo under `/Users/jdoe/workspace/my-lookml-repo`, then set `base_folder` to `/Users/jdoe/workspace/my-lookml-repo`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">emit_reachable_views_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, only views that are reachable from explores defined in the model files are emitted. If set to False, all views imported in model files are emitted. Views that are unreachable i.e. not explicitly defined in the model files are currently not emitted however reported as warning for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, extracts column-level lineage from Views and Explores <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">liquid_variables</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | A dictionary containing Liquid variables with their corresponding values, utilized in SQL-defined derived views. The Liquid template will be resolved in view.derived_table.sql and view.sql_table_name. Defaults to an empty dictionary. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">looker_environment</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "prod", "dev" <div className="default-line default-line-with-docs">Default: <span className="default-value">prod</span></div> |
| <div className="path-line"><span className="path-main">lookml_constants</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">max_file_snippet_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | When extracting the view definition from a lookml file, the maximum number of characters to extract. <div className="default-line default-line-with-docs">Default: <span className="default-value">512000</span></div> |
| <div className="path-line"><span className="path-main">parse_table_names_from_sql</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | See note below. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">populate_sql_logic_for_missing_descriptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, field descriptions will include the sql logic for computed fields if descriptions are missing <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">process_isolation_for_sql_parsing</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, sql parsing will be executed in a separate process to prevent memory leaks. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">process_refinements</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, looker refinement will be processed to adapt an existing view. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">project_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Required if you don't specify the `api` section. The project name within which all the model files live. See (https://docs.looker.com/data-modeling/getting-started/how-project-works) to understand what the Looker project name should be. The simplest way to see your projects is to click on `Develop` followed by `Manage LookML Projects` in the Looker application. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tag_measures_and_dimensions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">use_api_cache_for_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, uses Looker API server-side caching for query execution. Requires 'api' configuration to be provided. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">use_api_for_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, uses Looker API to get SQL representation of views for lineage parsing instead of parsing LookML files directly. Requires 'api' configuration to be provided.Coverage of regex based lineage extraction has limitations, it only supportes ${TABLE}.column_name syntax, See (https://cloud.google.com/looker/docs/reference/param-field-sql#sql_for_dimensions) tounderstand the other substitutions and cross-references allowed in LookML. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">api</span></div> <div className="type-name-line"><span className="type-name">One of LookerAPIConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">api.</span><span className="path-main">base_url</span>&nbsp;<abbr title="Required if api is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.  |
| <div className="path-line"><span className="path-prefix">api.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if api is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Looker API client id.  |
| <div className="path-line"><span className="path-prefix">api.</span><span className="path-main">client_secret</span>&nbsp;<abbr title="Required if api is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Looker API client secret.  |
| <div className="path-line"><span className="path-prefix">api.</span><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of retries for Looker API calls <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-prefix">api.</span><span className="path-main">transport_options</span></div> <div className="type-name-line"><span className="type-name">One of TransportOptionsConfig, null</span></div> | Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">api.transport_options.</span><span className="path-main">headers</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">api.transport_options.</span><span className="path-main">timeout</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">integer</span></div> |   |
| <div className="path-line"><span className="path-main">connection_to_platform_map</span></div> <div className="type-name-line"><span className="type-name">One of LookerConnectionDefinition, null</span></div> | A mapping of [Looker connection names](https://docs.looker.com/reference/model-params/connection-for-model) to DataHub platform, database, and schema values. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if connection_to_platform_map is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">default_db</span>&nbsp;<abbr title="Required if connection_to_platform_map is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform_env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The environment that the platform is located in. Leaving this empty will inherit defaults from the top level Looker configuration <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">explore_browse_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div> |   |
| <div className="path-line"><span className="path-prefix">explore_browse_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if explore_browse_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">explore_naming_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div> |   |
| <div className="path-line"><span className="path-prefix">explore_naming_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if explore_naming_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">git_info</span></div> <div className="type-name-line"><span className="type-name">One of GitInfo, null</span></div> | Reference to your git location. If present, supplies handy links to your lookml on the dataset entity page. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">repo</span>&nbsp;<abbr title="Required if git_info is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.  |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">branch</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Branch on which your files live by default. Typically main or master. This can also be a commit hash. <div className="default-line default-line-with-docs">Default: <span className="default-value">main</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">deploy_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">deploy_key_file</span></div> <div className="type-name-line"><span className="type-name">One of string(file-path), null</span></div> | A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string. We expect the key not have a passphrase. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">repo_ssh_locator</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">url_subdir</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Prefix to prepend when generating URLs for files - useful when files are in a subdirectory. Only affects URL generation, not git operations. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">url_template</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">model_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">model_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">project_dependencies</span></div> <div className="type-name-line"><span className="type-name">One of map(str,union)(directory-path), map(str,union)</span></div> |   |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">repo</span>&nbsp;<abbr title="Required if project_dependencies is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.  |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">branch</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Branch on which your files live by default. Typically main or master. This can also be a commit hash. <div className="default-line default-line-with-docs">Default: <span className="default-value">main</span></div> |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">deploy_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">deploy_key_file</span></div> <div className="type-name-line"><span className="type-name">One of string(file-path), null</span></div> | A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string. We expect the key not have a passphrase. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">repo_ssh_locator</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">url_subdir</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Prefix to prepend when generating URLs for files - useful when files are in a subdirectory. Only affects URL generation, not git operations. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">url_template</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">transport_options</span></div> <div className="type-name-line"><span className="type-name">One of TransportOptionsConfig, null</span></div> | Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">transport_options.</span><span className="path-main">headers</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">transport_options.</span><span className="path-main">timeout</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">integer</span></div> |   |
| <div className="path-line"><span className="path-main">view_browse_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerViewNamingPattern</span></div> |   |
| <div className="path-line"><span className="path-prefix">view_browse_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if view_browse_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">view_naming_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerViewNamingPattern</span></div> |   |
| <div className="path-line"><span className="path-prefix">view_naming_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if view_naming_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
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
    "GitInfo": {
      "additionalProperties": false,
      "description": "A reference to a Git repository, including a deploy key that can be used to clone it.",
      "properties": {
        "repo": {
          "description": "Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.",
          "title": "Repo",
          "type": "string"
        },
        "branch": {
          "default": "main",
          "description": "Branch on which your files live by default. Typically main or master. This can also be a commit hash.",
          "title": "Branch",
          "type": "string"
        },
        "url_subdir": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Prefix to prepend when generating URLs for files - useful when files are in a subdirectory. Only affects URL generation, not git operations.",
          "title": "Url Subdir"
        },
        "url_template": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path}",
          "title": "Url Template"
        },
        "deploy_key_file": {
          "anyOf": [
            {
              "format": "file-path",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string. We expect the key not have a passphrase.",
          "title": "Deploy Key File"
        },
        "deploy_key": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key.",
          "title": "Deploy Key"
        },
        "repo_ssh_locator": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts.",
          "title": "Repo Ssh Locator"
        }
      },
      "required": [
        "repo"
      ],
      "title": "GitInfo",
      "type": "object"
    },
    "LookerAPIConfig": {
      "additionalProperties": false,
      "properties": {
        "client_id": {
          "description": "Looker API client id.",
          "title": "Client Id",
          "type": "string"
        },
        "client_secret": {
          "description": "Looker API client secret.",
          "title": "Client Secret",
          "type": "string"
        },
        "base_url": {
          "description": "Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.",
          "title": "Base Url",
          "type": "string"
        },
        "transport_options": {
          "anyOf": [
            {
              "$ref": "#/$defs/TransportOptionsConfig"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client"
        },
        "max_retries": {
          "default": 3,
          "description": "Number of retries for Looker API calls",
          "title": "Max Retries",
          "type": "integer"
        }
      },
      "required": [
        "client_id",
        "client_secret",
        "base_url"
      ],
      "title": "LookerAPIConfig",
      "type": "object"
    },
    "LookerConnectionDefinition": {
      "additionalProperties": false,
      "properties": {
        "platform": {
          "title": "Platform",
          "type": "string"
        },
        "default_db": {
          "title": "Default Db",
          "type": "string"
        },
        "default_schema": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Default Schema"
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
          "title": "Platform Instance"
        },
        "platform_env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The environment that the platform is located in. Leaving this empty will inherit defaults from the top level Looker configuration",
          "title": "Platform Env"
        }
      },
      "required": [
        "platform",
        "default_db"
      ],
      "title": "LookerConnectionDefinition",
      "type": "object"
    },
    "LookerNamingPattern": {
      "additionalProperties": false,
      "properties": {
        "pattern": {
          "title": "Pattern",
          "type": "string"
        }
      },
      "required": [
        "pattern"
      ],
      "title": "LookerNamingPattern",
      "type": "object"
    },
    "LookerViewNamingPattern": {
      "additionalProperties": false,
      "properties": {
        "pattern": {
          "title": "Pattern",
          "type": "string"
        }
      },
      "required": [
        "pattern"
      ],
      "title": "LookerViewNamingPattern",
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
    },
    "TransportOptionsConfig": {
      "additionalProperties": false,
      "properties": {
        "timeout": {
          "title": "Timeout",
          "type": "integer"
        },
        "headers": {
          "additionalProperties": {
            "type": "string"
          },
          "title": "Headers",
          "type": "object"
        }
      },
      "required": [
        "timeout",
        "headers"
      ],
      "title": "TransportOptionsConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
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
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
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
      "description": ""
    },
    "explore_naming_pattern": {
      "$ref": "#/$defs/LookerNamingPattern",
      "default": {
        "pattern": "{model}.explore.{name}"
      },
      "description": "Pattern for providing dataset names to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name']"
    },
    "explore_browse_pattern": {
      "$ref": "#/$defs/LookerNamingPattern",
      "default": {
        "pattern": "/Explore/{model}"
      },
      "description": "Pattern for providing browse paths to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name']"
    },
    "view_naming_pattern": {
      "$ref": "#/$defs/LookerViewNamingPattern",
      "default": {
        "pattern": "{project}.view.{name}"
      },
      "description": "Pattern for providing dataset names to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name', 'file_path', 'folder_path']"
    },
    "view_browse_pattern": {
      "$ref": "#/$defs/LookerViewNamingPattern",
      "default": {
        "pattern": "/Develop/{project}/{folder_path}"
      },
      "description": "Pattern for providing browse paths to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name', 'file_path', 'folder_path']"
    },
    "tag_measures_and_dimensions": {
      "default": true,
      "description": "When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column.",
      "title": "Tag Measures And Dimensions",
      "type": "boolean"
    },
    "extract_column_level_lineage": {
      "default": true,
      "description": "When enabled, extracts column-level lineage from Views and Explores",
      "title": "Extract Column Level Lineage",
      "type": "boolean"
    },
    "git_info": {
      "anyOf": [
        {
          "$ref": "#/$defs/GitInfo"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Reference to your git location. If present, supplies handy links to your lookml on the dataset entity page."
    },
    "base_folder": {
      "anyOf": [
        {
          "format": "directory-path",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Required if not providing github configuration and deploy keys. A pointer to a local directory (accessible to the ingestion system) where the root of the LookML repo has been checked out (typically via a git clone). This is typically the root folder where the `*.model.lkml` and `*.view.lkml` files are stored. e.g. If you have checked out your LookML repo under `/Users/jdoe/workspace/my-lookml-repo`, then set `base_folder` to `/Users/jdoe/workspace/my-lookml-repo`.",
      "title": "Base Folder"
    },
    "project_dependencies": {
      "additionalProperties": {
        "anyOf": [
          {
            "format": "directory-path",
            "type": "string"
          },
          {
            "$ref": "#/$defs/GitInfo"
          }
        ]
      },
      "default": {},
      "description": "A map of project_name to local directory (accessible to the ingestion system) or Git credentials. Every local_dependencies or private remote_dependency listed in the main project's manifest.lkml file should have a corresponding entry here.If a deploy key is not provided, the ingestion system will use the same deploy key as the main project. ",
      "title": "Project Dependencies",
      "type": "object"
    },
    "connection_to_platform_map": {
      "anyOf": [
        {
          "additionalProperties": {
            "$ref": "#/$defs/LookerConnectionDefinition"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "A mapping of [Looker connection names](https://docs.looker.com/reference/model-params/connection-for-model) to DataHub platform, database, and schema values.",
      "title": "Connection To Platform Map"
    },
    "model_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "List of regex patterns for LookML models to include in the extraction."
    },
    "view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "List of regex patterns for LookML views to include in the extraction."
    },
    "parse_table_names_from_sql": {
      "default": true,
      "description": "See note below.",
      "title": "Parse Table Names From Sql",
      "type": "boolean"
    },
    "use_api_for_view_lineage": {
      "default": false,
      "description": "When enabled, uses Looker API to get SQL representation of views for lineage parsing instead of parsing LookML files directly. Requires 'api' configuration to be provided.Coverage of regex based lineage extraction has limitations, it only supportes ${TABLE}.column_name syntax, See (https://cloud.google.com/looker/docs/reference/param-field-sql#sql_for_dimensions) tounderstand the other substitutions and cross-references allowed in LookML.",
      "title": "Use Api For View Lineage",
      "type": "boolean"
    },
    "use_api_cache_for_view_lineage": {
      "default": false,
      "description": "When enabled, uses Looker API server-side caching for query execution. Requires 'api' configuration to be provided.",
      "title": "Use Api Cache For View Lineage",
      "type": "boolean"
    },
    "api": {
      "anyOf": [
        {
          "$ref": "#/$defs/LookerAPIConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "project_name": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Required if you don't specify the `api` section. The project name within which all the model files live. See (https://docs.looker.com/data-modeling/getting-started/how-project-works) to understand what the Looker project name should be. The simplest way to see your projects is to click on `Develop` followed by `Manage LookML Projects` in the Looker application.",
      "title": "Project Name"
    },
    "transport_options": {
      "anyOf": [
        {
          "$ref": "#/$defs/TransportOptionsConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client"
    },
    "max_file_snippet_length": {
      "default": 512000,
      "description": "When extracting the view definition from a lookml file, the maximum number of characters to extract.",
      "title": "Max File Snippet Length",
      "type": "integer"
    },
    "emit_reachable_views_only": {
      "default": true,
      "description": "When enabled, only views that are reachable from explores defined in the model files are emitted. If set to False, all views imported in model files are emitted. Views that are unreachable i.e. not explicitly defined in the model files are currently not emitted however reported as warning for debugging purposes.",
      "title": "Emit Reachable Views Only",
      "type": "boolean"
    },
    "populate_sql_logic_for_missing_descriptions": {
      "default": false,
      "description": "When enabled, field descriptions will include the sql logic for computed fields if descriptions are missing",
      "title": "Populate Sql Logic For Missing Descriptions",
      "type": "boolean"
    },
    "process_isolation_for_sql_parsing": {
      "default": false,
      "description": "When enabled, sql parsing will be executed in a separate process to prevent memory leaks.",
      "title": "Process Isolation For Sql Parsing",
      "type": "boolean"
    },
    "process_refinements": {
      "default": false,
      "description": "When enabled, looker refinement will be processed to adapt an existing view.",
      "title": "Process Refinements",
      "type": "boolean"
    },
    "liquid_variables": {
      "additionalProperties": true,
      "default": {},
      "description": "A dictionary containing Liquid variables with their corresponding values, utilized in SQL-defined derived views. The Liquid template will be resolved in view.derived_table.sql and view.sql_table_name. Defaults to an empty dictionary.",
      "title": "Liquid Variables",
      "type": "object"
    },
    "lookml_constants": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "A dictionary containing LookML constants (`@{constant_name}`) and their values. If a constant is defined in the `manifest.lkml` file, its value will be used. If not found in the manifest, the value from this config will be used instead. Defaults to an empty dictionary.",
      "title": "Lookml Constants",
      "type": "object"
    },
    "looker_environment": {
      "default": "prod",
      "description": "A looker prod or dev environment. It helps to evaluate looker if comments i.e. -- if prod --. All if comments are evaluated to true for configured looker_environment value",
      "enum": [
        "prod",
        "dev"
      ],
      "title": "Looker Environment",
      "type": "string"
    }
  },
  "title": "LookMLSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>

### Configuration Notes

1. Handling Liquid Templates

   If a view contains a liquid template, for example:

   ```
   sql_table_name: {{ user_attributes['db'] }}.kafka_streaming.events
   ```

   where `db=ANALYTICS_PROD`, you need to specify the values of those variables in the liquid_variables configuration as shown below:

   ```yml
   liquid_variables:
     user_attributes:
       db: ANALYTICS_PROD
   ```

2. Resolving LookML Constants

   If a view contains a LookML constant, for example:

   ```
   sql_table_name: @{db}.kafka_streaming.events;
   ```

   Ingestion attempts to resolve it's value by looking at project manifest files

   ```yml
   manifest.lkml
     constant: db {
         value: "ANALYTICS_PROD"
     }
   ```

   - If the constant's value is not resolved or incorrectly resolved, you can specify `lookml_constants` configuration in ingestion recipe as shown below. The constant value in recipe takes precedence over constant values resolved from manifest.

     ```yml
     lookml_constants:
       db: ANALYTICS_PROD
     ```

**Liquid Template Support Limits:**

- Supported: Simple variable interpolation (`{{ var }}`) and condition directives (`{% condition filter_name %} field {% endcondition %}`)
- Unsupported: Conditional logic with `if`/`else`/`endif` and custom Looker tags like `date_start`, `date_end`, and `parameter`

Unsupported templates may cause lineage extraction to fail for some assets.

**Additional Notes**

Although liquid variables and LookML constants can be used anywhere in LookML code, their values are currently resolved only for LookML views by DataHub LookML ingestion. This behavior is sufficient since LookML ingestion processes only views and their upstream dependencies.

### Multi-Project LookML (Advanced)

Looker projects support organization as multiple git repos, with [remote includes that can refer to projects that are stored in a different repo](https://cloud.google.com/looker/docs/importing-projects#include_files_from_an_imported_project). If your Looker implementation uses multi-project setup, you can configure the LookML source to pull in metadata from your remote projects as well.

If you are using local or remote dependencies, you will see include directives in your lookml files that look like this:

```
include: "//e_flights/views/users.view.lkml"
include: "//e_commerce/public/orders.view.lkml"
```

Also, you will see projects that are being referred to listed in your `manifest.lkml` file. Something like this:

```
project_name: this_project

local_dependency: {
    project: "my-remote-project"
}

remote_dependency: ga_360_block {
  url: "https://github.com/llooker/google_ga360"
  ref: "0bbbef5d8080e88ade2747230b7ed62418437c21"
}
```

To ingest Looker repositories that are including files defined in other projects, you will need to use the `project_dependencies` directive within the configuration section.
Consider the following scenario:

- Your primary project refers to a remote project called `my_remote_project`
- The remote project is homed in the GitHub repo `my_org/my_remote_project`
- You have provisioned a GitHub deploy key and stored the credential in the environment variable (or UI secret), `${MY_REMOTE_PROJECT_DEPLOY_KEY}`

In this case, you can add this section to your recipe to activate multi-project LookML ingestion.

```
source:
  type: lookml
  config:
    ... other config variables

    project_dependencies:
      my_remote_project:
         repo: my_org/my_remote_project
         deploy_key: ${MY_REMOTE_PROJECT_DEPLOY_KEY}
```

Under the hood, DataHub will check out your remote repository using the provisioned deploy key, and use it to navigate includes that you have in the model files from your primary project.

If you have the remote project checked out locally, and do not need DataHub to clone the project for you, you can provide DataHub directly with the path to the project like the config snippet below:

```
source:
  type: lookml
  config:
    ... other config variables

    project_dependencies:
      my_remote_project: /path/to/local_git_clone_of_remote_project
```

:::note

This is not the same as ingesting the remote project as a primary Looker project because DataHub will not be processing the model files that might live in the remote project. If you want to additionally include the views accessible via the models in the remote project, create a second recipe where your remote project is the primary project.

:::

### Debugging LookML Parsing Errors

If you see messages like `my_file.view.lkml': "failed to load view file: Unable to find a matching expression for '<literal>' on line 5"` in the failure logs, it indicates a parsing error for the LookML file.

The first thing to check is that the Looker IDE can validate the file without issues. You can check this by clicking this "Validate LookML" button in the IDE when in development mode.

If that's not the issue, it might be because DataHub's parser, which is based on the [joshtemple/lkml](https://github.com/joshtemple/lkml) library, is slightly more strict than the official Looker parser.
Note that there's currently only one known discrepancy between the two parsers, and it's related to using [leading colons in blocks](https://github.com/joshtemple/lkml/issues/90).

To check if DataHub can parse your LookML file syntax, you can use the `lkml` CLI tool. If this raises an exception, DataHub will fail to parse the file.

```sh
pip install lkml

lkml path/to/my_file.view.lkml
```

### Code Coordinates
- Class Name: `datahub.ingestion.source.looker.lookml_source.LookMLSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/looker/lookml_source.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Looker, feel free to ping us on [our Slack](https://datahub.com/slack).
