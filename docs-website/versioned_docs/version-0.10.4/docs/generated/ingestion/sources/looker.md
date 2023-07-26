---
sidebar_position: 24
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

| Capability                                          | Status | Notes                                                        |
| --------------------------------------------------- | ------ | ------------------------------------------------------------ |
| Dataset Usage                                       | ✅     | Enabled by default, configured using `extract_usage_history` |
| Descriptions                                        | ✅     | Enabled by default                                           |
| Extract Ownership                                   | ✅     | Enabled by default, configured using `extract_owners`        |
| [Platform Instance](../../../platform-instances.md) | ❌     | Not supported                                                |

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
  <img width="70%" src="https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs//looker_datahub_permission_set.png"/>
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

#### Install the Plugin

```shell
pip install 'acryl-datahub[looker]'
```

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
# sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                                                          | Description                                                                                                                                                                                                                                                                                                                                   |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">base_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                             | Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.                                                                                                                                         |
| <div className="path-line"><span className="path-main">client_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                            | Looker API client id.                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">client_secret</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                        | Looker API client secret.                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">actor</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                      | This config is deprecated in favor of `extract_owners`. Previously, was the actor to use in ownership properties of ingested metadata.                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">external_base_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                          | Optional URL to use when constructing external URLs to Looker if the `base_url` is not the correct one to use. For example, `https://looker-public.company.com`. If not provided, the external base URL will default to `base_url`.                                                                                                           |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                              | When enabled, extracts column-level lineage from Views and Explores <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                           |
| <div className="path-line"><span className="path-main">extract_embed_urls</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                        | Produce URLs used to render Looker Explores as Previews inside of DataHub UI. Embeds must be enabled inside of Looker to use this feature. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                    |
| <div className="path-line"><span className="path-main">extract_independent_looks</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                 | Extract looks which are not part of any Dashboard. To enable this flag the stateful_ingestion should also be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                        |
| <div className="path-line"><span className="path-main">extract_owners</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                            | When enabled, extracts ownership from Looker directly. When disabled, ownership is left empty for dashboards and charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                      |
| <div className="path-line"><span className="path-main">extract_usage_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                     | Whether to ingest usage statistics for dashboards. Setting this to True will query looker system activity explores to fetch historical dashboard usage. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                       |
| <div className="path-line"><span className="path-main">extract_usage_history_for_interval</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                         | Used only if extract_usage_history is set to True. Interval to extract looker dashboard usage history for. See https://docs.looker.com/reference/filter-expressions#date_and_time. <div className="default-line default-line-with-docs">Default: <span className="default-value">30 days</span></div>                                         |
| <div className="path-line"><span className="path-main">include_deleted</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                           | Whether to include deleted dashboards and looks. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">max_threads</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                                                                               | Max parallelism for Looker API calls. Defaults to cpuCount or 40 <div className="default-line default-line-with-docs">Default: <span className="default-value">16</span></div>                                                                                                                                                                |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                          | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">platform_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                              | Default platform name. Don't change. <div className="default-line default-line-with-docs">Default: <span className="default-value">looker</span></div>                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">skip_personal_folders</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                     | Whether to skip ingestion of dashboards in personal folders. Setting this to True will only ingest dashboards in the Shared folder space. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                    |
| <div className="path-line"><span className="path-main">strip_user_ids_from_email</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                 | When enabled, converts Looker user emails of the form name@domain.com to urn:li:corpuser:name when assigning ownership <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                       |
| <div className="path-line"><span className="path-main">tag_measures_and_dimensions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                               | When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                               |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                        | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                          |
| <div className="path-line"><span className="path-main">chart_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                                    | Patterns for selecting chart ids that are to be included <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                      |
| <div className="path-line"><span className="path-prefix">chart_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                            |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">chart_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                             |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">chart_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                             | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">dashboard_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                                | Patterns for selecting dashboard ids that are to be included <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                  |
| <div className="path-line"><span className="path-prefix">dashboard_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                        |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">dashboard_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                         |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">dashboard_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                         | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">explore_browse_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div>                                                                                                        | Pattern for providing browse paths to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name'] <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;pattern&#x27;: &#x27;/&#123;env&#125;/&#123;platform&#125;/&#123;project&#125;/explores&#x27;...</span></div> |
| <div className="path-line"><span className="path-prefix">explore_browse_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if explore_browse_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">explore_naming_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div>                                                                                                        | Pattern for providing dataset names to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name'] <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;pattern&#x27;: &#x27;&#123;model&#125;.explore.&#123;name&#125;&#x27;&#125;</span></div>                     |
| <div className="path-line"><span className="path-prefix">explore_naming_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if explore_naming_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">transport_options</span></div> <div className="type-name-line"><span className="type-name">TransportOptionsConfig</span></div>                                                                                                          | Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">transport_options.</span><span className="path-main">headers</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>  |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">transport_options.</span><span className="path-main">timeout</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">integer</span></div>          |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">view_browse_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div>                                                                                                           | Pattern for providing browse paths to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name'] <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;pattern&#x27;: &#x27;/&#123;env&#125;/&#123;platform&#125;/&#123;project&#125;/views&#x27;&#125;</span></div>    |
| <div className="path-line"><span className="path-prefix">view_browse_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if view_browse_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>       |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">view_naming_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div>                                                                                                           | Pattern for providing dataset names to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name'] <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;pattern&#x27;: &#x27;&#123;project&#125;.view.&#123;name&#125;&#x27;&#125;</span></div>                         |
| <div className="path-line"><span className="path-prefix">view_naming_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if view_naming_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>       |                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                                                                             | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                           | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                             | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                  |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "LookerDashboardSourceConfig",
  "description": "Any source that is a primary producer of Dataset metadata should inherit this class",
  "type": "object",
  "properties": {
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "stateful_ingestion": {
      "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "explore_naming_pattern": {
      "title": "Explore Naming Pattern",
      "description": "Pattern for providing dataset names to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name']",
      "default": {
        "pattern": "{model}.explore.{name}"
      },
      "allOf": [
        {
          "$ref": "#/definitions/LookerNamingPattern"
        }
      ]
    },
    "explore_browse_pattern": {
      "title": "Explore Browse Pattern",
      "description": "Pattern for providing browse paths to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name']",
      "default": {
        "pattern": "/{env}/{platform}/{project}/explores"
      },
      "allOf": [
        {
          "$ref": "#/definitions/LookerNamingPattern"
        }
      ]
    },
    "view_naming_pattern": {
      "title": "View Naming Pattern",
      "description": "Pattern for providing dataset names to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name']",
      "default": {
        "pattern": "{project}.view.{name}"
      },
      "allOf": [
        {
          "$ref": "#/definitions/LookerNamingPattern"
        }
      ]
    },
    "view_browse_pattern": {
      "title": "View Browse Pattern",
      "description": "Pattern for providing browse paths to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name']",
      "default": {
        "pattern": "/{env}/{platform}/{project}/views"
      },
      "allOf": [
        {
          "$ref": "#/definitions/LookerNamingPattern"
        }
      ]
    },
    "tag_measures_and_dimensions": {
      "title": "Tag Measures And Dimensions",
      "description": "When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column.",
      "default": true,
      "type": "boolean"
    },
    "platform_name": {
      "title": "Platform Name",
      "description": "Default platform name. Don't change.",
      "default": "looker",
      "type": "string"
    },
    "extract_column_level_lineage": {
      "title": "Extract Column Level Lineage",
      "description": "When enabled, extracts column-level lineage from Views and Explores",
      "default": true,
      "type": "boolean"
    },
    "client_id": {
      "title": "Client Id",
      "description": "Looker API client id.",
      "type": "string"
    },
    "client_secret": {
      "title": "Client Secret",
      "description": "Looker API client secret.",
      "type": "string"
    },
    "base_url": {
      "title": "Base Url",
      "description": "Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.",
      "type": "string"
    },
    "transport_options": {
      "title": "Transport Options",
      "description": "Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client",
      "allOf": [
        {
          "$ref": "#/definitions/TransportOptionsConfig"
        }
      ]
    },
    "dashboard_pattern": {
      "title": "Dashboard Pattern",
      "description": "Patterns for selecting dashboard ids that are to be included",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "chart_pattern": {
      "title": "Chart Pattern",
      "description": "Patterns for selecting chart ids that are to be included",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "include_deleted": {
      "title": "Include Deleted",
      "description": "Whether to include deleted dashboards and looks.",
      "default": false,
      "type": "boolean"
    },
    "extract_owners": {
      "title": "Extract Owners",
      "description": "When enabled, extracts ownership from Looker directly. When disabled, ownership is left empty for dashboards and charts.",
      "default": true,
      "type": "boolean"
    },
    "actor": {
      "title": "Actor",
      "description": "This config is deprecated in favor of `extract_owners`. Previously, was the actor to use in ownership properties of ingested metadata.",
      "type": "string"
    },
    "strip_user_ids_from_email": {
      "title": "Strip User Ids From Email",
      "description": "When enabled, converts Looker user emails of the form name@domain.com to urn:li:corpuser:name when assigning ownership",
      "default": false,
      "type": "boolean"
    },
    "skip_personal_folders": {
      "title": "Skip Personal Folders",
      "description": "Whether to skip ingestion of dashboards in personal folders. Setting this to True will only ingest dashboards in the Shared folder space.",
      "default": false,
      "type": "boolean"
    },
    "max_threads": {
      "title": "Max Threads",
      "description": "Max parallelism for Looker API calls. Defaults to cpuCount or 40",
      "default": 16,
      "type": "integer"
    },
    "external_base_url": {
      "title": "External Base Url",
      "description": "Optional URL to use when constructing external URLs to Looker if the `base_url` is not the correct one to use. For example, `https://looker-public.company.com`. If not provided, the external base URL will default to `base_url`.",
      "type": "string"
    },
    "extract_usage_history": {
      "title": "Extract Usage History",
      "description": "Whether to ingest usage statistics for dashboards. Setting this to True will query looker system activity explores to fetch historical dashboard usage.",
      "default": true,
      "type": "boolean"
    },
    "extract_usage_history_for_interval": {
      "title": "Extract Usage History For Interval",
      "description": "Used only if extract_usage_history is set to True. Interval to extract looker dashboard usage history for. See https://docs.looker.com/reference/filter-expressions#date_and_time.",
      "default": "30 days",
      "type": "string"
    },
    "extract_embed_urls": {
      "title": "Extract Embed Urls",
      "description": "Produce URLs used to render Looker Explores as Previews inside of DataHub UI. Embeds must be enabled inside of Looker to use this feature.",
      "default": true,
      "type": "boolean"
    },
    "extract_independent_looks": {
      "title": "Extract Independent Looks",
      "description": "Extract looks which are not part of any Dashboard. To enable this flag the stateful_ingestion should also be enabled.",
      "default": false,
      "type": "boolean"
    }
  },
  "required": [
    "client_id",
    "client_secret",
    "base_url"
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
    },
    "LookerNamingPattern": {
      "title": "LookerNamingPattern",
      "type": "object",
      "properties": {
        "pattern": {
          "title": "Pattern",
          "type": "string"
        }
      },
      "required": [
        "pattern"
      ],
      "additionalProperties": false
    },
    "TransportOptionsConfig": {
      "title": "TransportOptionsConfig",
      "type": "object",
      "properties": {
        "timeout": {
          "title": "Timeout",
          "type": "integer"
        },
        "headers": {
          "title": "Headers",
          "type": "object",
          "additionalProperties": {
            "type": "string"
          }
        }
      },
      "required": [
        "timeout",
        "headers"
      ],
      "additionalProperties": false
    },
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
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

- Class Name: `datahub.ingestion.source.looker.looker_source.LookerDashboardSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/looker/looker_source.py)

## Module `lookml`

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                          | Status | Notes                                                               |
| --------------------------------------------------- | ------ | ------------------------------------------------------------------- |
| Column-level Lineage                                | ✅     | Enabled by default, configured using `extract_column_level_lineage` |
| [Platform Instance](../../../platform-instances.md) | ✅     | Supported using the `connection_to_platform_map`                    |
| Table-Level Lineage                                 | ✅     | Supported by default                                                |

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

1. Generate a private-public ssh key pair. This will typically generate two files, e.g. looker_datahub_deploy_key (this is the private key) and looker_datahub_deploy_key.pub (this is the public key)
   ![Image](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/gitssh/ssh-key-generation.png)

2. Add the public key to your Looker git repo as a deploy key with read access (no need to provision write access). Follow the guide [here](https://docs.github.com/en/developers/overview/managing-deploy-keys#deploy-keys) for that.
   ![Image](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/gitssh/git-deploy-key.png)

3. Make note of the private key file, you will need to paste the contents of the file into the **GitHub Deploy Key** field later while setting up [ingestion using the UI](#ui-based-ingestion-recommended-for-ease-of-use).

#### [Optional] Create an API key with admin privileges

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.
You need to ensure that the API key is attached to a user that has Admin privileges.

If that is not possible, read the configuration section and provide an offline specification of the `connection_to_platform_map` and the `project_name`.

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
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: "3.10"
      - name: Run LookML ingestion
        run: |
          pip install 'acryl-datahub[lookml,datahub-rest]'
          cat << EOF > lookml_ingestion.yml
          # LookML ingestion configuration
          source:
            type: "lookml"
            config:
              base_folder: ${{ github.workspace }}
              parse_table_names_from_sql: true
              github_info:
                repo: ${{ github.repository }}
                branch: ${{ github.ref }}
              # Options
              #connection_to_platform_map:
              #  connection-name:
                  #platform: platform-name (e.g. snowflake)
                  #default_db: default-db-name (e.g. DEMO_PIPELINE)
              api:
                client_id: ${LOOKER_CLIENT_ID}
                client_secret: ${LOOKER_CLIENT_SECRET}
                base_url: ${LOOKER_BASE_URL}
          sink:
            type: datahub-rest
            config:
              server: ${DATAHUB_GMS_HOST}
              token: ${DATAHUB_TOKEN}
          EOF
          datahub ingest -c lookml_ingestion.yml
        env:
          DATAHUB_GMS_HOST: ${{ secrets.DATAHUB_GMS_HOST }}
          DATAHUB_TOKEN: ${{ secrets.DATAHUB_TOKEN }}
          LOOKER_BASE_URL: ${{ secrets.LOOKER_BASE_URL }}
          LOOKER_CLIENT_ID: ${{ secrets.LOOKER_CLIENT_ID }}
          LOOKER_CLIENT_SECRET: ${{ secrets.LOOKER_CLIENT_SECRET }}
```

If you want to ingest lookml using the **datahub** cli directly, read on for instructions and configuration details.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[lookml]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: "lookml"
  config:
    # GitHub Coordinates: Used to check out the repo locally and add github links on the dataset's entity page.
    github_info:
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
# See https://datahubproject.io/docs/metadata-ingestion/sink_docs/datahub for customization options
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                                                                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">base_folder</span></div> <div className="type-name-line"><span className="type-name">string(directory-path)</span></div>                                                                                                                                 | Required if not providing github configuration and deploy keys. A pointer to a local directory (accessible to the ingestion system) where the root of the LookML repo has been checked out (typically via a git clone). This is typically the root folder where the `*.model.lkml` and `*.view.lkml` files are stored. e.g. If you have checked out your LookML repo under `/Users/jdoe/workspace/my-lookml-repo`, then set `base_folder` to `/Users/jdoe/workspace/my-lookml-repo`. |
| <div className="path-line"><span className="path-main">emit_reachable_views_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                                  | When enabled, only views that are reachable from explores defined in the model files are emitted <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                               | When enabled, extracts column-level lineage from Views and Explores <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">max_file_snippet_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                                                                                    | When extracting the view definition from a lookml file, the maximum number of characters to extract. <div className="default-line default-line-with-docs">Default: <span className="default-value">512000</span></div>                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">parse_table_names_from_sql</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                                 | See note below. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                           | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">platform_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                               | Default platform name. Don't change. <div className="default-line default-line-with-docs">Default: <span className="default-value">looker</span></div>                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">populate_sql_logic_for_missing_descriptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                | When enabled, field descriptions will include the sql logic for computed fields if descriptions are missing <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">process_isolation_for_sql_parsing</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                          | When enabled, sql parsing will be executed in a separate process to prevent memory leaks. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">process_refinements</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                                        | When enabled, looker refinement will be processed to adapt an existing view. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">project_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                                | Required if you don't specify the `api` section. The project name within which all the model files live. See (https://docs.looker.com/data-modeling/getting-started/how-project-works) to understand what the Looker project name should be. The simplest way to see your projects is to click on `Develop` followed by `Manage LookML Projects` in the Looker application.                                                                                                          |
| <div className="path-line"><span className="path-main">sql_parser</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                                  | See note below. <div className="default-line default-line-with-docs">Default: <span className="default-value">datahub.utilities.sql_parser.DefaultSQLParser</span></div>                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">tag_measures_and_dimensions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                                | When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                                         | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">api</span></div> <div className="type-name-line"><span className="type-name">LookerAPIConfig</span></div>                                                                                                                                                |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">api.</span><span className="path-main">base_url</span>&nbsp;<abbr title="Required if api is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">api.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if api is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                      | Looker API client id.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">api.</span><span className="path-main">client_secret</span>&nbsp;<abbr title="Required if api is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                  | Looker API client secret.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">api.</span><span className="path-main">transport_options</span></div> <div className="type-name-line"><span className="type-name">TransportOptionsConfig</span></div>                                                                                  | Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">api.transport_options.</span><span className="path-main">headers</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">api.transport_options.</span><span className="path-main">timeout</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">integer</span></div>                       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">connection_to_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,LookerConnectionDefinition)</span></div>                                                                                                     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if connection_to_platform_map is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">default_db</span>&nbsp;<abbr title="Required if connection_to_platform_map is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform_env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                          | The environment that the platform is located in. Leaving this empty will inherit defaults from the top level Looker configuration                                                                                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">explore_browse_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div>                                                                                                                         | Pattern for providing browse paths to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name'] <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;pattern&#x27;: &#x27;/&#123;env&#125;/&#123;platform&#125;/&#123;project&#125;/explores&#x27;...</span></div>                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">explore_browse_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if explore_browse_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">explore_naming_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div>                                                                                                                         | Pattern for providing dataset names to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name'] <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;pattern&#x27;: &#x27;&#123;model&#125;.explore.&#123;name&#125;&#x27;&#125;</span></div>                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">explore_naming_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if explore_naming_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">git_info</span></div> <div className="type-name-line"><span className="type-name">GitInfo</span></div>                                                                                                                                                   | Reference to your git location. If present, supplies handy links to your lookml on the dataset entity page.                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">repo</span>&nbsp;<abbr title="Required if git_info is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                 | Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">branch</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                        | Branch on which your files live by default. Typically main or master. This can also be a commit hash. <div className="default-line default-line-with-docs">Default: <span className="default-value">main</span></div>                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">deploy_key</span></div> <div className="type-name-line"><span className="type-name">string(password)</span></div>                                                                                          | A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key.                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">deploy_key_file</span></div> <div className="type-name-line"><span className="type-name">string(file-path)</span></div>                                                                                    | A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string.                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">repo_ssh_locator</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                              | The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts.                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">git_info.</span><span className="path-main">url_template</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                  | Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path}                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">model_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                                                     | List of regex patterns for LookML models to include in the extraction. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">model_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                                             |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">model_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                                              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">model_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                              | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">project_dependencies</span></div> <div className="type-name-line"><span className="type-name">One of map(str,union)(directory-path), map(str,union)</span></div>                                                                                         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">repo</span>&nbsp;<abbr title="Required if project_dependencies is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                   | Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">branch</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                      | Branch on which your files live by default. Typically main or master. This can also be a commit hash. <div className="default-line default-line-with-docs">Default: <span className="default-value">main</span></div>                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">deploy_key</span></div> <div className="type-name-line"><span className="type-name">string(password)</span></div>                                                                        | A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key.                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">deploy_key_file</span></div> <div className="type-name-line"><span className="type-name">string(file-path)</span></div>                                                                  | A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string.                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">repo_ssh_locator</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                            | The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts.                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">project_dependencies.`key`.</span><span className="path-main">url_template</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                | Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path}                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">transport_options</span></div> <div className="type-name-line"><span className="type-name">TransportOptionsConfig</span></div>                                                                                                                           | Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">transport_options.</span><span className="path-main">headers</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">transport_options.</span><span className="path-main">timeout</span>&nbsp;<abbr title="Required if transport_options is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">integer</span></div>                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">view_browse_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div>                                                                                                                            | Pattern for providing browse paths to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name'] <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;pattern&#x27;: &#x27;/&#123;env&#125;/&#123;platform&#125;/&#123;project&#125;/views&#x27;&#125;</span></div>                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">view_browse_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if view_browse_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">view_naming_pattern</span></div> <div className="type-name-line"><span className="type-name">LookerNamingPattern</span></div>                                                                                                                            | Pattern for providing dataset names to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name'] <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;pattern&#x27;: &#x27;&#123;project&#125;.view.&#123;name&#125;&#x27;&#125;</span></div>                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">view_naming_pattern.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if view_naming_pattern is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                                                      | List of regex patterns for LookML views to include in the extraction. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                                              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                                               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                               | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                                                                                              | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                            | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                              | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                         |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "LookMLSourceConfig",
  "description": "Any source that is a primary producer of Dataset metadata should inherit this class",
  "type": "object",
  "properties": {
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "stateful_ingestion": {
      "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "explore_naming_pattern": {
      "title": "Explore Naming Pattern",
      "description": "Pattern for providing dataset names to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name']",
      "default": {
        "pattern": "{model}.explore.{name}"
      },
      "allOf": [
        {
          "$ref": "#/definitions/LookerNamingPattern"
        }
      ]
    },
    "explore_browse_pattern": {
      "title": "Explore Browse Pattern",
      "description": "Pattern for providing browse paths to explores. Allowed variables are ['platform', 'env', 'project', 'model', 'name']",
      "default": {
        "pattern": "/{env}/{platform}/{project}/explores"
      },
      "allOf": [
        {
          "$ref": "#/definitions/LookerNamingPattern"
        }
      ]
    },
    "view_naming_pattern": {
      "title": "View Naming Pattern",
      "description": "Pattern for providing dataset names to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name']",
      "default": {
        "pattern": "{project}.view.{name}"
      },
      "allOf": [
        {
          "$ref": "#/definitions/LookerNamingPattern"
        }
      ]
    },
    "view_browse_pattern": {
      "title": "View Browse Pattern",
      "description": "Pattern for providing browse paths to views. Allowed variables are ['platform', 'env', 'project', 'model', 'name']",
      "default": {
        "pattern": "/{env}/{platform}/{project}/views"
      },
      "allOf": [
        {
          "$ref": "#/definitions/LookerNamingPattern"
        }
      ]
    },
    "tag_measures_and_dimensions": {
      "title": "Tag Measures And Dimensions",
      "description": "When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column.",
      "default": true,
      "type": "boolean"
    },
    "platform_name": {
      "title": "Platform Name",
      "description": "Default platform name. Don't change.",
      "default": "looker",
      "type": "string"
    },
    "extract_column_level_lineage": {
      "title": "Extract Column Level Lineage",
      "description": "When enabled, extracts column-level lineage from Views and Explores",
      "default": true,
      "type": "boolean"
    },
    "git_info": {
      "title": "Git Info",
      "description": "Reference to your git location. If present, supplies handy links to your lookml on the dataset entity page.",
      "allOf": [
        {
          "$ref": "#/definitions/GitInfo"
        }
      ]
    },
    "base_folder": {
      "title": "Base Folder",
      "description": "Required if not providing github configuration and deploy keys. A pointer to a local directory (accessible to the ingestion system) where the root of the LookML repo has been checked out (typically via a git clone). This is typically the root folder where the `*.model.lkml` and `*.view.lkml` files are stored. e.g. If you have checked out your LookML repo under `/Users/jdoe/workspace/my-lookml-repo`, then set `base_folder` to `/Users/jdoe/workspace/my-lookml-repo`.",
      "format": "directory-path",
      "type": "string"
    },
    "project_dependencies": {
      "title": "Project Dependencies",
      "description": "A map of project_name to local directory (accessible to the ingestion system) or Git credentials. Every local_dependencies or private remote_dependency listed in the main project's manifest.lkml file should have a corresponding entry here. If a deploy key is not provided, the ingestion system will use the same deploy key as the main project. ",
      "default": {},
      "type": "object",
      "additionalProperties": {
        "anyOf": [
          {
            "type": "string",
            "format": "directory-path"
          },
          {
            "$ref": "#/definitions/GitInfo"
          }
        ]
      }
    },
    "connection_to_platform_map": {
      "title": "Connection To Platform Map",
      "description": "A mapping of [Looker connection names](https://docs.looker.com/reference/model-params/connection-for-model) to DataHub platform, database, and schema values.",
      "type": "object",
      "additionalProperties": {
        "$ref": "#/definitions/LookerConnectionDefinition"
      }
    },
    "model_pattern": {
      "title": "Model Pattern",
      "description": "List of regex patterns for LookML models to include in the extraction.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "view_pattern": {
      "title": "View Pattern",
      "description": "List of regex patterns for LookML views to include in the extraction.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "parse_table_names_from_sql": {
      "title": "Parse Table Names From Sql",
      "description": "See note below.",
      "default": false,
      "type": "boolean"
    },
    "sql_parser": {
      "title": "Sql Parser",
      "description": "See note below.",
      "default": "datahub.utilities.sql_parser.DefaultSQLParser",
      "type": "string"
    },
    "api": {
      "$ref": "#/definitions/LookerAPIConfig"
    },
    "project_name": {
      "title": "Project Name",
      "description": "Required if you don't specify the `api` section. The project name within which all the model files live. See (https://docs.looker.com/data-modeling/getting-started/how-project-works) to understand what the Looker project name should be. The simplest way to see your projects is to click on `Develop` followed by `Manage LookML Projects` in the Looker application.",
      "type": "string"
    },
    "transport_options": {
      "title": "Transport Options",
      "description": "Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client",
      "allOf": [
        {
          "$ref": "#/definitions/TransportOptionsConfig"
        }
      ]
    },
    "max_file_snippet_length": {
      "title": "Max File Snippet Length",
      "description": "When extracting the view definition from a lookml file, the maximum number of characters to extract.",
      "default": 512000,
      "type": "integer"
    },
    "emit_reachable_views_only": {
      "title": "Emit Reachable Views Only",
      "description": "When enabled, only views that are reachable from explores defined in the model files are emitted",
      "default": true,
      "type": "boolean"
    },
    "populate_sql_logic_for_missing_descriptions": {
      "title": "Populate Sql Logic For Missing Descriptions",
      "description": "When enabled, field descriptions will include the sql logic for computed fields if descriptions are missing",
      "default": false,
      "type": "boolean"
    },
    "process_isolation_for_sql_parsing": {
      "title": "Process Isolation For Sql Parsing",
      "description": "When enabled, sql parsing will be executed in a separate process to prevent memory leaks.",
      "default": false,
      "type": "boolean"
    },
    "process_refinements": {
      "title": "Process Refinements",
      "description": "When enabled, looker refinement will be processed to adapt an existing view.",
      "default": false,
      "type": "boolean"
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
    },
    "LookerNamingPattern": {
      "title": "LookerNamingPattern",
      "type": "object",
      "properties": {
        "pattern": {
          "title": "Pattern",
          "type": "string"
        }
      },
      "required": [
        "pattern"
      ],
      "additionalProperties": false
    },
    "GitInfo": {
      "title": "GitInfo",
      "description": "A reference to a Git repository, including a deploy key that can be used to clone it.",
      "type": "object",
      "properties": {
        "repo": {
          "title": "Repo",
          "description": "Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo.",
          "type": "string"
        },
        "branch": {
          "title": "Branch",
          "description": "Branch on which your files live by default. Typically main or master. This can also be a commit hash.",
          "default": "main",
          "type": "string"
        },
        "url_template": {
          "title": "Url Template",
          "description": "Template for generating a URL to a file in the repo e.g. '{repo_url}/blob/{branch}/{file_path}'. We can infer this for GitHub and GitLab repos, and it is otherwise required.It supports the following variables: {repo_url}, {branch}, {file_path}",
          "type": "string"
        },
        "deploy_key_file": {
          "title": "Deploy Key File",
          "description": "A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string.",
          "format": "file-path",
          "type": "string"
        },
        "deploy_key": {
          "title": "Deploy Key",
          "description": "A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key.",
          "type": "string",
          "writeOnly": true,
          "format": "password"
        },
        "repo_ssh_locator": {
          "title": "Repo Ssh Locator",
          "description": "The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts.",
          "type": "string"
        }
      },
      "required": [
        "repo"
      ],
      "additionalProperties": false
    },
    "LookerConnectionDefinition": {
      "title": "LookerConnectionDefinition",
      "type": "object",
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
          "title": "Default Schema",
          "type": "string"
        },
        "platform_instance": {
          "title": "Platform Instance",
          "type": "string"
        },
        "platform_env": {
          "title": "Platform Env",
          "description": "The environment that the platform is located in. Leaving this empty will inherit defaults from the top level Looker configuration",
          "type": "string"
        }
      },
      "required": [
        "platform",
        "default_db"
      ],
      "additionalProperties": false
    },
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "TransportOptionsConfig": {
      "title": "TransportOptionsConfig",
      "type": "object",
      "properties": {
        "timeout": {
          "title": "Timeout",
          "type": "integer"
        },
        "headers": {
          "title": "Headers",
          "type": "object",
          "additionalProperties": {
            "type": "string"
          }
        }
      },
      "required": [
        "timeout",
        "headers"
      ],
      "additionalProperties": false
    },
    "LookerAPIConfig": {
      "title": "LookerAPIConfig",
      "type": "object",
      "properties": {
        "client_id": {
          "title": "Client Id",
          "description": "Looker API client id.",
          "type": "string"
        },
        "client_secret": {
          "title": "Client Secret",
          "description": "Looker API client secret.",
          "type": "string"
        },
        "base_url": {
          "title": "Base Url",
          "description": "Url to your Looker instance: `https://company.looker.com:19999` or `https://looker.company.com`, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.",
          "type": "string"
        },
        "transport_options": {
          "title": "Transport Options",
          "description": "Populates the [TransportOptions](https://github.com/looker-open-source/sdk-codegen/blob/94d6047a0d52912ac082eb91616c1e7c379ab262/python/looker_sdk/rtl/transport.py#L70) struct for looker client",
          "allOf": [
            {
              "$ref": "#/definitions/TransportOptionsConfig"
            }
          ]
        }
      },
      "required": [
        "client_id",
        "client_secret",
        "base_url"
      ],
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

#### Configuration Notes

:::note

The integration can use an SQL parser to try to parse the tables the views depends on.

:::

This parsing is disabled by default, but can be enabled by setting `parse_table_names_from_sql: True`. The default parser is based on the [`sqllineage`](https://pypi.org/project/sqllineage/) package.
As this package doesn't officially support all the SQL dialects that Looker supports, the result might not be correct. You can, however, implement a custom parser and take it into use by setting the `sql_parser` configuration value. A custom SQL parser must inherit from `datahub.utilities.sql_parser.SQLParser`
and must be made available to Datahub by ,for example, installing it. The configuration then needs to be set to `module_name.ClassName` of the parser.

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

### Code Coordinates

- Class Name: `datahub.ingestion.source.looker.lookml_source.LookMLSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/looker/lookml_source.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Looker, feel free to ping us on [our Slack](https://slack.datahubproject.io).
