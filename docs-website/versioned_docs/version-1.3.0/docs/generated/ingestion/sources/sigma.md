---
sidebar_position: 65
title: Sigma
slug: /generated/ingestion/sources/sigma
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/sigma.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Sigma
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Sigma Workspace. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default, configured using `ingest_owner`. |
| Extract Tags | ✅ | Enabled by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |
| Test Connection | ✅ | Enabled by default. |


This plugin extracts the following:
- Sigma Workspaces and Workbooks as Container.
- Sigma Datasets
- Pages as Dashboard and its Elements as Charts

## Integration Details

This source extracts the following:

- Workspaces and workbooks within that workspaces as Container.
- Sigma Datasets as Datahub Datasets.
- Pages as Datahub dashboards and elements present inside pages as charts.

## Configuration Notes

1. Refer [doc](https://help.sigmacomputing.com/docs/generate-api-client-credentials) to generate an API client credentials.
2. Provide the generated Client ID and Secret in Recipe.

We have observed issues with the Sigma API, where certain API endpoints do not return the expected results, even when the user is an admin. In those cases, a workaround is to manually add the user associated with the Client ID/Secret to each workspace with missing metadata.
Empty workspaces are listed in the ingestion report in the logs with the key `empty_workspaces`.

## Concept mapping

| Sigma       | Datahub                                                       | Notes                       |
| ----------- | ------------------------------------------------------------- | --------------------------- |
| `Workspace` | [Container](../../metamodel/entities/container.md)            | SubType `"Sigma Workspace"` |
| `Workbook`  | [Dashboard](../../metamodel/entities/dashboard.md)            | SubType `"Sigma Workbook"`  |
| `Page`      | [Dashboard](../../metamodel/entities/dashboard.md)            |                             |
| `Element`   | [Chart](../../metamodel/entities/chart.md)                    |                             |
| `Dataset`   | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Sigma Dataset"`   |
| `User`      | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted        |

## Advanced Configurations

### Chart source platform mapping

If you want to provide platform details(platform name, platform instance and env) for chart's all external upstream data sources, then you can use `chart_sources_platform_mapping` as below:

#### Example - For just one specific chart's external upstream data sources

```yml
chart_sources_platform_mapping:
  "workspace_name/workbook_name/chart_name_1":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD

  "workspace_name/folder_name/workbook_name/chart_name_2":
    data_source_platform: postgres
    platform_instance: cloud_instance
    env: DEV
```

#### Example - For all charts within one specific workbook

```yml
chart_sources_platform_mapping:
  "workspace_name/workbook_name_1":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD

  "workspace_name/folder_name/workbook_name_2":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

#### Example - For all workbooks charts within one specific workspace

```yml
chart_sources_platform_mapping:
  "workspace_name":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

#### Example - All workbooks use the same connection

```yml
chart_sources_platform_mapping:
  "*":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: sigma
  config:
    # Coordinates
    api_url: "https://aws-api.sigmacomputing.com/v2"
    # Credentials
    client_id: "CLIENTID"
    client_secret: "CLIENT_SECRET"
    
    # Optional - filter for certain workspace names instead of ingesting everything.
    # workspace_pattern:
    #   allow:
    #     - workspace_name

    ingest_owner: true
    
    # Optional - mapping of sigma workspace/workbook/chart folder path to all chart's data sources platform details present inside that folder path.
    # chart_sources_platform_mapping:
    #   folder_path:
    #     data_source_platform: postgres
    #     platform_instance: cloud_instance
    #     env: DEV

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
| <div className="path-line"><span className="path-main">client_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Sigma Client ID  |
| <div className="path-line"><span className="path-main">client_secret</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Sigma Client Secret  |
| <div className="path-line"><span className="path-main">api_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Sigma API hosted URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://aws-api.sigmacomputing.com/v2</span></div> |
| <div className="path-line"><span className="path-main">extract_lineage</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to extract lineage of workbook's elements and datasets or not. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_owner</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Ingest Owner from source. This will override Owner info entered from UI. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_shared_entities</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ingest the shared entities or not. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">chart_sources_platform_mapping</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">chart_sources_platform_mapping.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">chart_sources_platform_mapping.`key`.</span><span className="path-main">data_source_platform</span>&nbsp;<abbr title="Required if chart_sources_platform_mapping is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | A chart's data sources platform name.  |
| <div className="path-line"><span className="path-prefix">chart_sources_platform_mapping.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">workbook_lineage_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workbook_lineage_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">workspace_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">workspace_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Sigma Stateful Ingestion Config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "PlatformDetail": {
      "additionalProperties": false,
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
        "data_source_platform": {
          "description": "A chart's data sources platform name.",
          "title": "Data Source Platform",
          "type": "string"
        }
      },
      "required": [
        "data_source_platform"
      ],
      "title": "PlatformDetail",
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
      "default": null,
      "description": "Sigma Stateful Ingestion Config."
    },
    "api_url": {
      "default": "https://aws-api.sigmacomputing.com/v2",
      "description": "Sigma API hosted URL.",
      "title": "Api Url",
      "type": "string"
    },
    "client_id": {
      "description": "Sigma Client ID",
      "title": "Client Id",
      "type": "string"
    },
    "client_secret": {
      "description": "Sigma Client Secret",
      "title": "Client Secret",
      "type": "string"
    },
    "workspace_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter Sigma workspaces in ingestion.Mention 'My documents' if personal entities also need to ingest."
    },
    "ingest_owner": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Ingest Owner from source. This will override Owner info entered from UI.",
      "title": "Ingest Owner"
    },
    "ingest_shared_entities": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Whether to ingest the shared entities or not.",
      "title": "Ingest Shared Entities"
    },
    "extract_lineage": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Whether to extract lineage of workbook's elements and datasets or not.",
      "title": "Extract Lineage"
    },
    "workbook_lineage_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter workbook's elements and datasets lineage in ingestion.Requires extract_lineage to be enabled."
    },
    "chart_sources_platform_mapping": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "default": {},
      "description": "A mapping of the sigma workspace/workbook/chart folder path to all chart's data sources platform details present inside that folder path.",
      "title": "Chart Sources Platform Mapping",
      "type": "object"
    }
  },
  "required": [
    "client_id",
    "client_secret"
  ],
  "title": "SigmaSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.sigma.sigma.SigmaSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sigma/sigma.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Sigma, feel free to ping us on [our Slack](https://datahub.com/slack).
