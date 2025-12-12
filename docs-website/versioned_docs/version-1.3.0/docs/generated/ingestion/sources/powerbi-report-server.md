---
sidebar_position: 53
title: PowerBI Report Server
slug: /generated/ingestion/sources/powerbi-report-server
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/powerbi-report-server.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PowerBI Report Server
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default. |


Use this plugin to connect to [PowerBI Report Server](https://powerbi.microsoft.com/en-us/report-server/).
It extracts the following:

Metadata that can be ingested:
   - report name
   - report description
   - ownership(can add existing users in DataHub as owners)
   - transfer folders structure to DataHub as it is in Report Server
   - webUrl to report in Report Server

Due to limits of PBIRS REST API, it's impossible to ingest next data for now:
   - tiles info
   - datasource of report
   - dataset of report

Next types of report can be ingested:
   - PowerBI report(.pbix)
   - Paginated report(.rdl)
   - Linked report

### Configuration Notes

See the

1. [Microsoft Grant user access to a Report Server doc](https://docs.microsoft.com/en-us/sql/reporting-services/security/grant-user-access-to-a-report-server?view=sql-server-ver16)
2. Use your user credentials from previous step in yaml file

### Concept mapping

| Power BI Report Server | Datahub     |
| ---------------------- | ----------- |
| `Paginated Report`     | `Dashboard` |
| `Power BI Report`      | `Dashboard` |
| `Mobile Report`        | `Dashboard` |
| `Linked Report`        | `Dashboard` |
| `Dataset, Datasource`  | `N/A`       |

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: powerbi-report-server
  config:
    # Your Power BI Report Server Windows username
    username: username
    # Your Power BI Report Server Windows password
    password: password
    # Your Workstation name
    workstation_name: workstation_name
    # Your Power BI Report Server host URL, example: localhost:80
    host_port: host_port
    # Your alias for Power BI Report Server host URL, example: local_powerbi_report_server
    server_alias: server_alias
    # Workspace's dataset environments, example: (PROD, DEV, QA, STAGE)
    env: DEV
    # Your Power BI Report Server base virtual directory name for reports
    report_virtual_directory_name: Reports
    #  Your Power BI Report Server base virtual directory name for report server
    report_server_virtual_directory_name: ReportServer
    # Enable/Disable extracting ownership information of Dashboard
    extract_ownership: True
    # Set ownership type
    ownership_type: TECHNICAL_OWNER


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
| <div className="path-line"><span className="path-main">host_port</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Power BI Report Server host URL  |
| <div className="path-line"><span className="path-main">password</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Windows account password  |
| <div className="path-line"><span className="path-main">report_server_virtual_directory_name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Report Server Virtual Directory URL name  |
| <div className="path-line"><span className="path-main">report_virtual_directory_name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Report Virtual Directory URL name  |
| <div className="path-line"><span className="path-main">username</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Windows account username  |
| <div className="path-line"><span className="path-main">extract_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether ownership should be ingested <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">graphql_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | [deprecated] Not used <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">ownership_type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Ownership type of owner <div className="default-line default-line-with-docs">Default: <span className="default-value">NONE</span></div> |
| <div className="path-line"><span className="path-main">platform_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">powerbi</span></div> |
| <div className="path-line"><span className="path-main">platform_urn</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">urn:li:dataPlatform:powerbi</span></div> |
| <div className="path-line"><span className="path-main">server_alias</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Alias for Power BI Report Server host URL <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">workstation_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Workstation name <div className="default-line default-line-with-docs">Default: <span className="default-value">localhost</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">chart_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">chart_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">report_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">report_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulIngestionConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

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
  "additionalProperties": false,
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
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
    "username": {
      "description": "Windows account username",
      "title": "Username",
      "type": "string"
    },
    "password": {
      "description": "Windows account password",
      "title": "Password",
      "type": "string"
    },
    "workstation_name": {
      "default": "localhost",
      "description": "Workstation name",
      "title": "Workstation Name",
      "type": "string"
    },
    "host_port": {
      "description": "Power BI Report Server host URL",
      "title": "Host Port",
      "type": "string"
    },
    "server_alias": {
      "default": "",
      "description": "Alias for Power BI Report Server host URL",
      "title": "Server Alias",
      "type": "string"
    },
    "graphql_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "[deprecated] Not used",
      "title": "Graphql Url"
    },
    "report_virtual_directory_name": {
      "description": "Report Virtual Directory URL name",
      "title": "Report Virtual Directory Name",
      "type": "string"
    },
    "report_server_virtual_directory_name": {
      "description": "Report Server Virtual Directory URL name",
      "title": "Report Server Virtual Directory Name",
      "type": "string"
    },
    "extract_ownership": {
      "default": true,
      "description": "Whether ownership should be ingested",
      "title": "Extract Ownership",
      "type": "boolean"
    },
    "ownership_type": {
      "default": "NONE",
      "description": "Ownership type of owner",
      "title": "Ownership Type",
      "type": "string"
    },
    "platform_name": {
      "default": "powerbi",
      "title": "Platform Name",
      "type": "string"
    },
    "platform_urn": {
      "default": "urn:li:dataPlatform:powerbi",
      "title": "Platform Urn",
      "type": "string"
    },
    "report_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      }
    },
    "chart_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      }
    }
  },
  "required": [
    "username",
    "password",
    "host_port",
    "report_virtual_directory_name",
    "report_server_virtual_directory_name"
  ],
  "title": "PowerBiReportServerDashboardSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.powerbi_report_server.report_server.PowerBiReportServerDashboardSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/powerbi_report_server/report_server.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for PowerBI Report Server, feel free to ping us on [our Slack](https://datahub.com/slack).
