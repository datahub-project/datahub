


# PowerBI Report Server

## Overview

Power BI Report Server is a business intelligence and analytics platform. Learn more in the [official Power BI Report Server documentation](https://learn.microsoft.com/power-bi/report-server/).

The DataHub integration for Power BI Report Server covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures ownership and stateful deletion detection.

## Concept Mapping

| Power BI Report Server | Datahub     |
| ---------------------- | ----------- |
| `Paginated Report`     | `Dashboard` |
| `Power BI Report`      | `Dashboard` |
| `Mobile Report`        | `Dashboard` |
| `Linked Report`        | `Dashboard` |
| `Dataset, Datasource`  | `N/A`       |


## Module `powerbi-report-server`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default. |

### Overview

The `powerbi-report-server` module ingests metadata from Powerbi Report Server into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

Metadata that can be ingested:

- eport name
- eport description
- wnership(can add existing users in DataHub as owners)
- ransfer folders structure to DataHub as it is in Report Server
- ebUrl to report in Report Server

Due to limits of PBIRS REST API, it's impossible to ingest next data for now:

- tiles info
- datasource of report
- dataset of report

Next types of report can be ingested:

- PowerBI report(.pbix)
- Paginated report(.rdl)
- Linked report

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Grant user access to Report Server following [Microsoft documentation](https://docs.microsoft.com/en-us/sql/reporting-services/security/grant-user-access-to-a-report-server?view=sql-server-ver16)
2. Use these credentials in your ingestion recipe


### Install the Plugin
```shell
pip install 'acryl-datahub[powerbi-report-server]'
```

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

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">host_port</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Power BI Report Server host URL  |
| <div className="path-line"><span className="path-main">password</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Windows account password  |
| <div className="path-line"><span className="path-main">report_server_virtual_directory_name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Report Server Virtual Directory URL name  |
| <div className="path-line"><span className="path-main">report_virtual_directory_name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Report Virtual Directory URL name  |
| <div className="path-line"><span className="path-main">username</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Windows account username  |
| <div className="path-line"><span className="path-main">extract_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether ownership should be ingested <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">graphql_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | [deprecated] Not used <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">ownership_type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Ownership type of owner <div className="default-line default-line-with-docs">Default: <span className="default-value">NONE</span></div> |
| <div className="path-line"><span className="path-main">server_alias</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Alias for Power BI Report Server host URL <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">workstation_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Workstation name <div className="default-line default-line-with-docs">Default: <span className="default-value">localhost</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">report_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">report_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulIngestionConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

</div>




#### Schema


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
      "format": "password",
      "title": "Password",
      "type": "string",
      "writeOnly": true
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
    "report_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter PowerBI Reports by name in ingestion."
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





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

- Ingestion is focused on report-server catalog assets and does not model all report-server object types as first-class DataHub entities.
- Upstream dataset/datasource lineage is limited compared with cloud Power BI ingestion.
- Metadata quality depends on permissions granted to the report-server user.

### Troubleshooting

- **Unauthorized responses**: verify the configured account has access to the report server and target folders.
- **Missing reports**: confirm report types are supported and the crawler scope includes the expected catalog paths.
- **Connection issues**: validate Report Server URL reachability and authentication settings from the ingestion runtime.


### Code Coordinates
- Class Name: `datahub.ingestion.source.powerbi_report_server.report_server.PowerBiReportServerDashboardSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/powerbi_report_server/report_server.py)


:::tip Questions?

If you've got any questions on configuring ingestion for PowerBI Report Server, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
