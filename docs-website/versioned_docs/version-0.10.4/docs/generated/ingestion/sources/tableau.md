---
sidebar_position: 49
title: Tableau
slug: /generated/ingestion/sources/tableau
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/tableau.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Tableau

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                                                                 |
| ---------------------------------------------------------------------------------------------------------- | ------ | --------------------------------------------------------------------- |
| Dataset Usage                                                                                              | ✅     | Dashboard/Chart view counts, enabled using extract_usage_stats config |
| Descriptions                                                                                               | ✅     | Enabled by default                                                    |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Enabled by default when stateful ingestion is turned on.              |
| [Domains](../../../domains.md)                                                                             | ❌     | Requires transformer                                                  |
| Extract Ownership                                                                                          | ✅     | Requires recipe configuration                                         |
| Extract Tags                                                                                               | ✅     | Requires recipe configuration                                         |
| [Platform Instance](../../../platform-instances.md)                                                        | ✅     | Enabled by default                                                    |
| Table-Level Lineage                                                                                        | ✅     | Enabled by default                                                    |

### Prerequisites

In order to ingest metadata from Tableau, you will need:

- Tableau Server Version 2021.1.10 and above. It may also work for older versions.
- [Enable the Tableau Metadata API](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_start.html#enable-the-tableau-metadata-api-for-tableau-server) for Tableau Server, if its not already enabled.
- Tableau Credentials (Username/Password or [Personal Access Token](https://help.tableau.com/current/pro/desktop/en-us/useracct.htm#create-and-revoke-personal-access-tokens))
- The user or token must have **Site Administrator Explorer** permissions.

### Ingestion through UI

The following video shows you how to get started with ingesting Tableau metadata through the UI.

<div
  style={{
    position: "relative",
    paddingBottom: "57.692307692307686%",
    height: 0
  }}
>
  <iframe
    src="https://www.loom.com/embed/ef521c4e66564614a6ddde35dc3840f8"
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

### Integration Details

This plugin extracts Sheets, Dashboards, Embedded and Published Data sources metadata within Workbooks in a given project
on a Tableau site. Tableau's GraphQL interface is used to extract metadata information. Queries used to extract metadata are located
in `metadata-ingestion/src/datahub/ingestion/source/tableau_common.py`

#### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept              | DataHub Concept                                               | Notes                             |
| --------------------------- | ------------------------------------------------------------- | --------------------------------- |
| `"Tableau"`                 | [Data Platform](../../metamodel/entities/dataPlatform.md)     |
| Project                     | [Container](../../metamodel/entities/container.md)            | SubType `"Project"`               |
| Embedded DataSource         | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Embedded Data Source"`  |
| Published DataSource        | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Published Data Source"` |
| Custom SQL Table            | [Dataset](../../metamodel/entities/dataset.md)                | SubTypes `"View"`, `"Custom SQL"` |
| Embedded or External Tables | [Dataset](../../metamodel/entities/dataset.md)                |                                   |
| Sheet                       | [Chart](../../metamodel/entities/chart.md)                    |                                   |
| Dashboard                   | [Dashboard](../../metamodel/entities/dashboard.md)            |                                   |
| User                        | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted              |
| Workbook                    | [Container](../../metamodel/entities/container.md)            | SubType `"Workbook"`              |
| Tag                         | [Tag](../../metamodel/entities/tag.md)                        | Optionally Extracted              |

#### Lineage

Lineage is emitted as received from Tableau's metadata API for

- Sheets contained within a Dashboard
- Embedded or Published Data Sources depended on by a Sheet
- Published Data Sources upstream to Embedded datasource
- Tables upstream to Embedded or Published Data Source
- Custom SQL datasources upstream to Embedded or Published Data Source
- Tables upstream to Custom SQL Data Source

#### Caveats

- Tableau metadata API might return incorrect schema name for tables for some databases, leading to incorrect metadata in DataHub. This source attempts to extract correct schema from databaseTable's fully qualified name, wherever possible. Read [Using the databaseTable object in query](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute) for caveats in using schema attribute.

### Troubleshooting

### Why are only some workbooks/custom SQLs/published datasources ingested from the specified project?

This may happen when the Tableau API returns NODE_LIMIT_EXCEEDED error in response to metadata query and returns partial results with message "Showing partial results. , The request exceeded the ‘n’ node limit. Use pagination, additional filtering, or both in the query to adjust results." To resolve this, consider

- reducing the page size using the `page_size` config param in datahub recipe (Defaults to 10).
- increasing tableau configuration [metadata query node limit](https://help.tableau.com/current/server/en-us/cli_configuration-set_tsm.htm#metadata_nodelimit) to higher value.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[tableau]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: tableau
  config:
    # Coordinates
    connect_uri: https://prod-ca-a.online.tableau.com
    site: acryl
    platform_instance: acryl_instance
    project_pattern:
      ["^default$", "^Project 2$", "^/Project A/Nested Project B$"]

    # Credentials
    username: "${TABLEAU_USER}"
    password: "${TABLEAU_PASSWORD}"

    # Options
    ingest_tags: True
    ingest_owner: True
    default_schema_map:
      mydatabase: public
      anotherdatabase: anotherschema

sink:
  # sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">connect_uri</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                     | Tableau host URL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">default_schema_map</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                    | Default schema to use when schema is not found. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">extract_column_level_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                         | When enabled, extracts column-level lineage from Tableau Datasources <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">extract_lineage_from_unsupported_custom_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                  | [Experimental] Whether to extract lineage from unsupported custom sql queries using SQL parsing <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">extract_project_hierarchy</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                            | Whether to extract entire project hierarchy for nested projects. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">extract_usage_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                  | [experimental] Extract usage statistics for dashboards and charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">ingest_embed_url</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                     | Ingest a URL to render an embedded Preview of assets within Tableau. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">ingest_external_links_for_charts</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                     | Ingest a URL to link out to from charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">ingest_external_links_for_dashboards</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                 | Ingest a URL to link out to from dashboards. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">ingest_owner</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                         | Ingest Owner from source. This will override Owner info entered from UI <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">ingest_tables_external</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                               | Ingest details for tables external to (not embedded in) tableau as entities. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">ingest_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                          | Ingest Tags from source. This will override Tags entered from UI <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                            | [advanced] Number of metadata objects (e.g. CustomSQLTable, PublishedDatasource, etc) to query at a time using the Tableau API. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                              | Tableau password, must be set if authenticating using username/password.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                     | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">project_path_separator</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                | The separator used for the project_pattern field between project names. By default, we use a slash. You can change this if your Tableau projects contain slashes in their names, and you'd like to filter by project. <div className="default-line default-line-with-docs">Default: <span className="default-value">/</span></div>                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">projects</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">site</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                  | Tableau Site. Always required for Tableau Online. Use emptystring to connect with Default site on Tableau Server. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">ssl_verify</span></div> <div className="type-name-line"><span className="type-name">One of boolean, string</span></div>                                                            | Whether to verify SSL certificates. If using self-signed certificates, set to false or provide the path to the .pem certificate bundle. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">token_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                            | Tableau token name, must be set if authenticating using a personal access token.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">token_value</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                           | Tableau token value, must be set if authenticating using a personal access token.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                              | Tableau username, must be set if authenticating using username/password.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">workbook_page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                   | [advanced] Number of workbooks to query at a time using the Tableau API. <div className="default-line default-line-with-docs">Default: <span className="default-value">1</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                   | Environment to use in namespace when constructing URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">lineage_overrides</span></div> <div className="type-name-line"><span className="type-name">TableauLineageOverrides</span></div>                                                    | Mappings to change generated dataset urns. Use only if you really know what you are doing.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">lineage_overrides.</span><span className="path-main">database_override_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">lineage_overrides.</span><span className="path-main">platform_override_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">project_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                             | Filter for specific Tableau projects. For example, use 'My Project' to ingest a root-level Project with name 'My Project', or 'My Project/Nested Project' to ingest a nested Project with name 'Nested Project'. By default, all Projects nested inside a matching Project will be included in ingestion. You can both allow and deny projects based on their name using their name, or a Regex pattern. Deny patterns always take precedence over allow patterns. By default, all projects will be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">project_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">project_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">project_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                      | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                        | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                      | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>        | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "TableauConfig",
  "description": "Any non-Dataset source that produces lineage to Datasets should inherit this class.\ne.g. Orchestrators, Pipelines, BI Tools etc.",
  "type": "object",
  "properties": {
    "connect_uri": {
      "title": "Connect Uri",
      "description": "Tableau host URL.",
      "type": "string"
    },
    "username": {
      "title": "Username",
      "description": "Tableau username, must be set if authenticating using username/password.",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "Tableau password, must be set if authenticating using username/password.",
      "type": "string"
    },
    "token_name": {
      "title": "Token Name",
      "description": "Tableau token name, must be set if authenticating using a personal access token.",
      "type": "string"
    },
    "token_value": {
      "title": "Token Value",
      "description": "Tableau token value, must be set if authenticating using a personal access token.",
      "type": "string"
    },
    "site": {
      "title": "Site",
      "description": "Tableau Site. Always required for Tableau Online. Use emptystring to connect with Default site on Tableau Server.",
      "default": "",
      "type": "string"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "ssl_verify": {
      "title": "Ssl Verify",
      "description": "Whether to verify SSL certificates. If using self-signed certificates, set to false or provide the path to the .pem certificate bundle.",
      "default": true,
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "string"
        }
      ]
    },
    "extract_column_level_lineage": {
      "title": "Extract Column Level Lineage",
      "description": "When enabled, extracts column-level lineage from Tableau Datasources",
      "default": true,
      "type": "boolean"
    },
    "env": {
      "title": "Env",
      "description": "Environment to use in namespace when constructing URNs.",
      "default": "PROD",
      "type": "string"
    },
    "stateful_ingestion": {
      "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
    },
    "platform_instance_map": {
      "title": "Platform Instance Map",
      "description": "A holder for platform -> platform_instance mappings to generate correct dataset urns",
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    },
    "projects": {
      "title": "Projects",
      "description": "[deprecated] Use project_pattern instead. List of tableau projects ",
      "default": [
        "default"
      ],
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "project_pattern": {
      "title": "Project Pattern",
      "description": "Filter for specific Tableau projects. For example, use 'My Project' to ingest a root-level Project with name 'My Project', or 'My Project/Nested Project' to ingest a nested Project with name 'Nested Project'. By default, all Projects nested inside a matching Project will be included in ingestion. You can both allow and deny projects based on their name using their name, or a Regex pattern. Deny patterns always take precedence over allow patterns. By default, all projects will be ingested.",
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
    "project_path_separator": {
      "title": "Project Path Separator",
      "description": "The separator used for the project_pattern field between project names. By default, we use a slash. You can change this if your Tableau projects contain slashes in their names, and you'd like to filter by project.",
      "default": "/",
      "type": "string"
    },
    "default_schema_map": {
      "title": "Default Schema Map",
      "description": "Default schema to use when schema is not found.",
      "default": {},
      "type": "object"
    },
    "ingest_tags": {
      "title": "Ingest Tags",
      "description": "Ingest Tags from source. This will override Tags entered from UI",
      "default": false,
      "type": "boolean"
    },
    "ingest_owner": {
      "title": "Ingest Owner",
      "description": "Ingest Owner from source. This will override Owner info entered from UI",
      "default": false,
      "type": "boolean"
    },
    "ingest_tables_external": {
      "title": "Ingest Tables External",
      "description": "Ingest details for tables external to (not embedded in) tableau as entities.",
      "default": false,
      "type": "boolean"
    },
    "page_size": {
      "title": "Page Size",
      "description": "[advanced] Number of metadata objects (e.g. CustomSQLTable, PublishedDatasource, etc) to query at a time using the Tableau API.",
      "default": 10,
      "type": "integer"
    },
    "workbook_page_size": {
      "title": "Workbook Page Size",
      "description": "[advanced] Number of workbooks to query at a time using the Tableau API.",
      "default": 1,
      "type": "integer"
    },
    "lineage_overrides": {
      "title": "Lineage Overrides",
      "description": "Mappings to change generated dataset urns. Use only if you really know what you are doing.",
      "allOf": [
        {
          "$ref": "#/definitions/TableauLineageOverrides"
        }
      ]
    },
    "extract_usage_stats": {
      "title": "Extract Usage Stats",
      "description": "[experimental] Extract usage statistics for dashboards and charts.",
      "default": false,
      "type": "boolean"
    },
    "ingest_embed_url": {
      "title": "Ingest Embed Url",
      "description": "Ingest a URL to render an embedded Preview of assets within Tableau.",
      "default": false,
      "type": "boolean"
    },
    "ingest_external_links_for_dashboards": {
      "title": "Ingest External Links For Dashboards",
      "description": "Ingest a URL to link out to from dashboards.",
      "default": true,
      "type": "boolean"
    },
    "ingest_external_links_for_charts": {
      "title": "Ingest External Links For Charts",
      "description": "Ingest a URL to link out to from charts.",
      "default": true,
      "type": "boolean"
    },
    "extract_project_hierarchy": {
      "title": "Extract Project Hierarchy",
      "description": "Whether to extract entire project hierarchy for nested projects.",
      "default": true,
      "type": "boolean"
    },
    "extract_lineage_from_unsupported_custom_sql_queries": {
      "title": "Extract Lineage From Unsupported Custom Sql Queries",
      "description": "[Experimental] Whether to extract lineage from unsupported custom sql queries using SQL parsing",
      "default": false,
      "type": "boolean"
    }
  },
  "required": [
    "connect_uri"
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
    "TableauLineageOverrides": {
      "title": "TableauLineageOverrides",
      "type": "object",
      "properties": {
        "platform_override_map": {
          "title": "Platform Override Map",
          "description": "A holder for platform -> platform mappings to generate correct dataset urns",
          "type": "object",
          "additionalProperties": {
            "type": "string"
          }
        },
        "database_override_map": {
          "title": "Database Override Map",
          "description": "A holder for database -> database mappings to generate correct dataset urns",
          "type": "object",
          "additionalProperties": {
            "type": "string"
          }
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

- Class Name: `datahub.ingestion.source.tableau.TableauSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/tableau.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Tableau, feel free to ping us on [our Slack](https://slack.datahubproject.io).
