---
sidebar_position: 40
title: Redash
slug: /generated/ingestion/sources/redash
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/redash.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Redash

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

This plugin extracts the following:

- Redash dashboards and queries/visualization
- Redash chart table lineages (disabled by default)

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[redash]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: "redash"
  config:
    connect_uri: http://localhost:5000/
    api_key: REDASH_API_KEY

    # Optionals
    # api_page_limit: 1 #default: None, no limit on ingested dashboards and charts API pagination
    # skip_draft: true  #default: true, only ingest published dashboards and charts
    # dashboard_patterns:
    #   deny:
    #     - ^denied dashboard.*
    #   allow:
    #     - .*allowed dashboard.*
    # chart_patterns:
    #   deny:
    #     - ^denied chart.*
    #   allow:
    #     - .*allowed chart.*
    # parse_table_names_from_sql: false
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                    | Description                                                                                                                                                                                                                                                                            |
| :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                              | Redash user API key. <div className="default-line default-line-with-docs">Default: <span className="default-value">REDASH_API_KEY</span></div>                                                                                                                                         |
| <div className="path-line"><span className="path-main">api_page_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                      | Limit on number of pages queried for ingesting dashboards and charts API during pagination. <div className="default-line default-line-with-docs">Default: <span className="default-value">9223372036854775807</span></div>                                                             |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                          | Redash base URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">http://localhost:5000</span></div>                                                                                                                                      |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                           | Limit on number of items to be queried at once. <div className="default-line default-line-with-docs">Default: <span className="default-value">25</span></div>                                                                                                                          |
| <div className="path-line"><span className="path-main">parallelism</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                         | Parallelism to use while processing. <div className="default-line default-line-with-docs">Default: <span className="default-value">1</span></div>                                                                                                                                      |
| <div className="path-line"><span className="path-main">parse_table_names_from_sql</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                          | See note below. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                       |
| <div className="path-line"><span className="path-main">skip_draft</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                          | Only ingest published dashboards and charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                           |
| <div className="path-line"><span className="path-main">sql_parser</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                           | custom SQL parser. See note below for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">datahub.utilities.sql_parser.DefaultSQLParser</span></div>                                                                                |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                  | Environment to use in namespace when constructing URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                |
| <div className="path-line"><span className="path-main">chart_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                             | regex patterns for charts to filter for ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>     |
| <div className="path-line"><span className="path-prefix">chart_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>     |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">chart_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>      |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">chart_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>      | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                            |
| <div className="path-line"><span className="path-main">dashboard_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                         | regex patterns for dashboards to filter for ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">dashboard_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div> |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">dashboard_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>  |                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">dashboard_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>  | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                            |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "RedashConfig",
  "type": "object",
  "properties": {
    "connect_uri": {
      "title": "Connect Uri",
      "description": "Redash base URL.",
      "default": "http://localhost:5000",
      "type": "string"
    },
    "api_key": {
      "title": "Api Key",
      "description": "Redash user API key.",
      "default": "REDASH_API_KEY",
      "type": "string"
    },
    "dashboard_patterns": {
      "title": "Dashboard Patterns",
      "description": "regex patterns for dashboards to filter for ingestion.",
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
    "chart_patterns": {
      "title": "Chart Patterns",
      "description": "regex patterns for charts to filter for ingestion.",
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
    "skip_draft": {
      "title": "Skip Draft",
      "description": "Only ingest published dashboards and charts.",
      "default": true,
      "type": "boolean"
    },
    "page_size": {
      "title": "Page Size",
      "description": "Limit on number of items to be queried at once.",
      "default": 25,
      "type": "integer"
    },
    "api_page_limit": {
      "title": "Api Page Limit",
      "description": "Limit on number of pages queried for ingesting dashboards and charts API during pagination.",
      "default": 9223372036854775807,
      "type": "integer"
    },
    "parallelism": {
      "title": "Parallelism",
      "description": "Parallelism to use while processing.",
      "default": 1,
      "type": "integer"
    },
    "parse_table_names_from_sql": {
      "title": "Parse Table Names From Sql",
      "description": "See note below.",
      "default": false,
      "type": "boolean"
    },
    "sql_parser": {
      "title": "Sql Parser",
      "description": "custom SQL parser. See note below for details.",
      "default": "datahub.utilities.sql_parser.DefaultSQLParser",
      "type": "string"
    },
    "env": {
      "title": "Env",
      "description": "Environment to use in namespace when constructing URNs.",
      "default": "PROD",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "definitions": {
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

Note! The integration can use an SQL parser to try to parse the tables the chart depends on. This parsing is disabled by default,
but can be enabled by setting `parse_table_names_from_sql: true`. The default parser is based on the [`sqllineage`](https://pypi.org/project/sqllineage/) package.
As this package doesn't officially support all the SQL dialects that Redash supports, the result might not be correct. You can, however, implement a
custom parser and take it into use by setting the `sql_parser` configuration value. A custom SQL parser must inherit from `datahub.utilities.sql_parser.SQLParser`
and must be made available to Datahub by ,for example, installing it. The configuration then needs to be set to `module_name.ClassName` of the parser.

### Code Coordinates

- Class Name: `datahub.ingestion.source.redash.RedashSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/redash.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Redash, feel free to ping us on [our Slack](https://slack.datahubproject.io).
