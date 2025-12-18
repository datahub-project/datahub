---
sidebar_position: 58
title: Redash
slug: /generated/ingestion/sources/redash
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/redash.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Redash
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Table-Level Lineage | ✅ | Enabled by default. |


This plugin extracts the following:

- Redash dashboards and queries/visualization
- Redash chart table lineages (disabled by default)


### CLI based Ingestion

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

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Redash user API key. <div className="default-line default-line-with-docs">Default: <span className="default-value">REDASH&#95;API&#95;KEY</span></div> |
| <div className="path-line"><span className="path-main">api_page_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Limit on number of pages queried for ingesting dashboards and charts API during pagination. <div className="default-line default-line-with-docs">Default: <span className="default-value">9223372036854775807</span></div> |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Redash base URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">http://localhost:5000</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Limit on number of items to be queried at once. <div className="default-line default-line-with-docs">Default: <span className="default-value">25</span></div> |
| <div className="path-line"><span className="path-main">parallelism</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Parallelism to use while processing. <div className="default-line default-line-with-docs">Default: <span className="default-value">1</span></div> |
| <div className="path-line"><span className="path-main">parse_table_names_from_sql</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | See note below. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">skip_draft</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Only ingest published dashboards and charts. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Environment to use in namespace when constructing URNs. <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">chart_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">chart_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">chart_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">chart_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">chart_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">chart_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">dashboard_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dashboard_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">dashboard_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">dashboard_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">dashboard_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">dashboard_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
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
  "properties": {
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
    "connect_uri": {
      "default": "http://localhost:5000",
      "description": "Redash base URL.",
      "title": "Connect Uri",
      "type": "string"
    },
    "api_key": {
      "default": "REDASH_API_KEY",
      "description": "Redash user API key.",
      "title": "Api Key",
      "type": "string"
    },
    "dashboard_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for dashboards to filter for ingestion."
    },
    "chart_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for charts to filter for ingestion."
    },
    "skip_draft": {
      "default": true,
      "description": "Only ingest published dashboards and charts.",
      "title": "Skip Draft",
      "type": "boolean"
    },
    "page_size": {
      "default": 25,
      "description": "Limit on number of items to be queried at once.",
      "title": "Page Size",
      "type": "integer"
    },
    "api_page_limit": {
      "default": 9223372036854775807,
      "description": "Limit on number of pages queried for ingesting dashboards and charts API during pagination.",
      "title": "Api Page Limit",
      "type": "integer"
    },
    "parallelism": {
      "default": 1,
      "description": "Parallelism to use while processing.",
      "title": "Parallelism",
      "type": "integer"
    },
    "parse_table_names_from_sql": {
      "default": false,
      "description": "See note below.",
      "title": "Parse Table Names From Sql",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "Environment to use in namespace when constructing URNs.",
      "title": "Env",
      "type": "string"
    }
  },
  "title": "RedashConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>

Note! The integration can use an SQL parser to try to parse the tables the chart depends on. This parsing is disabled by default,
but can be enabled by setting `parse_table_names_from_sql: true`. The parser is based on the [`sqlglot`](https://pypi.org/project/sqlglot/) package.

### Code Coordinates
- Class Name: `datahub.ingestion.source.redash.RedashSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/redash.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Redash, feel free to ping us on [our Slack](https://datahub.com/slack).
