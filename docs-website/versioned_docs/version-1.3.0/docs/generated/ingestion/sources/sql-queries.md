---
sidebar_position: 69
title: SQL Queries
slug: /generated/ingestion/sources/sql-queries
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/sql-queries.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SQL Queries
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Column-level Lineage | ✅ | Parsed from SQL queries. |
| Table-Level Lineage | ✅ | Parsed from SQL queries. |


This source reads a newline-delimited JSON file containing SQL queries and parses them to generate lineage.

### Query File Format
This file should contain one JSON object per line, with the following fields:
- query: string - The SQL query to parse.
- timestamp (optional): number - The timestamp of the query, in seconds since the epoch.
- user (optional): string - The user who ran the query.
This user value will be directly converted into a DataHub user urn.
- operation_type (optional): string - Platform-specific operation type, used if the operation type can't be parsed.
- session_id (optional): string - Session identifier for temporary table resolution across queries.
- downstream_tables (optional): string[] - Fallback list of tables that the query writes to,
 used if the query can't be parsed.
- upstream_tables (optional): string[] - Fallback list of tables the query reads from,
 used if the query can't be parsed.

### Incremental Lineage
When `incremental_lineage` is enabled, this source will emit lineage as patches rather than full overwrites.
This allows you to add lineage edges without removing existing ones, which is useful for:
- Gradually building up lineage from multiple sources
- Preserving manually curated lineage
- Avoiding conflicts when multiple ingestion processes target the same datasets

Note: Incremental lineage only applies to UpstreamLineage aspects. Other aspects like queries and usage
statistics will still be emitted normally.

#### Example Queries File

```json
{"query": "SELECT x FROM my_table", "timestamp": 1689232738.051, "user": "user_a", "downstream_tables": [], "upstream_tables": ["my_database.my_schema.my_table"]}
{"query": "INSERT INTO my_table VALUES (1, 'a')", "timestamp": 1689232737.669, "user": "user_b", "downstream_tables": ["my_database.my_schema.my_table"], "upstream_tables": []}
```

Note that this file does not represent a single JSON object, but instead newline-delimited JSON, in which
each line is a separate JSON object.

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
datahub_api:  # Only necessary if using a non-DataHub sink, e.g. the file sink
  server: http://localhost:8080
  timeout_sec: 60
source:
  type: sql-queries
  config:
    platform: "snowflake"
    default_db: "SNOWFLAKE"
    query_file: "./queries.json"

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">platform</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The platform for which to generate data, e.g. snowflake  |
| <div className="path-line"><span className="path-main">query_file</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to file to ingest  |
| <div className="path-line"><span className="path-main">default_db</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The default database to use for unqualified table names <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The default schema to use for unqualified table names <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">override_dialect</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The SQL dialect to use when parsing queries. Overrides automatic dialect detection. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">usage</span></div> <div className="type-name-line"><span className="type-name">BaseUsageConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report read operational stats. Experimental. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">include_top_n_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest the top_n_queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">top_n_queries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of top queries to save to each table. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">usage.</span><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">usage.user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

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
    "BaseUsageConfig": {
      "additionalProperties": false,
      "properties": {
        "bucket_duration": {
          "$ref": "#/$defs/BucketDuration",
          "default": "DAY",
          "description": "Size of the time window to aggregate usage stats."
        },
        "end_time": {
          "description": "Latest date of lineage/usage to consider. Default: Current time in UTC",
          "format": "date-time",
          "title": "End Time",
          "type": "string"
        },
        "start_time": {
          "default": null,
          "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
          "format": "date-time",
          "title": "Start Time",
          "type": "string"
        },
        "top_n_queries": {
          "default": 10,
          "description": "Number of top queries to save to each table.",
          "exclusiveMinimum": 0,
          "title": "Top N Queries",
          "type": "integer"
        },
        "user_email_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "regex patterns for user emails to filter in usage."
        },
        "include_operational_stats": {
          "default": true,
          "description": "Whether to display operational stats.",
          "title": "Include Operational Stats",
          "type": "boolean"
        },
        "include_read_operational_stats": {
          "default": false,
          "description": "Whether to report read operational stats. Experimental.",
          "title": "Include Read Operational Stats",
          "type": "boolean"
        },
        "format_sql_queries": {
          "default": false,
          "description": "Whether to format sql queries",
          "title": "Format Sql Queries",
          "type": "boolean"
        },
        "include_top_n_queries": {
          "default": true,
          "description": "Whether to ingest the top_n_queries.",
          "title": "Include Top N Queries",
          "type": "boolean"
        }
      },
      "title": "BaseUsageConfig",
      "type": "object"
    },
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "properties": {
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
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
    "query_file": {
      "description": "Path to file to ingest",
      "title": "Query File",
      "type": "string"
    },
    "platform": {
      "description": "The platform for which to generate data, e.g. snowflake",
      "title": "Platform",
      "type": "string"
    },
    "usage": {
      "$ref": "#/$defs/BaseUsageConfig",
      "default": {
        "bucket_duration": "DAY",
        "end_time": "2025-10-06T21:15:18.001955Z",
        "start_time": "2025-10-05T00:00:00Z",
        "queries_character_limit": 24000,
        "top_n_queries": 10,
        "user_email_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "include_operational_stats": true,
        "include_read_operational_stats": false,
        "format_sql_queries": false,
        "include_top_n_queries": true
      },
      "description": "The usage config to use when generating usage statistics"
    },
    "default_db": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The default database to use for unqualified table names",
      "title": "Default Db"
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
      "description": "The default schema to use for unqualified table names",
      "title": "Default Schema"
    },
    "override_dialect": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The SQL dialect to use when parsing queries. Overrides automatic dialect detection.",
      "title": "Override Dialect"
    }
  },
  "required": [
    "query_file",
    "platform"
  ],
  "title": "SqlQueriesSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.sql_queries.SqlQueriesSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sql_queries.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for SQL Queries, feel free to ping us on [our Slack](https://datahub.com/slack).
