---
sidebar_position: 26
title: Metabase
slug: /generated/ingestion/sources/metabase
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/metabase.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Metabase

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                          | Status | Notes              |
| --------------------------------------------------- | ------ | ------------------ |
| [Platform Instance](../../../platform-instances.md) | ✅     | Enabled by default |

This plugin extracts Charts, dashboards, and associated metadata. This plugin is in beta and has only been tested
on PostgreSQL and H2 database.

### Dashboard

[/api/dashboard](https://www.metabase.com/docs/latest/api-documentation.html#dashboard) endpoint is used to
retrieve the following dashboard information.

- Title and description
- Last edited by
- Owner
- Link to the dashboard in Metabase
- Associated charts

### Chart

[/api/card](https://www.metabase.com/docs/latest/api-documentation.html#card) endpoint is used to
retrieve the following information.

- Title and description
- Last edited by
- Owner
- Link to the chart in Metabase
- Datasource and lineage

The following properties for a chart are ingested in DataHub.

| Name         | Description                                     |
| ------------ | ----------------------------------------------- |
| `Dimensions` | Column names                                    |
| `Filters`    | Any filters applied to the chart                |
| `Metrics`    | All columns that are being used for aggregation |

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[metabase]'
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                              | Description                                                                                                                                                                              |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">connect_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                    | Metabase host URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">localhost:3000</span></div>                                             |
| <div className="path-line"><span className="path-main">database_alias_map</span></div> <div className="type-name-line"><span className="type-name">object</span></div>             | Database name map to use when constructing dataset URN.                                                                                                                                  |
| <div className="path-line"><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                 | Default schema name to use when schema is not provided in an SQL query <div className="default-line default-line-with-docs">Default: <span className="default-value">public</span></div> |
| <div className="path-line"><span className="path-main">engine_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>   |                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string(password)</span></div>             | Metabase password.                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                       | Metabase username.                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                            | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>     |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "MetabaseConfig",
  "description": "Any non-Dataset source that produces lineage to Datasets should inherit this class.\ne.g. Orchestrators, Pipelines, BI Tools etc.",
  "type": "object",
  "properties": {
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance_map": {
      "title": "Platform Instance Map",
      "description": "A holder for platform -> platform_instance mappings to generate correct dataset urns",
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    },
    "connect_uri": {
      "title": "Connect Uri",
      "description": "Metabase host URL.",
      "default": "localhost:3000",
      "type": "string"
    },
    "username": {
      "title": "Username",
      "description": "Metabase username.",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "Metabase password.",
      "type": "string",
      "writeOnly": true,
      "format": "password"
    },
    "database_alias_map": {
      "title": "Database Alias Map",
      "description": "Database name map to use when constructing dataset URN.",
      "type": "object"
    },
    "engine_platform_map": {
      "title": "Engine Platform Map",
      "description": "Custom mappings between metabase database engines and DataHub platforms",
      "type": "object",
      "additionalProperties": {
        "type": "string"
      }
    },
    "default_schema": {
      "title": "Default Schema",
      "description": "Default schema name to use when schema is not provided in an SQL query",
      "default": "public",
      "type": "string"
    }
  },
  "additionalProperties": false
}
```

</TabItem>
</Tabs>

Metabase databases will be mapped to a DataHub platform based on the engine listed in the
[api/database](https://www.metabase.com/docs/latest/api-documentation.html#database) response. This mapping can be
customized by using the `engine_platform_map` config option. For example, to map databases using the `athena` engine to
the underlying datasets in the `glue` platform, the following snippet can be used:

```yml
engine_platform_map:
  athena: glue
```

DataHub will try to determine database name from Metabase [api/database](https://www.metabase.com/docs/latest/api-documentation.html#database)
payload. However, the name can be overridden from `database_alias_map` for a given database connected to Metabase.

## Compatibility

Metabase version [v0.41.2](https://www.metabase.com/start/oss/)

### Code Coordinates

- Class Name: `datahub.ingestion.source.metabase.MetabaseSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/metabase.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Metabase, feel free to ping us on [our Slack](https://slack.datahubproject.io).
