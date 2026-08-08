


# SnapLogic

## Overview

Snaplogic is a streaming or integration platform. Learn more in the [official Snaplogic documentation](https://www.snaplogic.com/).

The DataHub integration for Snaplogic covers streaming/integration entities such as topics, connectors, pipelines, or jobs. It also captures table- and column-level lineage.

## Concept Mapping

| Source Concept | DataHub Concept                                                    | Notes                                                                                                                                   |
| -------------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| Snap-pack      | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) | Snap-packs are mapped to Data Platforms, either directly (e.g., Snowflake) or dynamically based on connection details (e.g., JDBC URL). |
| Table/Dataset  | [Dataset](docs/generated/metamodel/entities/dataset.md)            | May be different. It depends on snap type. For SQL databases it is a table, and for Kafka it is a topic.                                |
| Snap           | [Data Job](docs/generated/metamodel/entities/dataJob.md)           |                                                                                                                                         |
| Pipeline       | [Data Flow](docs/generated/metamodel/entities/dataFlow.md)         |                                                                                                                                         |


## Module `snaplogic`
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Column-level Lineage | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ❌ | Not supported yet. |
| [Platform Instance](../../../platform-instances.md) | ❌ | SnapLogic does not support platform instances. |
| Table-Level Lineage | ✅ | Enabled by default. |

### Overview

The `snaplogic` module ingests metadata from SnapLogic into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

Extracts data lineage from the SnapLogic Lineage API to track data transformations and dependencies across SnapLogic pipelines.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

Requires valid SnapLogic credentials with access to the SnapLogic Lineage API.


### Install the Plugin
```shell
pip install 'acryl-datahub[snaplogic]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
pipeline_name: "snaplogic_incremental_ingestion"
source:
  type: snaplogic
  config:
    username: example@snaplogic.com
    password: password
    base_url: https://elastic.snaplogic.com
    org_name: "ExampleOrg"
    namespace_mapping:
      snowflake://snaplogic: snaplogic
    case_insensitive_namespaces:
      - snowflake://snaplogic
    stateful_ingestion:
      enabled: True
      remove_stale_metadata: False

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">org_name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Organization name from SnapLogic instance  |
| <div className="path-line"><span className="path-main">password</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Password  |
| <div className="path-line"><span className="path-main">username</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Username  |
| <div className="path-line"><span className="path-main">base_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Url to your SnapLogic instance: `https://elastic.snaplogic.com`, or similar. Used for making API calls to SnapLogic. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://elastic.snaplogic.com</span></div> |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-main">create_non_snaplogic_datasets</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to create datasets for non-SnapLogic datasets (e.g., databases, S3, etc.) <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_lineage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_usage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-main">namespace_mapping</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Mapping of namespaces to platform instances <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">SnapLogic</span></div> |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">case_insensitive_namespaces</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of namespaces that should be treated as case insensitive <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">case_insensitive_namespaces.</span><span className="path-main">object</span></div> <div className="type-name-line"><span className="type-name">object</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
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
    "enable_stateful_usage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead.",
      "title": "Enable Stateful Usage Ingestion",
      "type": "boolean"
    },
    "enable_stateful_lineage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead.",
      "title": "Enable Stateful Lineage Ingestion",
      "type": "boolean"
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
      "default": null
    },
    "platform": {
      "default": "SnapLogic",
      "title": "Platform",
      "type": "string"
    },
    "username": {
      "description": "Username",
      "title": "Username",
      "type": "string"
    },
    "password": {
      "description": "Password",
      "format": "password",
      "title": "Password",
      "type": "string",
      "writeOnly": true
    },
    "base_url": {
      "default": "https://elastic.snaplogic.com",
      "description": "Url to your SnapLogic instance: `https://elastic.snaplogic.com`, or similar. Used for making API calls to SnapLogic.",
      "title": "Base Url",
      "type": "string"
    },
    "org_name": {
      "description": "Organization name from SnapLogic instance",
      "title": "Org Name",
      "type": "string"
    },
    "namespace_mapping": {
      "additionalProperties": true,
      "default": {},
      "description": "Mapping of namespaces to platform instances",
      "title": "Namespace Mapping",
      "type": "object"
    },
    "case_insensitive_namespaces": {
      "default": [],
      "description": "List of namespaces that should be treated as case insensitive",
      "items": {},
      "title": "Case Insensitive Namespaces",
      "type": "array"
    },
    "create_non_snaplogic_datasets": {
      "default": false,
      "description": "Whether to create datasets for non-SnapLogic datasets (e.g., databases, S3, etc.)",
      "title": "Create Non Snaplogic Datasets",
      "type": "boolean"
    }
  },
  "required": [
    "username",
    "password",
    "org_name"
  ],
  "title": "SnaplogicConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.snaplogic.snaplogic.SnaplogicSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/snaplogic/snaplogic.py)


:::tip Questions?

If you've got any questions on configuring ingestion for SnapLogic, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
