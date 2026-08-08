


# DataHubGc

## Overview

DataHub GC is a DataHub utility or metadata-focused integration. Learn more in the [official DataHub GC documentation](https://datahub.com/docs/).

The DataHub integration for DataHub GC covers metadata entities and operational objects relevant to this connector. It performs soft-deletion of stale entities using configurable retention policies.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `datahub-gc`
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
Capability metadata is not explicitly declared for this module. Refer to module documentation and configuration sections below.

### Overview

The DataHub Garbage Collection (GC) source is a maintenance component responsible for cleaning up various types of metadata to maintain system performance and data quality. It performs multiple cleanup tasks, each focusing on different aspects of DataHub's metadata.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.


### Install the Plugin
```shell
pip install 'acryl-datahub[datahub-gc]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: datahub-gc
  config:
    dry_run: false
    cleanup_expired_tokens: true
    truncate_indices: true
    dataprocess_cleanup:
      retention_days: 10
      delete_empty_data_jobs: true
      delete_empty_data_flows: true
      hard_delete_entities: false
      keep_last_n: 5
    query_cleanup:
      enabled: true
      retention_days: 180

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">cleanup_expired_tokens</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to clean up expired tokens or not <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">dry_run</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform a dry run or not. This is only supported for dataprocess cleanup, query cleanup, and soft deleted entities cleanup. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">truncate_index_older_than_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Indices older than this number of days will be truncated <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">truncate_indices</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to truncate elasticsearch indices or not which can be safely truncated <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">truncation_sleep_between_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Sleep between truncation monitoring. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">truncation_watch_until</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Wait for truncation of indices until this number of documents are left <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-main">dataprocess_cleanup</span></div> <div className="type-name-line"><span className="type-name">DataProcessCleanupConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The number of entities to get in a batch from API <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">delay</span></div> <div className="type-name-line"><span className="type-name">One of number, null</span></div> | Delay between each batch <div className="default-line default-line-with-docs">Default: <span className="default-value">0.25</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">delete_empty_data_flows</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to delete Data Flows without runs <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">delete_empty_data_jobs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to delete Data Jobs without runs <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do data process cleanup. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">hard_delete_entities</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to hard delete entities <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">keep_last_n</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number of latest aspects to keep <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The number of workers to use for deletion <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">retention_days</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number of days to retain metadata in DataHub <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.</span><span className="path-main">aspects_to_clean</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of aspect names to clean up <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;DataprocessInstance&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">dataprocess_cleanup.aspects_to_clean.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">execution_request_cleanup</span></div> <div className="type-name-line"><span className="type-name">DatahubExecutionRequestCleanupConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.</span><span className="path-main">batch_read_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of records per read operation <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Global switch for this cleanup task <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.</span><span className="path-main">keep_history_max_count</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of execution requests to keep, per ingestion source <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.</span><span className="path-main">keep_history_max_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of days to keep execution requests for, per ingestion source <div className="default-line default-line-with-docs">Default: <span className="default-value">90</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.</span><span className="path-main">keep_history_min_count</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Minimum number of execution requests to keep, per ingestion source <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.</span><span className="path-main">limit_entities_delete</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Max number of execution requests to hard delete. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.</span><span className="path-main">max_read_errors</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of read errors before aborting <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.</span><span className="path-main">runtime_limit_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum runtime in seconds for the cleanup task <div className="default-line default-line-with-docs">Default: <span className="default-value">3600</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.</span><span className="path-main">source_type_overrides</span></div> <div className="type-name-line"><span className="type-name">map(str,DatahubExecutionRequestCleanupSourceTypeOverride)</span></div> | Per-``source.type`` retention override. <br />  <br /> Any field left unset falls back to the corresponding top-level default.  |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.source_type_overrides.`key`.</span><span className="path-main">keep_history_min_count</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Overrides keep_history_min_count for this source type; falls back to the global value when unset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.source_type_overrides.`key`.</span><span className="path-main">keep_history_max_count</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Overrides keep_history_max_count for this source type; falls back to the global value when unset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">execution_request_cleanup.source_type_overrides.`key`.</span><span className="path-main">keep_history_max_days</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Overrides keep_history_max_days for this source type; falls back to the global value when unset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">query_cleanup</span></div> <div className="type-name-line"><span className="type-name">QueryCleanupConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">query_cleanup.</span><span className="path-main">batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The number of entities to fetch in a batch from search. <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-prefix">query_cleanup.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do query cleanup or not. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">query_cleanup.</span><span className="path-main">limit_entities_delete</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Approximate max number of queries to soft-delete in a single run. Set to null to disable the cap. <div className="default-line default-line-with-docs">Default: <span className="default-value">25000</span></div> |
| <div className="path-line"><span className="path-prefix">query_cleanup.</span><span className="path-main">retention_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Soft-delete SYSTEM queries whose lastModifiedAt is older than this many days. Defaults to 180 days (~6 months), a conservative window that avoids touching queries still within any reasonable connector ingestion window. Set this to at least the largest connector ingestion window: a live query's lastModifiedAt is refreshed on every re-observation, so only queries that have aged out of every ingestion window can cross the cutoff. This is a distinct clock from soft_deleted_entities_cleanup.retention_days (which measures time since soft-deletion). <div className="default-line default-line-with-docs">Default: <span className="default-value">180</span></div> |
| <div className="path-line"><span className="path-prefix">query_cleanup.</span><span className="path-main">runtime_limit_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Runtime limit in seconds for a single run. <div className="default-line default-line-with-docs">Default: <span className="default-value">7200</span></div> |
| <div className="path-line"><span className="path-main">soft_deleted_entities_cleanup</span></div> <div className="type-name-line"><span className="type-name">SoftDeletedEntitiesCleanupConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The number of entities to get in a batch from GraphQL <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">delay</span></div> <div className="type-name-line"><span className="type-name">One of number, null</span></div> | Delay between each batch <div className="default-line default-line-with-docs">Default: <span className="default-value">0.25</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do soft deletion cleanup. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">futures_max_at_time</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Max number of futures to have at a time. <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">limit_entities_delete</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Max number of entities to delete. <div className="default-line default-line-with-docs">Default: <span className="default-value">25000</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The number of workers to use for deletion <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform to cleanup <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">query</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Query to filter entities <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">retention_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of days to retain metadata in DataHub <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">runtime_limit_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Runtime limit in seconds <div className="default-line default-line-with-docs">Default: <span className="default-value">7200</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Environment to cleanup <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.</span><span className="path-main">entity_types</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | List of entity types to cleanup <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;dataset&#x27;, &#x27;dashboard&#x27;, &#x27;chart&#x27;, &#x27;mlmodel&#x27;, &#x27;mlmo...</span></div> |
| <div className="path-line"><span className="path-prefix">soft_deleted_entities_cleanup.entity_types.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "DataProcessCleanupConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": true,
          "description": "Whether to do data process cleanup.",
          "title": "Enabled",
          "type": "boolean"
        },
        "retention_days": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 10,
          "description": "Number of days to retain metadata in DataHub",
          "title": "Retention Days"
        },
        "aspects_to_clean": {
          "default": [
            "DataprocessInstance"
          ],
          "description": "List of aspect names to clean up",
          "items": {
            "type": "string"
          },
          "title": "Aspects To Clean",
          "type": "array"
        },
        "keep_last_n": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5,
          "description": "Number of latest aspects to keep",
          "title": "Keep Last N"
        },
        "delete_empty_data_jobs": {
          "default": false,
          "description": "Whether to delete Data Jobs without runs",
          "title": "Delete Empty Data Jobs",
          "type": "boolean"
        },
        "delete_empty_data_flows": {
          "default": false,
          "description": "Whether to delete Data Flows without runs",
          "title": "Delete Empty Data Flows",
          "type": "boolean"
        },
        "hard_delete_entities": {
          "default": false,
          "description": "Whether to hard delete entities",
          "title": "Hard Delete Entities",
          "type": "boolean"
        },
        "batch_size": {
          "default": 500,
          "description": "The number of entities to get in a batch from API",
          "title": "Batch Size",
          "type": "integer"
        },
        "max_workers": {
          "default": 10,
          "description": "The number of workers to use for deletion",
          "title": "Max Workers",
          "type": "integer"
        },
        "delay": {
          "anyOf": [
            {
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": 0.25,
          "description": "Delay between each batch",
          "title": "Delay"
        }
      },
      "title": "DataProcessCleanupConfig",
      "type": "object"
    },
    "DatahubExecutionRequestCleanupConfig": {
      "additionalProperties": false,
      "properties": {
        "keep_history_min_count": {
          "default": 10,
          "description": "Minimum number of execution requests to keep, per ingestion source",
          "title": "Keep History Min Count",
          "type": "integer"
        },
        "keep_history_max_count": {
          "default": 1000,
          "description": "Maximum number of execution requests to keep, per ingestion source",
          "title": "Keep History Max Count",
          "type": "integer"
        },
        "keep_history_max_days": {
          "default": 90,
          "description": "Maximum number of days to keep execution requests for, per ingestion source",
          "title": "Keep History Max Days",
          "type": "integer"
        },
        "batch_read_size": {
          "default": 100,
          "description": "Number of records per read operation",
          "title": "Batch Read Size",
          "type": "integer"
        },
        "enabled": {
          "default": true,
          "description": "Global switch for this cleanup task",
          "title": "Enabled",
          "type": "boolean"
        },
        "runtime_limit_seconds": {
          "default": 3600,
          "description": "Maximum runtime in seconds for the cleanup task",
          "title": "Runtime Limit Seconds",
          "type": "integer"
        },
        "limit_entities_delete": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 10000,
          "description": "Max number of execution requests to hard delete.",
          "title": "Limit Entities Delete"
        },
        "max_read_errors": {
          "default": 10,
          "description": "Maximum number of read errors before aborting",
          "title": "Max Read Errors",
          "type": "integer"
        },
        "source_type_overrides": {
          "additionalProperties": {
            "$ref": "#/$defs/DatahubExecutionRequestCleanupSourceTypeOverride"
          },
          "description": "Per-source.type retention overrides, keyed by the execution request source.type. Applies a distinct retention policy to non-ingestion execution requests, which are grouped by source.type rather than by ingestion source URN.",
          "title": "Source Type Overrides",
          "type": "object"
        }
      },
      "title": "DatahubExecutionRequestCleanupConfig",
      "type": "object"
    },
    "DatahubExecutionRequestCleanupSourceTypeOverride": {
      "additionalProperties": false,
      "description": "Per-``source.type`` retention override.\n\nAny field left unset falls back to the corresponding top-level default.",
      "properties": {
        "keep_history_min_count": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Overrides keep_history_min_count for this source type; falls back to the global value when unset.",
          "title": "Keep History Min Count"
        },
        "keep_history_max_count": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Overrides keep_history_max_count for this source type; falls back to the global value when unset.",
          "title": "Keep History Max Count"
        },
        "keep_history_max_days": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Overrides keep_history_max_days for this source type; falls back to the global value when unset.",
          "title": "Keep History Max Days"
        }
      },
      "title": "DatahubExecutionRequestCleanupSourceTypeOverride",
      "type": "object"
    },
    "QueryCleanupConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": true,
          "description": "Whether to do query cleanup or not.",
          "title": "Enabled",
          "type": "boolean"
        },
        "retention_days": {
          "default": 180,
          "description": "Soft-delete SYSTEM queries whose lastModifiedAt is older than this many days. Defaults to 180 days (~6 months), a conservative window that avoids touching queries still within any reasonable connector ingestion window. Set this to at least the largest connector ingestion window: a live query's lastModifiedAt is refreshed on every re-observation, so only queries that have aged out of every ingestion window can cross the cutoff. This is a distinct clock from soft_deleted_entities_cleanup.retention_days (which measures time since soft-deletion).",
          "minimum": 1,
          "title": "Retention Days",
          "type": "integer"
        },
        "batch_size": {
          "default": 500,
          "description": "The number of entities to fetch in a batch from search.",
          "minimum": 1,
          "title": "Batch Size",
          "type": "integer"
        },
        "limit_entities_delete": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 25000,
          "description": "Approximate max number of queries to soft-delete in a single run. Set to null to disable the cap.",
          "title": "Limit Entities Delete"
        },
        "runtime_limit_seconds": {
          "default": 7200,
          "description": "Runtime limit in seconds for a single run.",
          "minimum": 1,
          "title": "Runtime Limit Seconds",
          "type": "integer"
        }
      },
      "title": "QueryCleanupConfig",
      "type": "object"
    },
    "SoftDeletedEntitiesCleanupConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": true,
          "description": "Whether to do soft deletion cleanup.",
          "title": "Enabled",
          "type": "boolean"
        },
        "retention_days": {
          "default": 10,
          "description": "Number of days to retain metadata in DataHub",
          "title": "Retention Days",
          "type": "integer"
        },
        "batch_size": {
          "default": 500,
          "description": "The number of entities to get in a batch from GraphQL",
          "title": "Batch Size",
          "type": "integer"
        },
        "delay": {
          "anyOf": [
            {
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": 0.25,
          "description": "Delay between each batch",
          "title": "Delay"
        },
        "max_workers": {
          "default": 10,
          "description": "The number of workers to use for deletion",
          "title": "Max Workers",
          "type": "integer"
        },
        "entity_types": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": [
            "dataset",
            "dashboard",
            "chart",
            "mlmodel",
            "mlmodelGroup",
            "mlfeatureTable",
            "mlfeature",
            "mlprimaryKey",
            "dataFlow",
            "dataJob",
            "glossaryTerm",
            "glossaryNode",
            "tag",
            "role",
            "corpuser",
            "corpGroup",
            "container",
            "domain",
            "dataProduct",
            "notebook",
            "businessAttribute",
            "schemaField",
            "query",
            "dataProcessInstance"
          ],
          "description": "List of entity types to cleanup",
          "title": "Entity Types"
        },
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Platform to cleanup",
          "title": "Platform"
        },
        "env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Environment to cleanup",
          "title": "Env"
        },
        "query": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Query to filter entities",
          "title": "Query"
        },
        "limit_entities_delete": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 25000,
          "description": "Max number of entities to delete.",
          "title": "Limit Entities Delete"
        },
        "futures_max_at_time": {
          "default": 1000,
          "description": "Max number of futures to have at a time.",
          "title": "Futures Max At Time",
          "type": "integer"
        },
        "runtime_limit_seconds": {
          "default": 7200,
          "description": "Runtime limit in seconds",
          "title": "Runtime Limit Seconds",
          "type": "integer"
        }
      },
      "title": "SoftDeletedEntitiesCleanupConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "dry_run": {
      "default": false,
      "description": "Whether to perform a dry run or not. This is only supported for dataprocess cleanup, query cleanup, and soft deleted entities cleanup.",
      "title": "Dry Run",
      "type": "boolean"
    },
    "cleanup_expired_tokens": {
      "default": true,
      "description": "Whether to clean up expired tokens or not",
      "title": "Cleanup Expired Tokens",
      "type": "boolean"
    },
    "truncate_indices": {
      "default": true,
      "description": "Whether to truncate elasticsearch indices or not which can be safely truncated",
      "title": "Truncate Indices",
      "type": "boolean"
    },
    "truncate_index_older_than_days": {
      "default": 30,
      "description": "Indices older than this number of days will be truncated",
      "title": "Truncate Index Older Than Days",
      "type": "integer"
    },
    "truncation_watch_until": {
      "default": 10000,
      "description": "Wait for truncation of indices until this number of documents are left",
      "title": "Truncation Watch Until",
      "type": "integer"
    },
    "truncation_sleep_between_seconds": {
      "default": 30,
      "description": "Sleep between truncation monitoring.",
      "title": "Truncation Sleep Between Seconds",
      "type": "integer"
    },
    "dataprocess_cleanup": {
      "$ref": "#/$defs/DataProcessCleanupConfig",
      "description": "Configuration for data process cleanup"
    },
    "soft_deleted_entities_cleanup": {
      "$ref": "#/$defs/SoftDeletedEntitiesCleanupConfig",
      "description": "Configuration for soft deleted entities cleanup"
    },
    "execution_request_cleanup": {
      "$ref": "#/$defs/DatahubExecutionRequestCleanupConfig",
      "description": "Configuration for execution request cleanup"
    },
    "query_cleanup": {
      "$ref": "#/$defs/QueryCleanupConfig",
      "description": "Configuration for query cleanup"
    }
  },
  "title": "DataHubGcSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Index Cleanup

Manages Elasticsearch indices in DataHub, particularly focusing on time-series data.

##### Configuration

```yaml
source:
  type: datahub-gc
  config:
    truncate_indices: true
    truncate_index_older_than_days: 30
    truncation_watch_until: 10000
    truncation_sleep_between_seconds: 30
```

##### Features

- Truncates old Elasticsearch indices for the following timeseries aspects:
  - DatasetOperations
  - DatasetUsageStatistics
  - ChartUsageStatistics
  - DashboardUsageStatistics
  - QueryUsageStatistics
  - Timeseries Aspects
- Monitors truncation progress
- Implements safe deletion with monitoring thresholds
- Supports gradual truncation with sleep intervals

#### Expired Token Cleanup

Manages access tokens in DataHub to maintain security and prevent token accumulation.

##### Configuration

```yaml
source:
  type: datahub-gc
  config:
    cleanup_expired_tokens: true
```

##### Features

- Automatically identifies and revokes expired access tokens
- Processes tokens in batches for efficiency
- Maintains system security by removing outdated credentials
- Reports number of tokens revoked
- Uses GraphQL API for token management

#### Data Process Cleanup

Manages the lifecycle of data processes, jobs, and their instances (DPIs) within DataHub.

##### Features

- Cleans up Data Process Instances (DPIs) based on age and count
- Can remove empty DataJobs and DataFlows
- Supports both soft and hard deletion
- Uses parallel processing for efficient cleanup
- Maintains configurable retention policies

##### Configuration

```yaml
source:
  type: datahub-gc
  config:
    dataprocess_cleanup:
      enabled: true
      retention_days: 10
      keep_last_n: 5
      delete_empty_data_jobs: false
      delete_empty_data_flows: false
      hard_delete_entities: false
      batch_size: 500
      max_workers: 10
      delay: 0.25
```

##### Limitations

- Maximum 9000 DPIs per job for performance

#### Execution Request Cleanup

Manages DataHub execution request records to prevent accumulation of historical execution data.

##### Features

- Maintains execution history per ingestion source
- Preserves minimum number of recent requests
- Removes old requests beyond retention period
- Special handling for running/pending requests
- Automatic cleanup of corrupted records

##### Configuration

```yaml
source:
  type: datahub-gc
  config:
    execution_request_cleanup:
      enabled: true
      keep_history_min_count: 10
      keep_history_max_count: 1000
      keep_history_max_days: 30
      batch_read_size: 100
      runtime_limit_seconds: 3600
      max_read_errors: 10
```

#### Query Cleanup

Soft-deletes old `SYSTEM` queries so stale query entities don't accumulate as orphans in Elasticsearch and primary storage. This is the soft-delete first pass; the Soft-Deleted Entities Cleanup pass below completes the hard delete on a later run.

##### Features

- Selects `SYSTEM` queries whose `lastModifiedAt` is older than `retention_days` using server-side search filters (no per-query reference lookup)
- Never touches `MANUAL` queries
- Soft-deletes matched queries rather than hard-deleting them directly
- Soft deletes are emitted as status workunits and batched/written by the sink
- Bounded by `limit_entities_delete` (approximate cap on queries deleted per run) and `runtime_limit_seconds`

##### Configuration

```yaml
source:
  type: datahub-gc
  config:
    query_cleanup:
      enabled: true # Enabled by default
      retention_days: 180 # Age cutoff on lastModifiedAt (~6 months, conservative default)
      batch_size: 500
      limit_entities_delete: 25000
      runtime_limit_seconds: 7200
```

Set `retention_days` to at least your largest connector ingestion window: a live query's `lastModifiedAt` is refreshed on every re-observation, so only queries that have aged out of every ingestion window can cross the cutoff. This is a distinct clock from `soft_deleted_entities_cleanup.retention_days` (which measures time since soft-deletion).

#### Soft-Deleted Entities Cleanup

Manages the permanent removal of soft-deleted entities after a retention period.

##### Features

- Permanently removes soft-deleted entities after retention period
- Handles entity references cleanup
- Special handling for query entities
- Supports filtering by entity type, platform, or environment
- Concurrent processing with safety limits

##### Configuration

```yaml
source:
  type: datahub-gc
  config:
    soft_deleted_entities_cleanup:
      enabled: true
      retention_days: 10
      batch_size: 500
      max_workers: 10
      delay: 0.25
      entity_types: null # Optional list of entity types to clean
      platform: null # Optional platform filter
      env: null # Optional environment filter
      query: null # Optional custom query filter
      limit_entities_delete: 25000
      futures_max_at_time: 1000
      runtime_limit_seconds: 7200
```

##### Performance Considerations

- Concurrent processing using thread pools
- Configurable batch sizes for optimal performance
- Rate limiting through configurable delays
- Maximum limits on concurrent operations

#### Reporting

Each cleanup task maintains detailed reports including:

- Number of entities processed
- Number of entities removed
- Errors encountered
- Sample of affected entities
- Runtime statistics
- Task-specific metrics

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.gc.datahub_gc.DataHubGcSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/gc/datahub_gc.py)


:::tip Questions?

If you've got any questions on configuring ingestion for DataHubGc, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
