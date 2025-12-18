---
sidebar_position: 11
title: DataHub
slug: /generated/ingestion/sources/datahub
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/datahub.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DataHub
Migrate data from one DataHub instance to another.

Requires direct access to the database, kafka broker, and kafka schema registry
of the source DataHub instance.
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Database. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |


### Overview

This source pulls data from two locations:

- The DataHub database, containing a single table holding all versioned aspects
- The DataHub Kafka cluster, reading from the [MCL Log](../../../../docs/what/mxe.md#metadata-change-log-mcl)
  topic for timeseries aspects.

All data is first read from the database, before timeseries data is ingested from kafka.
To prevent this source from potentially running forever, it will not ingest data produced after the
datahub_source ingestion job is started. This `stop_time` is reflected in the report.

Data from the database and kafka are read in chronological order, specifically by the
createdon timestamp in the database and by kafka offset per partition. In order to
properly read from the database, please ensure that the `createdon` column is indexed.
Newly created databases should have this index, named `timeIndex`, by default, but older
ones you may have to create yourself, with the statement:

```
CREATE INDEX timeIndex ON metadata_aspect_v2 (createdon);
```

_If you do not have this index, the source may run incredibly slowly and produce
significant database load._

#### Stateful Ingestion

On first run, the source will read from the earliest data in the database and the earliest
kafka offsets. Every `commit_state_interval` (default 1000) records, the source will store
a checkpoint to remember its place, i.e. the last createdon timestamp and kafka offsets.
This allows you to stop and restart the source without losing much progress, but note that
you will re-ingest some data at the start of the new run.

If any errors are encountered in the ingestion process, e.g. we are unable to emit an aspect
due to network errors, the source will keep running, but will stop committing checkpoints,
unless `commit_with_parse_errors` (default `false`) is set. Thus, if you re-run the ingestion,
you can re-ingest the data that was missed, but note it will all re-ingest all subsequent data.

If you want to re-ingest all data, you can set a different `pipeline_name` in your recipe,
or set `stateful_ingestion.ignore_old_state`:

```yaml
source:
  config:
    # ... connection config, etc.
    stateful_ingestion:
      enabled: true
      ignore_old_state: true
    urn_pattern: # URN pattern to ignore/include in the ingestion
      deny:
        # Ignores all datahub metadata where the urn matches the regex
        - ^denied.urn.*
      allow:
        # Ingests all datahub metadata where the urn matches the regex.
        - ^allowed.urn.*
```

#### Limitations

- Can only pull timeseries aspects retained by Kafka, which by default lasts 90 days.
- Does not detect hard timeseries deletions, e.g. if via a `datahub delete` command using the CLI.
  Therefore, if you deleted data in this way, it will still exist in the destination instance.
- If you have a significant amount of aspects with the exact same `createdon` timestamp,
  stateful ingestion will not be able to save checkpoints partially through that timestamp.
  On a subsequent run, all aspects for that timestamp will be ingested.

#### Performance

On your destination DataHub instance, we suggest the following settings:

- Enable [async ingestion](../../../../docs/deploy/environment-vars.md#ingestion)
- Use standalone consumers
  ([mae-consumer](../../../../metadata-jobs/mae-consumer-job/README.md)
  and [mce-consumer](../../../../metadata-jobs/mce-consumer-job/README.md))
  - If you are migrating large amounts of data, consider scaling consumer replicas.
- Increase the number of gms pods to add redundancy and increase resilience to node evictions
  - If you are migrating large amounts of data, consider increasing elasticsearch's
    thread count via the `ELASTICSEARCH_THREAD_COUNT` environment variable.

#### Exclusions

You will likely want to exclude some urn types from your ingestion, as they contain instance-specific
metadata, such as settings, roles, policies, ingestion sources, and ingestion runs. For example, you
will likely want to start with this:

```yaml
source:
  config:
    urn_pattern: # URN pattern to ignore/include in the ingestion
      deny:
        # Ignores all datahub metadata where the urn matches the regex
        - ^urn:li:role.* # Only exclude if you do not want to ingest roles
        - ^urn:li:dataHubRole.* # Only exclude if you do not want to ingest roles
        - ^urn:li:dataHubPolicy.* # Only exclude if you do not want to ingest policies
        - ^urn:li:dataHubIngestionSource.* # Only exclude if you do not want to ingest ingestion sources
        - ^urn:li:dataHubSecret.*
        - ^urn:li:dataHubExecutionRequest.*
        - ^urn:li:dataHubAccessToken.*
        - ^urn:li:dataHubUpgrade.*
        - ^urn:li:inviteToken.*
        - ^urn:li:globalSettings.*
        - ^urn:li:dataHubStepState.*
```

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
pipeline_name: datahub_source_1
datahub_api:
  server: "http://localhost:8080" # Migrate data from DataHub instance on localhost:8080
  token: "<token>"
source:
  type: datahub
  config:
    include_all_versions: false
    database_connection:
      scheme: "mysql+pymysql" # or "postgresql+psycopg2" for Postgres
      host_port: "<database_host>:<database_port>"
      username: "<username>"
      password: "<password>"
      database: "<database>"
    kafka_connection:
      bootstrap: "<boostrap_url>:9092"
      schema_registry_url: "<schema_registry_url>:8081"
    stateful_ingestion:
      enabled: true
      ignore_old_state: false
    urn_pattern:
      deny:
        # Ignores all datahub metadata where the urn matches the regex
        - ^denied.urn.*
      allow:
        # Ingests all datahub metadata where the urn matches the regex.
        - ^allowed.urn.*

flags:
  set_system_metadata: false # Replicate system metadata

# Here, we write to a DataHub instance
# You can also use a different sink, e.g. to write the data to a file instead
sink:
  type: datahub-rest
  config:
    server: "<destination_gms_url>"
    token: "<token>"

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">commit_state_interval</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number of records to process before committing state <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">commit_with_parse_errors</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to update createdon timestamp and kafka offset despite parse errors. Enable if you want to ignore the errors. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">database_query_batch_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of records to fetch from the database at a time <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-main">database_table_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of database table containing all versioned aspects <div className="default-line default-line-with-docs">Default: <span className="default-value">metadata&#95;aspect&#95;v2</span></div> |
| <div className="path-line"><span className="path-main">drop_duplicate_schema_fields</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to drop duplicate schema fields in the schemaMetadata aspect. Useful if the source system has duplicate field paths in the db, but we're pushing to a system with server-side duplicate checking. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_all_versions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, include all versions of each aspect. Otherwise, only include the latest version of each aspect.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_soft_deleted_entities</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, include entities that have been soft deleted. Otherwise, include all entities regardless of removal status.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">kafka_topic_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of kafka topic containing timeseries MCLs <div className="default-line default-line-with-docs">Default: <span className="default-value">MetadataChangeLog&#95;Timeseries&#95;v1</span></div> |
| <div className="path-line"><span className="path-main">preserve_system_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Copy system metadata from the source system <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">query_timeout</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Timeout for each query in seconds.  <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">database_connection</span></div> <div className="type-name-line"><span className="type-name">One of SQLAlchemyConnectionConfig, null</span></div> | Database connection config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">database_connection.</span><span className="path-main">host_port</span>&nbsp;<abbr title="Required if database_connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | host URL  |
| <div className="path-line"><span className="path-prefix">database_connection.</span><span className="path-main">scheme</span>&nbsp;<abbr title="Required if database_connection is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | scheme  |
| <div className="path-line"><span className="path-prefix">database_connection.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | database (catalog) <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">database_connection.</span><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs. To set connection arguments in the URL, specify them under `connect_args`.  |
| <div className="path-line"><span className="path-prefix">database_connection.</span><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | password <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">database_connection.</span><span className="path-main">sqlalchemy_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">database_connection.</span><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | username <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">exclude_aspects</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Set of aspect names to exclude from ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;dataHubSecretKey&#x27;, &#x27;datahubIngestionRunSummary&#x27;,...</span></div> |
| <div className="path-line"><span className="path-prefix">exclude_aspects.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">kafka_connection</span></div> <div className="type-name-line"><span className="type-name">One of KafkaConsumerConnectionConfig, null</span></div> | Kafka connection config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">kafka_connection.</span><span className="path-main">bootstrap</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |  <div className="default-line ">Default: <span className="default-value">localhost:9092</span></div> |
| <div className="path-line"><span className="path-prefix">kafka_connection.</span><span className="path-main">client_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The request timeout used when interacting with the Kafka APIs. <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-prefix">kafka_connection.</span><span className="path-main">consumer_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Extra consumer config serialized as JSON. These options will be passed into Kafka's DeserializingConsumer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#deserializingconsumer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md .  |
| <div className="path-line"><span className="path-prefix">kafka_connection.</span><span className="path-main">schema_registry_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Extra schema registry config serialized as JSON. These options will be passed into Kafka's SchemaRegistryClient. https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?#schemaregistryclient  |
| <div className="path-line"><span className="path-prefix">kafka_connection.</span><span className="path-main">schema_registry_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Schema registry URL. Can be overridden with KAFKA_SCHEMAREGISTRY_URL environment variable, or will use DATAHUB_GMS_BASE_PATH if not set.  |
| <div className="path-line"><span className="path-main">urn_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">urn_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulIngestionConfig</span></div> | Basic Stateful Ingestion Specific Configuration for any source.  |
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
    "KafkaConsumerConnectionConfig": {
      "additionalProperties": false,
      "description": "Configuration class for holding connectivity information for Kafka consumers",
      "properties": {
        "bootstrap": {
          "default": "localhost:9092",
          "title": "Bootstrap",
          "type": "string"
        },
        "schema_registry_url": {
          "description": "Schema registry URL. Can be overridden with KAFKA_SCHEMAREGISTRY_URL environment variable, or will use DATAHUB_GMS_BASE_PATH if not set.",
          "title": "Schema Registry Url",
          "type": "string"
        },
        "schema_registry_config": {
          "additionalProperties": true,
          "description": "Extra schema registry config serialized as JSON. These options will be passed into Kafka's SchemaRegistryClient. https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?#schemaregistryclient",
          "title": "Schema Registry Config",
          "type": "object"
        },
        "client_timeout_seconds": {
          "default": 60,
          "description": "The request timeout used when interacting with the Kafka APIs.",
          "title": "Client Timeout Seconds",
          "type": "integer"
        },
        "consumer_config": {
          "additionalProperties": true,
          "description": "Extra consumer config serialized as JSON. These options will be passed into Kafka's DeserializingConsumer. See https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#deserializingconsumer and https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md .",
          "title": "Consumer Config",
          "type": "object"
        }
      },
      "title": "KafkaConsumerConnectionConfig",
      "type": "object"
    },
    "SQLAlchemyConnectionConfig": {
      "additionalProperties": false,
      "properties": {
        "username": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "username",
          "title": "Username"
        },
        "password": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "password",
          "title": "Password"
        },
        "host_port": {
          "description": "host URL",
          "title": "Host Port",
          "type": "string"
        },
        "database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "database (catalog)",
          "title": "Database"
        },
        "scheme": {
          "description": "scheme",
          "title": "Scheme",
          "type": "string"
        },
        "sqlalchemy_uri": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.",
          "title": "Sqlalchemy Uri"
        },
        "options": {
          "additionalProperties": true,
          "description": "Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs. To set connection arguments in the URL, specify them under `connect_args`.",
          "title": "Options",
          "type": "object"
        }
      },
      "required": [
        "host_port",
        "scheme"
      ],
      "title": "SQLAlchemyConnectionConfig",
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
      "$ref": "#/$defs/StatefulIngestionConfig",
      "default": {
        "enabled": true,
        "max_checkpoint_state_size": 16777216,
        "state_provider": {
          "config": {},
          "type": "datahub"
        },
        "ignore_old_state": false,
        "ignore_new_state": false
      },
      "description": "Stateful Ingestion Config"
    },
    "database_connection": {
      "anyOf": [
        {
          "$ref": "#/$defs/SQLAlchemyConnectionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Database connection config"
    },
    "kafka_connection": {
      "anyOf": [
        {
          "$ref": "#/$defs/KafkaConsumerConnectionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Kafka connection config"
    },
    "include_all_versions": {
      "default": false,
      "description": "If enabled, include all versions of each aspect. Otherwise, only include the latest version of each aspect. ",
      "title": "Include All Versions",
      "type": "boolean"
    },
    "include_soft_deleted_entities": {
      "default": true,
      "description": "If enabled, include entities that have been soft deleted. Otherwise, include all entities regardless of removal status. ",
      "title": "Include Soft Deleted Entities",
      "type": "boolean"
    },
    "exclude_aspects": {
      "default": [
        "dataHubSecretKey",
        "datahubIngestionRunSummary",
        "datahubIngestionCheckpoint",
        "testResults",
        "dataHubIngestionSourceInfo",
        "globalSettingsInfo",
        "dataHubExecutionRequestInput",
        "dataHubExecutionRequestResult",
        "dataHubSecretValue",
        "globalSettingsKey",
        "dataHubExecutionRequestSignal",
        "dataHubIngestionSourceKey",
        "dataHubExecutionRequestKey"
      ],
      "description": "Set of aspect names to exclude from ingestion",
      "items": {
        "type": "string"
      },
      "title": "Exclude Aspects",
      "type": "array",
      "uniqueItems": true
    },
    "database_query_batch_size": {
      "default": 10000,
      "description": "Number of records to fetch from the database at a time",
      "title": "Database Query Batch Size",
      "type": "integer"
    },
    "database_table_name": {
      "default": "metadata_aspect_v2",
      "description": "Name of database table containing all versioned aspects",
      "title": "Database Table Name",
      "type": "string"
    },
    "kafka_topic_name": {
      "default": "MetadataChangeLog_Timeseries_v1",
      "description": "Name of kafka topic containing timeseries MCLs",
      "title": "Kafka Topic Name",
      "type": "string"
    },
    "commit_state_interval": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 1000,
      "description": "Number of records to process before committing state",
      "title": "Commit State Interval"
    },
    "commit_with_parse_errors": {
      "default": false,
      "description": "Whether to update createdon timestamp and kafka offset despite parse errors. Enable if you want to ignore the errors.",
      "title": "Commit With Parse Errors",
      "type": "boolean"
    },
    "urn_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      }
    },
    "drop_duplicate_schema_fields": {
      "default": false,
      "description": "Whether to drop duplicate schema fields in the schemaMetadata aspect. Useful if the source system has duplicate field paths in the db, but we're pushing to a system with server-side duplicate checking.",
      "title": "Drop Duplicate Schema Fields",
      "type": "boolean"
    },
    "query_timeout": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Timeout for each query in seconds. ",
      "title": "Query Timeout"
    },
    "preserve_system_metadata": {
      "default": true,
      "description": "Copy system metadata from the source system",
      "title": "Preserve System Metadata",
      "type": "boolean"
    }
  },
  "title": "DataHubSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.datahub.datahub_source.DataHubSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/datahub/datahub_source.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for DataHub, feel free to ping us on [our Slack](https://datahub.com/slack).
