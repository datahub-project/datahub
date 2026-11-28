


# Hightouch

## Overview

Hightouch is a Reverse ETL platform that syncs data from your data warehouse to business tools such as CRMs, marketing automation platforms, and analytics applications. Learn more in the [official Hightouch documentation](https://hightouch.com/docs).

The DataHub integration for Hightouch covers sources, models, syncs, destinations, and sync run execution history. It captures coarse-grained and column-level lineage from sources through models to destinations, and supports stateful deletion detection.

## Concept Mapping

| Hightouch Object | DataHub Entity                                                           | Description                                                   |
| ---------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------- |
| `Source`         | [Dataset](../../metamodel/entities/dataset.md)                           | Source database/warehouse (referenced as input)               |
| `Model`          | [Dataset](../../metamodel/entities/dataset.md)                           | SQL query or transformation (optional, platform: "hightouch") |
| `Sync`           | [Data Job](../../metamodel/entities/dataJob.md)                          | Data pipeline that moves data from model to destination       |
| `Destination`    | [Dataset](../../metamodel/entities/dataset.md)                           | Target system (referenced as output)                          |
| `Sync Run`       | [Data Process Instance](../../metamodel/entities/dataProcessInstance.md) | Execution instance with statistics                            |
| `Workspace`      | Platform Instance                                                        | Hightouch workspace (optional grouping)                       |


## Module `hightouch`
![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default, one per workspace. |
| Column-level Lineage | ✅ | Enabled by default, from parsed model SQL and sync field mappings. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default via `extract_owners`. |
| Extract Tags | ✅ | Enabled by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default for models. |
| Table-Level Lineage | ✅ | Enabled by default, from warehouse tables through models and syncs to destinations. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `hightouch` module ingests metadata from Hightouch into DataHub. It is intended for
production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

#### 1. Generate a Hightouch API Key

1. Log in to your Hightouch account
2. Navigate to **Settings** → **API Keys**
3. Click **Create API Key**
4. Give your key a descriptive name (e.g., "DataHub Integration")
5. Select the appropriate permissions:
   - **Read access** to: Sources, Models, Syncs, Destinations, Sync Runs
6. Copy the generated API key (you won't be able to see it again)
7. Store the API key securely — you'll use it in the DataHub recipe configuration

#### 2. Required Permissions

The API key needs read access to:

- Sources
- Models
- Syncs
- Destinations
- Sync Runs (for execution history)


### Install the Plugin
```shell
pip install 'acryl-datahub[hightouch]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: hightouch
  config:
    api_config:
      api_key: "${HIGHTOUCH_API_KEY}"

    env: "PROD"
    platform_instance: "hightouch-prod"

    emit_models_as_datasets: true

    include_sync_runs: true
    max_sync_runs_per_sync: 10

    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_config</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">HightouchAPIConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">api_config.</span><span className="path-main">api_key</span>&nbsp;<abbr title="Required if api_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Hightouch API key (a workspace API key with read access) used as a Bearer token on every request.  |
| <div className="path-line"><span className="path-prefix">api_config.</span><span className="path-main">base_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Base URL of the Hightouch REST API. Override for EU or self-hosted deployments. A trailing slash is stripped automatically. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://api.hightouch.com/api/v1</span></div> |
| <div className="path-line"><span className="path-prefix">api_config.</span><span className="path-main">request_timeout_sec</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Per-request timeout in seconds for calls to the Hightouch API. Increase this if large workspaces time out on listing endpoints. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-main">emit_models_as_datasets</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit Hightouch models as datasets on the `hightouch` platform, with lineage from the warehouse tables they read. When disabled, syncs read directly from those warehouse tables. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_owners</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Set the user who created a model or sync as its technical owner, using their email address as the DataHub user id. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_contracts</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest Hightouch Event Contracts. Each event becomes a dataset with its JSON Schema, a DataContract holding a schema assertion, and lineage from the contract's event sources. Event Contracts are not available on every Hightouch plan. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_sibling_relationships</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Make each table model a sibling of the warehouse table it reads, with the Hightouch model as the primary sibling. The warehouse table only gets the back-link when it already exists in DataHub. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_sync_runs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest each sync's recent runs as DataProcessInstances. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_sync_runs_per_sync</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of runs to ingest per sync. Must be at least 1; set include_sync_runs: false to skip runs. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">parse_model_sql</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Parse raw SQL models to derive table and column lineage from the tables they query. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">contract_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">contract_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">contract_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">contract_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">contract_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">contract_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">destination_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform of the source or destination. Detected from the Hightouch type when unset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Lowercase table names, including database and schema, so they match the platform connector's URNs. Defaults to true for Snowflake, whose connector lowercases URNs by default, and false otherwise. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Database used to qualify table names. Defaults to the `database` in the Hightouch source configuration. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">include_schema_in_urn</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include the schema in dataset URNs. Turn off for platforms whose connector emits two-part `database.table` names. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance used by the connector that ingests this source or destination. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Environment of the source or destination datasets. When unset, the connector-level `env` is used. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">model_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">model_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">model_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">model_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">model_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">model_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">sources_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform of the source or destination. Detected from the Hightouch type when unset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Lowercase table names, including database and schema, so they match the platform connector's URNs. Defaults to true for Snowflake, whose connector lowercases URNs by default, and false otherwise. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Database used to qualify table names. Defaults to the `database` in the Hightouch source configuration. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">include_schema_in_urn</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include the schema in dataset URNs. Turn off for platforms whose connector emits two-part `database.table` names. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance used by the connector that ingests this source or destination. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">sources_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Environment of the source or destination datasets. When unset, the connector-level `env` is used. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">sync_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">sync_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">sync_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">sync_patterns.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">sync_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">sync_patterns.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Hightouch Stateful Ingestion Config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes.\n\nPatterns are matched against the start of the string only, not the entire\nstring - a pattern does not need to match to the end to be considered a match.\nFor example, the pattern \"prod\" matches \"prod\", \"prod_east\", and \"production\".\nTo require an exact match, anchor your pattern explicitly, e.g. \"^prod$\".",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
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
    "HightouchAPIConfig": {
      "additionalProperties": false,
      "properties": {
        "api_key": {
          "description": "Hightouch API key (a workspace API key with read access) used as a Bearer token on every request.",
          "format": "password",
          "title": "Api Key",
          "type": "string",
          "writeOnly": true
        },
        "base_url": {
          "default": "https://api.hightouch.com/api/v1",
          "description": "Base URL of the Hightouch REST API. Override for EU or self-hosted deployments. A trailing slash is stripped automatically.",
          "title": "Base Url",
          "type": "string"
        },
        "request_timeout_sec": {
          "default": 30,
          "description": "Per-request timeout in seconds for calls to the Hightouch API. Increase this if large workspaces time out on listing endpoints.",
          "title": "Request Timeout Sec",
          "type": "integer"
        }
      },
      "required": [
        "api_key"
      ],
      "title": "HightouchAPIConfig",
      "type": "object"
    },
    "PlatformDetail": {
      "additionalProperties": false,
      "properties": {
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
          "description": "DataHub platform of the source or destination. Detected from the Hightouch type when unset.",
          "title": "Platform"
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
          "description": "Platform instance used by the connector that ingests this source or destination.",
          "title": "Platform Instance"
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
          "description": "Environment of the source or destination datasets. When unset, the connector-level `env` is used.",
          "title": "Env"
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
          "description": "Database used to qualify table names. Defaults to the `database` in the Hightouch source configuration.",
          "title": "Database"
        },
        "include_schema_in_urn": {
          "default": true,
          "description": "Include the schema in dataset URNs. Turn off for platforms whose connector emits two-part `database.table` names.",
          "title": "Include Schema In Urn",
          "type": "boolean"
        },
        "convert_urns_to_lowercase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Lowercase table names, including database and schema, so they match the platform connector's URNs. Defaults to true for Snowflake, whose connector lowercases URNs by default, and false otherwise.",
          "title": "Convert Urns To Lowercase"
        }
      },
      "title": "PlatformDetail",
      "type": "object"
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
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Hightouch Stateful Ingestion Config."
    },
    "api_config": {
      "$ref": "#/$defs/HightouchAPIConfig",
      "description": "Hightouch API configuration"
    },
    "sync_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns matched against each sync's slug."
    },
    "model_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns matched against each model's display name."
    },
    "contract_patterns": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns matched against each event contract's name."
    },
    "emit_models_as_datasets": {
      "default": true,
      "description": "Emit Hightouch models as datasets on the `hightouch` platform, with lineage from the warehouse tables they read. When disabled, syncs read directly from those warehouse tables.",
      "title": "Emit Models As Datasets",
      "type": "boolean"
    },
    "include_sync_runs": {
      "default": true,
      "description": "Ingest each sync's recent runs as DataProcessInstances.",
      "title": "Include Sync Runs",
      "type": "boolean"
    },
    "max_sync_runs_per_sync": {
      "default": 10,
      "description": "Maximum number of runs to ingest per sync. Must be at least 1; set include_sync_runs: false to skip runs.",
      "title": "Max Sync Runs Per Sync",
      "type": "integer"
    },
    "parse_model_sql": {
      "default": true,
      "description": "Parse raw SQL models to derive table and column lineage from the tables they query.",
      "title": "Parse Model Sql",
      "type": "boolean"
    },
    "include_contracts": {
      "default": false,
      "description": "Ingest Hightouch Event Contracts. Each event becomes a dataset with its JSON Schema, a DataContract holding a schema assertion, and lineage from the contract's event sources. Event Contracts are not available on every Hightouch plan.",
      "title": "Include Contracts",
      "type": "boolean"
    },
    "extract_owners": {
      "default": true,
      "description": "Set the user who created a model or sync as its technical owner, using their email address as the DataHub user id.",
      "title": "Extract Owners",
      "type": "boolean"
    },
    "sources_to_platform_instance": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "default": {},
      "description": "Platform details keyed by Hightouch source id, so upstream dataset URNs match those of the source platform's connector.",
      "title": "Sources To Platform Instance",
      "type": "object"
    },
    "destination_to_platform_instance": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "default": {},
      "description": "Platform details keyed by Hightouch destination id, so destination dataset URNs match those of the destination's connector.",
      "title": "Destination To Platform Instance",
      "type": "object"
    },
    "include_sibling_relationships": {
      "default": true,
      "description": "Make each table model a sibling of the warehouse table it reads, with the Hightouch model as the primary sibling. The warehouse table only gets the back-link when it already exists in DataHub.",
      "title": "Include Sibling Relationships",
      "type": "boolean"
    }
  },
  "required": [
    "api_config"
  ],
  "title": "HightouchSourceConfig",
  "type": "object"
}
```





### Capabilities

This connector extracts the following metadata from Hightouch:

- **Workspaces** — One container per Hightouch workspace, holding its models and syncs
- **Sources** — Database and warehouse connections (as DataHub Dataset references)
- **Models** — SQL queries and transformations (optionally as DataHub Datasets)
- **Syncs** — One DataFlow per workspace, with a DataJob per sync from its model to its destination
- **Destinations** — Target systems like Salesforce, HubSpot, etc. (as DataHub Dataset references)
- **Sync Runs** — Execution history with detailed statistics (as DataHub DataProcessInstances)
- **Lineage** — Complete data flow from sources through models and syncs to destinations
- **Column-level Lineage** — Field mappings between source and destination systems
- **Ownership** — The user who created a model or sync, as its technical owner
- **Tags** — Model and sync tags, as `key:value` tags
- **Event Contracts** — Event schema definitions emitted as Datasets, each with a DataHub DataContract housing a schema assertion, plus lineage from the contract's Event Sources

#### Model Ingestion Options

You can configure how Hightouch models are represented in DataHub:

- **Option 1 (Recommended)**: Set `emit_models_as_datasets: true`

  - Models appear as separate Dataset entities in DataHub with platform "hightouch"
  - Lineage flows: Source Tables → Hightouch Models → Syncs → Destination Tables
  - Provides visibility into intermediate transformations

- **Option 2**: Set `emit_models_as_datasets: false`
  - Models are not created as separate entities
  - Lineage flows directly: Source Tables → Syncs → Destination Tables
  - Simpler lineage graph, but less visibility into transformations

#### Lineage

The connector extracts comprehensive lineage:

1. **Coarse-grained Lineage**: Table-to-table relationships showing data flow
2. **Fine-grained Lineage**: Column-to-column mappings extracted from sync field mappings
3. **Multi-hop Lineage**: Complete path from source databases through models to destinations

SQL parsing is supported for `raw_sql` models, automatically extracting upstream table
dependencies and creating column-level lineage. The model's schema is the query's output columns.
When the upstream tables are already in DataHub, their names are matched with the casing they
were ingested with. Column casing is normalized to match upstream
table schemas (e.g., Snowflake lowercasing). For best results:

- Ensure upstream source tables are already ingested into DataHub
- Configure `sources_to_platform_instance` for accurate URN generation
- Enable a DataHub graph connection for schema resolution

#### Sibling Relationships

For table models, which expose a warehouse table as is, you can establish sibling relationships
between the Hightouch model and that table. Raw SQL models get lineage to the tables they read but
no sibling, because a query over a table is a different asset from the table. dbt models also get
lineage but no sibling, because the dbt connector already makes its dbt node the table's primary
sibling:

```yaml
include_sibling_relationships: true # default: true
```

When enabled, the Hightouch model is marked as the **primary** sibling and the upstream source
table is linked as a **secondary** sibling. Sibling aspects are only emitted on the upstream
table if it already exists in DataHub, preventing the creation of "ghost" entities. Requires
`sources_to_platform_instance` configuration for proper URN matching.

#### Event Contracts

Hightouch [Event Contracts](https://hightouch.com/docs/events/contracts) define the schemas used to
validate incoming events. Event Contracts are not available on every Hightouch plan, so they are
off by default. With `include_contracts: true`, the connector ingests them as:

- one **Dataset** per event, whose schema is the event's JSON Schema and whose custom properties
  capture the enforcement rules (`on_schema_violation`, `on_undeclared_fields`, `on_undeclared_schema`),
- a **DataContract** on each event dataset, housing a schema assertion derived from the JSON Schema,
- **lineage** from the contract's Event Sources (emitted as datasets) to each event dataset.

```yaml
include_contracts: true # default: false
contract_patterns:
  allow:
    - ".*"
```

The Hightouch API does not expose contract validation history, so the assertions are emitted as
definitions only (no pass/fail run results).

#### Platform Instance Configuration

If you have multiple instances of source or destination systems, configure platform instances to
generate correct lineage edges:

```yaml
sources_to_platform_instance:
  # Key is the Hightouch source ID (visible in Hightouch URL)
  "12345":
    platform: "snowflake"
    platform_instance: "prod-snowflake"
    env: "PROD"
    database: "analytics"
    # Lowercase upstream table names to match the source connector's URNs.
    # Defaults to true for Snowflake and false otherwise.
    convert_urns_to_lowercase: true

destination_to_platform_instance:
  # Key is the Hightouch destination ID
  "dest_123":
    platform: "salesforce"
    platform_instance: "prod-salesforce"
    env: "PROD"
```

To find source and destination IDs, navigate to the resource in the Hightouch UI and check the
URL (`https://app.hightouch.com/[workspace]/sources/[source_id]`), or query the Hightouch API:

```bash
curl -H "Authorization: Bearer YOUR_API_KEY" https://api.hightouch.com/api/v1/sources
```

#### Filtering

Control which syncs and models are ingested using regex patterns. `sync_patterns` match each
sync's slug, and `model_patterns` match each model's display name:

```yaml
sync_patterns:
  allow:
    - "prod-.*"
  deny:
    - "test-.*"
    - ".*-staging"

model_patterns:
  allow:
    - "customer-.*"
  deny:
    - ".*-draft"
```

#### Sync Run History

Limit the number of historical sync runs ingested:

```yaml
include_sync_runs: true
max_sync_runs_per_sync: 10 # last 10 runs per sync
```

Set `include_sync_runs: false` to skip sync run history entirely.
A run that has not finished (queued, querying, processing or reporting) is recorded with its
start only.

#### Supported Source and Destination Types

This connector supports 30+ data source types and 50+ destination types. Platform detection
works automatically for major sources including Snowflake, BigQuery, Redshift, Databricks,
PostgreSQL, MySQL, SQL Server, and common SaaS destinations. For sources or destinations not
automatically recognized, configure the mapping manually using `sources_to_platform_instance`
and `destination_to_platform_instance`. For the complete list of supported integrations, refer
to the [Hightouch documentation](https://hightouch.com/docs/destinations/overview/).

### Limitations

- **Ownership**: The creator of a model or sync becomes its technical owner, with their email
  address as the DataHub user id. Set `extract_owners: false` if your DataHub users are keyed
  differently.
- **Destination lineage**: Lineage to a destination is carried by the sync's DataJob. Nothing is
  written onto destination datasets, so their own connector's lineage is left as it is. A sync
  whose configuration names no destination object the connector recognises has no outlet, and is
  listed under `unresolved_destinations` in the report.
- **Model schemas**: The API returns no column list for a model. A table or dbt model takes its
  schema from the table it reads, when that table is already in DataHub, and a raw SQL model
  from its query's output columns. Otherwise the schema is the union of the columns its syncs
  reference, without types.
- **API Rate Limits**: The Hightouch API has rate limits. The connector implements retry logic
  with exponential backoff, but very large workspaces with thousands of syncs may need to be
  ingested in batches.
- **Platform Detection**: Automatic platform mapping works for major databases and SaaS tools.
  Custom or less common source/destination types may need manual platform mapping via
  `sources_to_platform_instance` or `destination_to_platform_instance`.
- **Complex SQL Queries**: SQL parsing works best for simple queries. Complex multi-table JOINs,
  CTEs, and subqueries may not produce complete column-level lineage.

### Troubleshooting

If ingestion fails, validate your API key, permissions, and connectivity first. Then review
ingestion logs for source-specific errors and adjust configuration accordingly.

#### Authentication Errors

If you see `401 Unauthorized`, verify your API key is correct and has not expired. Generate a
new key from **Settings** → **API Keys** in the Hightouch UI if needed.

#### Missing Lineage

If lineage is not appearing correctly:

1. Verify source and destination IDs are configured in `sources_to_platform_instance` /
   `destination_to_platform_instance`
2. Check that `emit_models_as_datasets` is set to your preferred mode
3. Ensure source and destination datasets already exist in DataHub (or ingest them separately)

#### Failed Runs and Stale Metadata

A sync or model that fails to process, Event Contracts that cannot be read (other than an API
key without access to them), an entity the API returns in an unexpected shape, or a list cut off
at the pagination limit fails the run. That keeps stale-metadata removal from soft-deleting
entities that still exist in Hightouch but were missing from the run. A single bad entity
therefore marks the whole run as failed on purpose, even though everything else was ingested;
the report names the entity.

A sync whose model could not be emitted reads directly from the model's warehouse tables instead,
so its lineage never points at a model that does not exist.

#### Rate Limiting

If you see `429 Too Many Requests`, the connector will automatically retry with backoff. For
large workspaces, consider using `sync_patterns` / `model_patterns` to reduce scope, or reducing
`max_sync_runs_per_sync` to limit API calls.


### Code Coordinates
- Class Name: `datahub.ingestion.source.hightouch.hightouch.HightouchSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/hightouch/hightouch.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Hightouch, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
