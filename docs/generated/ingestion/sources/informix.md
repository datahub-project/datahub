


# Informix

## Overview

IBM Informix is a relational database management system used for transactional and analytical workloads. Learn more in the [official Informix documentation](https://www.ibm.com/docs/en/informix-servers).

The DataHub integration for Informix covers tables, views, schema fields, and containers (database/schema hierarchy). It also extracts foreign-key relationships, table- and column-level lineage for views, ownership, and approximate row counts, and supports stateful deletion detection. It connects via JDBC, so provisioning the IBM Informix JDBC driver is required before ingestion can run.

## Concept Mapping

| Source Concept | DataHub Concept         | Notes                                                            |
| -------------- | ----------------------- | ---------------------------------------------------------------- |
| Database       | Container               | Top-level container.                                             |
| Owner (schema) | Container               | Nested under the database container.                             |
| Table / View   | Dataset                 | `tabtype` `'T'`/`'V'` in `systables`.                            |
| Column         | SchemaField             | Mapped from `syscolumns.coltype`.                                |
| `owner`        | Ownership (`DATAOWNER`) | The creating database user, applied to schemas and datasets.     |
| Foreign key    | ForeignKeyConstraint    | From `sysconstraints` / `sysreferences`; tables only.            |
| View text      | Upstream lineage        | `sysviews.viewtext`, parsed for table- and column-level lineage. |
| `nrows`        | Dataset profile         | Approximate row count from `systables`, not a row scan.          |


## Module `informix`
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| Column-level Lineage | ✅ | Column-level view lineage. Supported for types - View. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Row counts only, via systables.nrows. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| Extract Ownership | ✅ | Schema/table/view owner from `systables.owner`, via the `include_ownership` config field. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | View lineage. Supported for types - View. |

### Overview

The `informix` module ingests metadata from IBM Informix into DataHub by querying the Informix system catalog over JDBC. Module-specific capabilities are documented below.

### Prerequisites

#### JDBC Driver

This source connects through the IBM Informix JDBC driver, which is not bundled with `acryl-datahub` because it is proprietary. Provide it in one of two ways:

1. **Bring your own jars** (recommended for air-gapped or license-restricted environments): set `driver_jar_paths` to explicit paths for the `com.ibm.informix:jdbc` jar and the `org.mongodb:bson` jar it depends on. No download is attempted when this is set.
2. **Auto-download**: set `accept_ibm_jdbc_license: true` to have the source download and checksum-verify the driver from Maven Central at runtime, caching it under `~/.datahub/jars/informix` (override with `driver_cache_dir`). Pin exact versions with `jdbc_driver_version` / `bson_version` if needed.

:::caution

Auto-download requires accepting the [IBM Informix JDBC Driver Software License Agreement](http://www-03.ibm.com/software/sla/sladb.nsf/doclookup/CA4476C0AF8346EC852579290012D218?OpenDocument). Setting `accept_ibm_jdbc_license: true` is your confirmation that you accept these terms; DataHub does not redistribute the driver.

:::

#### Permissions

The connecting user needs `SELECT` on the Informix system catalog to enumerate metadata:

- `systables`, `syscolumns` — tables, views, and columns.
- `sysconstraints`, `sysindexes`, `sysreferences` — primary- and foreign-key detection.
- `sysviews` — view definitions (used for view lineage).

No access to user table data is required; row counts are approximate values read from the system catalog (`systables.nrows`), not row scans or sampling.


### Install the Plugin
```shell
pip install 'acryl-datahub[informix]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: informix
  config:
    host_port: "localhost:9088"
    server: "informix"
    database: "mydb"
    username: "informix"
    password: "${INFORMIX_PASSWORD}"
    accept_ibm_jdbc_license: true

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
| <div className="path-line"><span className="path-main">database</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Informix database to ingest from.  |
| <div className="path-line"><span className="path-main">server</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Informix server name (INFORMIXSERVER).  |
| <div className="path-line"><span className="path-main">accept_ibm_jdbc_license</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Set true to allow auto-downloading the proprietary IBM Informix JDBC driver from Maven Central under the IBM Informix JDBC Software License Agreement. Ignored when driver_jar_paths is set. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">bson_version</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pinned org.mongodb:bson version to download. <div className="default-line default-line-with-docs">Default: <span className="default-value">4.11.1</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">driver_cache_dir</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Directory to cache downloaded jars. Defaults to ~/.datahub/jars/informix. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">extra_props</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Extra JDBC properties appended to the connection URL, e.g. 'DB_LOCALE=en_US.utf8;CLIENT_LOCALE=en_US.utf8'. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">host_port</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Informix host and port. <div className="default-line default-line-with-docs">Default: <span className="default-value">localhost:9088</span></div> |
| <div className="path-line"><span className="path-main">include_foreign_keys</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract foreign-key relationships from sysconstraints/sysreferences. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit ownership for schemas, tables and views from systables.owner. Informix records the owning database user rather than a person, so this produces a corpuser URN for that account (e.g. 'informix'), not an individual's identity. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_row_counts</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit approximate row counts from systables.nrows. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether tables should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract table- and column-level lineage for views by parsing their SQL definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether views should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">jdbc_driver_version</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pinned com.ibm.informix:jdbc version to download. <div className="default-line default-line-with-docs">Default: <span className="default-value">4.50.10</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Login password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Login user. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">driver_jar_paths</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Explicit paths to the Informix JDBC jar and org.mongodb bson jar. If set, no download is attempted (use for air-gapped environments). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">driver_jar_paths.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion / stale-entity removal config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
  "description": "Configuration for the Informix metadata ingestion source.",
  "properties": {
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase. This value is part of each dataset's URN identity, so it must stay fixed for the life of a deployment. Changing it after data has been ingested re-keys every dataset (e.g. `MyDb.MyTable` becomes `mydb.mytable`); with stateful ingestion enabled the old-cased URNs are then soft-deleted as stale while the new-cased ones are created, producing duplicate or orphaned entities. Pick one value before the first run and leave it unchanged.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
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
      "description": "Stateful ingestion / stale-entity removal config."
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
    "host_port": {
      "default": "localhost:9088",
      "description": "Informix host and port.",
      "title": "Host Port",
      "type": "string"
    },
    "server": {
      "description": "Informix server name (INFORMIXSERVER).",
      "title": "Server",
      "type": "string"
    },
    "database": {
      "description": "Informix database to ingest from.",
      "title": "Database",
      "type": "string"
    },
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
      "description": "Login user.",
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
      "description": "Login password.",
      "title": "Password"
    },
    "extra_props": {
      "default": "",
      "description": "Extra JDBC properties appended to the connection URL, e.g. 'DB_LOCALE=en_US.utf8;CLIENT_LOCALE=en_US.utf8'.",
      "title": "Extra Props",
      "type": "string"
    },
    "driver_jar_paths": {
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
      "default": null,
      "description": "Explicit paths to the Informix JDBC jar and org.mongodb bson jar. If set, no download is attempted (use for air-gapped environments).",
      "title": "Driver Jar Paths"
    },
    "accept_ibm_jdbc_license": {
      "default": false,
      "description": "Set true to allow auto-downloading the proprietary IBM Informix JDBC driver from Maven Central under the IBM Informix JDBC Software License Agreement. Ignored when driver_jar_paths is set.",
      "title": "Accept Ibm Jdbc License",
      "type": "boolean"
    },
    "jdbc_driver_version": {
      "default": "4.50.10",
      "description": "Pinned com.ibm.informix:jdbc version to download.",
      "title": "Jdbc Driver Version",
      "type": "string"
    },
    "bson_version": {
      "default": "4.11.1",
      "description": "Pinned org.mongodb:bson version to download.",
      "title": "Bson Version",
      "type": "string"
    },
    "driver_cache_dir": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Directory to cache downloaded jars. Defaults to ~/.datahub/jars/informix.",
      "title": "Driver Cache Dir"
    },
    "schema_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for owners/schemas to filter in ingestion. Specify regex to only match the owner (schema) name."
    },
    "table_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.owner.table format."
    },
    "view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for views to filter in ingestion. Note: defaults to table_pattern if not specified. Specify regex to match the entire view name in database.owner.view format."
    },
    "include_tables": {
      "default": true,
      "description": "Whether tables should be ingested.",
      "title": "Include Tables",
      "type": "boolean"
    },
    "include_views": {
      "default": true,
      "description": "Whether views should be ingested.",
      "title": "Include Views",
      "type": "boolean"
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "description": "Attach domains to databases, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like \"Marketing\". If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.",
      "title": "Domain",
      "type": "object"
    },
    "include_row_counts": {
      "default": true,
      "description": "Emit approximate row counts from systables.nrows.",
      "title": "Include Row Counts",
      "type": "boolean"
    },
    "include_foreign_keys": {
      "default": true,
      "description": "Extract foreign-key relationships from sysconstraints/sysreferences.",
      "title": "Include Foreign Keys",
      "type": "boolean"
    },
    "include_view_lineage": {
      "default": true,
      "description": "Extract table- and column-level lineage for views by parsing their SQL definitions.",
      "title": "Include View Lineage",
      "type": "boolean"
    },
    "include_ownership": {
      "default": true,
      "description": "Emit ownership for schemas, tables and views from systables.owner. Informix records the owning database user rather than a person, so this produces a corpuser URN for that account (e.g. 'informix'), not an individual's identity.",
      "title": "Include Ownership",
      "type": "boolean"
    }
  },
  "required": [
    "server",
    "database"
  ],
  "title": "InformixSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required. This module:

- Emits a database → owner (schema) container hierarchy, with tables and views as datasets underneath.
- Emits schema fields with native types (including length, e.g. `VARCHAR(100)`), nullability, and primary-key flags.
- Extracts foreign-key relationships from `sysconstraints` / `sysreferences`, for tables (`include_foreign_keys`).
- Extracts table- and column-level lineage for views by parsing `sysviews.viewtext` (`include_view_lineage`).
- Emits `viewProperties` (the stored view SQL) for every view whose definition is readable.
- Emits approximate row counts from `systables.nrows`, for tables (`include_row_counts`).
- Assigns ownership from `systables.owner` (`include_ownership`).
- Supports stateful ingestion with stale-entity (deletion) detection.

#### Ownership

Ownership is taken from `systables.owner`, which Informix populates with the database
user that created the object. Each schema, table and view is assigned that user as a
`DATAOWNER`.

Two things to be aware of before relying on this:

- Informix records a **database account**, not a person or team. Objects created by an
  administrative account all come back owned by that account (commonly `informix`), so
  the resulting owner is an identity in the DataHub sense but not necessarily a useful
  point of contact.
- The owner name is also the schema name — in Informix the two are the same concept —
  so a schema container is owned by the user it is named after.

Set `include_ownership: false` to skip emitting it.

### Limitations

This module does not support:

- **Column profiling** — no row sampling, null counts, or other column-level statistics; only approximate row counts from `systables.nrows` are emitted.
- **Stored procedures** — SPL routines are not ingested as DataJobs.
- **Usage / query-log lineage** — view lineage is derived only from parsing view SQL definitions, not from query logs or runtime usage.

#### View lineage and Informix-specific SQL

View lineage is produced by parsing `sysviews.viewtext` with sqlglot. sqlglot has no
Informix dialect, so the `postgres` dialect is used: Informix normalizes stored view
text into a qualified, aliased, comma-join form that `postgres` parses correctly for
the common case.

Views whose stored text retains Informix-specific syntax — `MATCHES` / `NOT MATCHES`,
`FIRST` / `SKIP`, native `OUTER` joins, or `DATETIME ... YEAR TO DAY` — will not parse
and get no lineage. This is per-view and non-fatal: the rest of the run is unaffected,
and each failure is counted as `view_lineage_failures` in the ingestion report.

A view can also resolve at the table level while its column lineage fails to parse. In
that case table-level lineage is still emitted, the shortfall is reported as a warning,
and it is counted as `view_column_lineage_failures`.

#### Composite foreign keys

Informix's catalog exposes a constraint's child and parent key columns as two
independent 16-slot `sysindexes` column lists, so a composite foreign key comes back as
a cross product rather than as ordered column pairs. Single-column foreign keys are
always exact. For composite keys the columns are paired best-effort and a warning is
reported, since the catalog does not record the pairing order.

If the two lists come back with different lengths — which happens when a constraint is
backed by a wider pre-existing index — the pairing is ambiguous, so that constraint is
skipped rather than emitted misaligned. Skipped constraints are counted as
`foreign_keys_dropped_mismatched` in the ingestion report.

#### Extended type mapping

`syscolumns.coltype` cannot identify an extended type on its own — `LVARCHAR` is type 40, and
`BOOLEAN`, `BLOB` and `CLOB` all share type 41 — so the column's `extended_id` is resolved
against `sysxtdtypes` to recover the real type name.

`LVARCHAR`, `BOOLEAN`, `BLOB` and `CLOB` map to their DataHub equivalents. A `DISTINCT` type
reports its own name (for example `MONEY_USD`) and takes its DataHub type from the built-in it
was defined over — read from `coltype` for an ordinary built-in, and from `sysxtdtypes.source`
when it was defined over `LVARCHAR`, `BOOLEAN`, `BLOB` or `CLOB`, which `coltype` cannot express.
A `DISTINCT` type defined over another `DISTINCT` type maps to a null type. A named `ROW` type
maps to a record.

User-defined opaque types (`JSON`, `BSON`, time series, and spatial types such as
`ST_Geometry`) have no DataHub equivalent and still map to a null type, but the native type name
is reported rather than a placeholder. Each one is counted as a warning in the ingestion report.

### Troubleshooting

If ingestion fails, first confirm the JDBC driver is resolvable (see Prerequisites) and that the connecting user has `SELECT` on the system catalog tables. Then review ingestion logs for connection or query errors.


### Code Coordinates
- Class Name: `datahub.ingestion.source.informix.source.InformixSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/informix/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Informix, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
