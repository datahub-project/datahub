


# Aerospike

## Overview

Aerospike is a high-performance, distributed NoSQL key-value database designed for real-time applications. Learn more in the [official Aerospike documentation](https://aerospike.com/docs/).

The DataHub integration for Aerospike extracts namespaces as containers and sets as datasets, including schema inference from sampled records. It also supports platform instance mapping, stateful ingestion for stale entity removal, and optional XDR (Cross-Datacenter Replication) metadata.

## Concept Mapping

| Source Concept | DataHub Concept   | Notes                                             |
| -------------- | ----------------- | ------------------------------------------------- |
| Namespace      | Container         | Top-level grouping of sets within a cluster.      |
| Set            | Dataset           | Primary ingested entity, analogous to a table.    |
| Record bins    | SchemaField       | Inferred from sampled records via schema probing. |
| Cluster        | Platform Instance | Optional; use when ingesting multiple clusters.   |


## Module `aerospike`
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |

### Overview

The `aerospike` module ingests metadata from Aerospike clusters into DataHub. It connects to an Aerospike node, discovers namespaces and sets, and infers schema by sampling records.

### Prerequisites

Before running ingestion, ensure you have:

1. **Network connectivity** to at least one Aerospike node on its service port (default 3000).
2. **Authentication credentials** (username/password) if the cluster has security enabled.
3. **Read permissions** on the namespaces and sets you want to ingest.

#### Authentication Modes

Aerospike supports three authentication modes:

- `AUTH_INTERNAL` (default) — standard username/password authentication.
- `AUTH_EXTERNAL` — external authentication (e.g., LDAP) over TLS.
- `AUTH_EXTERNAL_INSECURE` — external authentication without TLS.

#### Schema Inference

Schema is inferred by sampling records from each set. You can control:

- `infer_schema_depth` — how many nesting levels to traverse (default `1`, use `-1` for all levels, `0` to skip).
- `schema_sampling_size` — number of records to sample per set (default `1000`, use `null` for full scan).
- `max_schema_size` — cap on schema fields emitted (default `300`).


### Install the Plugin
```shell
pip install 'acryl-datahub[aerospike]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: aerospike
  config:
    # Coordinates
    hosts:
      - - host1
        - 3000
      - - host2
        - 3000

    # Credentials
    username: user
    password: "${AEROSPIKE_PASSWORD}"
    # auth_mode: AUTH_INTERNAL  # AUTH_INTERNAL (default), AUTH_EXTERNAL, AUTH_EXTERNAL_INSECURE

    # TLS (optional)
    # tls_enabled: false
    # tls_cafile: /path/to/ca.crt
    # tls_capath: /path/to/ca/dir

    # Platform instance (optional) — use when ingesting multiple Aerospike clusters
    # platform_instance: prod-cluster

    # Namespace and set filtering (regex allow/deny patterns)
    # namespace_pattern:
    #   allow:
    #     - "^prod.*"
    #   deny:
    #     - "^test.*"
    # set_pattern:
    #   allow:
    #     - "^events.*"

    # Schema inference
    # infer_schema_depth: 1     # Depth of nested fields to infer. -1 = all levels, 0 = skip inference.
    # schema_sampling_size: 1000  # Number of records to sample per set. null = scan entire set.
    # max_schema_size: 300        # Maximum number of fields to include in the schema.
    # ignore_empty_sets: false  # Skip sets with zero records.

    # Performance
    # records_per_second: 0   # Rate limit for Aerospike queries. 0 = no limit.
    # schema_query_timeout_ms: null  # Socket timeout in ms for schema inference queries. null = client default.
    # login_timeout_ms: null  # Login timeout in ms. null = Aerospike client default.

    # XDR (Cross-Datacenter Replication) metadata
    # include_xdr: false  # Annotate sets with the DCs they are replicated to.

    # Stateful ingestion — removes stale entities from DataHub when sets are dropped
    # stateful_ingestion:
    #   enabled: true


sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">auth_mode</span></div> <div className="type-name-line"><span className="type-name">One of Enum, null</span></div> | The authentication mode with the server. <div className="default-line default-line-with-docs">Default: <span className="default-value">0</span></div> |
| <div className="path-line"><span className="path-main">ignore_empty_sets</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ignore empty sets in the schema inference. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_xdr</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include XDR information in the dataset properties. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">inferSchemaDepth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | The depth of nested fields to infer schema. If set to `-1`, infer schema at all levels. If set to `0`, does not infer the schema. Default is `1`. <div className="default-line default-line-with-docs">Default: <span className="default-value">1</span></div> |
| <div className="path-line"><span className="path-main">login_timeout_ms</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Login timeout in milliseconds. Default None, using the default value of the Aerospike Python client. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">maxSchemaSize</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Maximum number of fields to include in the schema. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Aerospike password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">records_per_second</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of records per second for Aerospike query. Default is 0, which means no limit. <div className="default-line default-line-with-docs">Default: <span className="default-value">0</span></div> |
| <div className="path-line"><span className="path-main">schemaSamplingSize</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number of documents to use when inferring schema. If set to `null`, all documents will be scanned. <div className="default-line default-line-with-docs">Default: <span className="default-value">1000</span></div> |
| <div className="path-line"><span className="path-main">schema_query_timeout_ms</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Socket timeout in milliseconds for schema inference queries. Default None uses the Aerospike client default. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tls_cafile</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to the CA certificate file. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tls_capath</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to the CA certificate directory. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">tls_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to use TLS for the connection. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Aerospike username. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">hosts</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Aerospike hosts list. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#91;&#x27;localhost&#x27;, 3000&#93;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">hosts.</span><span className="path-main">array</span></div> <div className="type-name-line"><span className="type-name">array</span></div> |   |
| <div className="path-line"><span className="path-prefix">hosts.array.</span><span className="path-main">object</span></div> <div className="type-name-line"><span className="type-name">object</span></div> |   |
| <div className="path-line"><span className="path-main">namespace_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">namespace_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">set_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">set_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
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
    "AuthMode": {
      "enum": [
        1,
        2,
        0
      ],
      "title": "AuthMode",
      "type": "integer"
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
    "hosts": {
      "default": [
        [
          "localhost",
          3000
        ]
      ],
      "description": "Aerospike hosts list.",
      "items": {
        "items": {},
        "type": "array"
      },
      "title": "Hosts",
      "type": "array"
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
      "description": "Aerospike username.",
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
      "description": "Aerospike password.",
      "title": "Password"
    },
    "auth_mode": {
      "anyOf": [
        {
          "$ref": "#/$defs/AuthMode"
        },
        {
          "type": "null"
        }
      ],
      "default": 0,
      "description": "The authentication mode with the server."
    },
    "tls_enabled": {
      "default": false,
      "description": "Whether to use TLS for the connection.",
      "title": "Tls Enabled",
      "type": "boolean"
    },
    "tls_capath": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to the CA certificate directory.",
      "title": "Tls Capath"
    },
    "tls_cafile": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to the CA certificate file.",
      "title": "Tls Cafile"
    },
    "inferSchemaDepth": {
      "default": 1,
      "description": "The depth of nested fields to infer schema. If set to `-1`, infer schema at all levels. If set to `0`, does not infer the schema. Default is `1`.",
      "title": "Inferschemadepth",
      "type": "integer"
    },
    "schemaSamplingSize": {
      "anyOf": [
        {
          "exclusiveMinimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 1000,
      "description": "Number of documents to use when inferring schema. If set to `null`, all documents will be scanned.",
      "title": "Schemasamplingsize"
    },
    "maxSchemaSize": {
      "anyOf": [
        {
          "exclusiveMinimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 300,
      "description": "Maximum number of fields to include in the schema.",
      "title": "Maxschemasize"
    },
    "namespace_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for namespaces to filter in ingestion."
    },
    "set_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for sets to filter in ingestion."
    },
    "ignore_empty_sets": {
      "default": false,
      "description": "Ignore empty sets in the schema inference.",
      "title": "Ignore Empty Sets",
      "type": "boolean"
    },
    "records_per_second": {
      "default": 0,
      "description": "Number of records per second for Aerospike query. Default is 0, which means no limit.",
      "title": "Records Per Second",
      "type": "integer"
    },
    "schema_query_timeout_ms": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Socket timeout in milliseconds for schema inference queries. Default None uses the Aerospike client default.",
      "title": "Schema Query Timeout Ms"
    },
    "login_timeout_ms": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Login timeout in milliseconds. Default None, using the default value of the Aerospike Python client.",
      "title": "Login Timeout Ms"
    },
    "include_xdr": {
      "default": false,
      "description": "Include XDR information in the dataset properties.",
      "title": "Include Xdr",
      "type": "boolean"
    }
  },
  "title": "AerospikeConfig",
  "type": "object"
}
```





### Capabilities

- **Schema inference** — bin names and types are inferred by sampling records. Nested maps and lists are traversed up to the configured depth.
- **Namespace containers** — namespaces are represented as DataHub containers, with sets organized under them.
- **XDR metadata** — when `include_xdr` is enabled, sets are annotated with the data centers they replicate to.
- **Stateful ingestion** — enables automatic removal of datasets from DataHub when the corresponding sets are dropped from Aerospike.

### Limitations

- Schema inference relies on sampling and may not capture all bin names if the data is sparse or heterogeneous.
- Aerospike does not store schema definitions; all type information is derived from record values at ingestion time.
- The primary key (`PK`) field is always reported as a string since Aerospike returns digest-based keys.

### Troubleshooting

#### Connection Refused

Ensure the Aerospike node is reachable on the configured host and port. Verify firewall rules and that the service port is correct.

#### Empty Schema

If sets appear with no schema fields, verify that `infer_schema_depth` is not set to `0` and that `schema_sampling_size` is large enough to capture representative records.


### Code Coordinates
- Class Name: `datahub.ingestion.source.aerospike.AerospikeSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/aerospike.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Aerospike, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
