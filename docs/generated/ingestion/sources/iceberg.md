


# Iceberg

## Overview

Apache Iceberg is a data platform used to store and query analytical or operational data. Learn more in the [official Apache Iceberg documentation](https://iceberg.apache.org/).

The DataHub integration for Apache Iceberg covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures data profiling, ownership, and stateful deletion detection.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `iceberg`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| Extract Ownership | ✅ | Automatically ingests ownership information from table properties based on `user_ownership_property` and `group_ownership_property`. |
| Partition Support | ❌ | Currently not supported. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Optionally enabled via configuration, an Iceberg instance represents the catalog name where the table is stored. |

### Overview

The `iceberg` module ingests metadata from Iceberg into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.


### Install the Plugin
```shell
pip install 'acryl-datahub[iceberg]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: iceberg
  config:
    catalog:
      my_catalog:
        type: "glue"
        s3.region: "us-west-2"
        region_name: "us-west-2"

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">catalog</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">map(str,object)</span></div> |   |
| <div className="path-line"><span className="path-main">group_ownership_property</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Iceberg table property to look for a `CorpGroup` owner.  Can only hold a single group value.  If property has no value, no owner information will be emitted. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">processing_threads</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | How many threads will be processing tables <div className="default-line default-line-with-docs">Default: <span className="default-value">1</span></div> |
| <div className="path-line"><span className="path-main">user_ownership_property</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Iceberg table property to look for a `CorpUser` owner.  Can only hold a single user value.  If property has no value, no owner information will be emitted. <div className="default-line default-line-with-docs">Default: <span className="default-value">owner</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">namespace_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">namespace_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">IcebergProfilingConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Iceberg Stateful Ingestion Config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "IcebergProfilingConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "include_field_null_count": {
          "default": true,
          "description": "Whether to profile for the number of nulls for each column.",
          "title": "Include Field Null Count",
          "type": "boolean"
        },
        "include_field_min_value": {
          "default": true,
          "description": "Whether to profile for the min value of numeric columns.",
          "title": "Include Field Min Value",
          "type": "boolean"
        },
        "include_field_max_value": {
          "default": true,
          "description": "Whether to profile for the max value of numeric columns.",
          "title": "Include Field Max Value",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        }
      },
      "title": "IcebergProfilingConfig",
      "type": "object"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
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
      "description": "Iceberg Stateful Ingestion Config."
    },
    "catalog": {
      "additionalProperties": {
        "additionalProperties": true,
        "type": "object"
      },
      "description": "Catalog configuration where to find Iceberg tables.  Only one catalog specification is supported.  The format is the same as [pyiceberg's catalog configuration](https://py.iceberg.apache.org/configuration/), where the catalog name is specified as the object name and attributes are set as key-value pairs.",
      "title": "Catalog",
      "type": "object"
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
      "description": "Regex patterns for tables to filter in ingestion."
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
      "description": "Regex patterns for namespaces to filter in ingestion."
    },
    "user_ownership_property": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "owner",
      "description": "Iceberg table property to look for a `CorpUser` owner.  Can only hold a single user value.  If property has no value, no owner information will be emitted.",
      "title": "User Ownership Property"
    },
    "group_ownership_property": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Iceberg table property to look for a `CorpGroup` owner.  Can only hold a single group value.  If property has no value, no owner information will be emitted.",
      "title": "Group Ownership Property"
    },
    "profiling": {
      "$ref": "#/$defs/IcebergProfilingConfig",
      "default": {
        "enabled": false,
        "include_field_null_count": true,
        "include_field_min_value": true,
        "include_field_max_value": true,
        "operation_config": {
          "lower_freq_profile_enabled": false,
          "profile_date_of_month": null,
          "profile_day_of_week": null
        }
      }
    },
    "processing_threads": {
      "default": 1,
      "description": "How many threads will be processing tables",
      "title": "Processing Threads",
      "type": "integer"
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "description": "A map of domain names to allow/deny patterns for tables and namespaces. Domain key can be a guid like \"urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba\" or a string like \"Engineering\". If multiple patterns match, at most one domain is assigned to the entity.",
      "title": "Domain",
      "type": "object"
    }
  },
  "required": [
    "catalog"
  ],
  "title": "IcebergSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Setting up connection to an Iceberg catalog

There are multiple servers compatible with the Iceberg Catalog specification. DataHub's `iceberg` connector uses `pyiceberg`
library to extract metadata from them. The recipe for the source consists of 2 parts:

1. `catalog` part which is passed as-is to the `pyiceberg` library and configures the connection and its details (i.e. authentication).
   The name of catalog specified in the recipe has no consequence, it is just a formal requirement from the library.
   Only one catalog will be considered for the ingestion.
2. The remaining configuration consists of parameters, such as `env` or `stateful_ingestion` which are standard
   DataHub's ingestor configuration parameters and are described in the [Config Details](#config-details) chapter.

This chapter showcases several examples of setting up connections to an Iceberg catalog, varying based on the underlying
implementation. Iceberg is designed to have catalog and warehouse separated, which is reflected in how we configure it.
It is especially visible when using Iceberg REST Catalog - which can use many blob storages
(AWS S3, Azure Blob Storage, MinIO) as a warehouse.

Note that, for advanced users, it is possible to specify a custom catalog client implementation via `py-catalog-impl`
configuration option - refer to `pyiceberg` documentation on details.

#### Glue catalog + S3 warehouse

The minimal configuration for connecting to Glue catalog with S3 warehouse:

```yaml
source:
  type: "iceberg"
  config:
    catalog:
      my_catalog:
        type: "glue"
        s3.region: "us-west-2"
        region_name: "us-west-2"
```

Where `us-west-2` is the region from which you want to ingest. The above configuration will work assuming your pod or environment in which
you run your datahub CLI is already authenticated to AWS and has proper permissions granted (see below). If you need
to specify secrets directly, use the following configuration as the template:

```yaml
source:
  type: "iceberg"
  config:
    catalog:
      demo:
        type: "glue"
        s3.region: "us-west-2"
        s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
        s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
        s3.session-token: "${AWS_SESSION_TOKEN}"

        aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
        aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
        aws_session_token: "${AWS_SESSION_TOKEN}"
        region_name: "us-west-2"
```

This example uses references to fill credentials (either from Secrets defined in Managed Ingestion or environmental variables).
It is possible (but not recommended due to security concerns) to provide those values in plaintext, directly in the recipe.

##### Glue and S3 permissions required

The role used by the ingestor for ingesting metadata from Glue Iceberg Catalog and S3 warehouse is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["glue:GetDatabases", "glue:GetTables", "glue:GetTable"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetObjectVersion"],
      "Resource": [
        "arn:aws:s3:::<bucket used by the warehouse>",
        "arn:aws:s3:::<bucket used by the warehouse>/*"
      ]
    }
  ]
}
```

#### Iceberg REST Catalog + MinIO

The following configuration assumes MinIO defines authentication using the `s3.*` prefix. Note the specification of `s3.endpoint`, assuming
MinIO listens on port `9000` at `minio-host`. The `uri` parameter points at Iceberg REST Catalog (IRC) endpoint (in this case `iceberg-catalog:8181`).

```yaml
source:
  type: "iceberg"
  config:
    catalog:
      demo:
        type: "rest"
        uri: "http://iceberg-catalog:8181"
        s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
        s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
        s3.region: "eu-east-1"
        s3.endpoint: "http://minio-host:9000"
```

#### Iceberg REST Catalog (with authentication) + S3

This example assumes IRC requires token authentication (via `Authorization` header). There are more options available,
see https://py.iceberg.apache.org/configuration/#rest-catalog for details. Moreover, the assumption here is that the
environment (i.e. pod) is already authenticated to perform actions against AWS S3.

```yaml
source:
  type: "iceberg"
  config:
    catalog:
      demo:
        type: "rest"
        uri: "http://iceberg-catalog-uri"
        token: "token-value"
        s3.region: "us-west-2"
```

##### Special REST connection parameters for resiliency

Unlike other parameters provided in the dictionary under the `catalog` key, `connection` parameter is a custom feature in
DataHub, allowing to inject connection resiliency parameters to the REST connection made by the ingestor. `connection`
allows for 2 parameters:

- `timeout` is provided as amount of seconds, it needs to be whole number (or `null` to turn it off)
- `retry` is a complex object representing parameters used to create [urllib3 Retry object](https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#module-urllib3.util.retry).
  There are many possible parameters, most important would be `total` (total retries) and `backoff_factor`. See the linked docs
  for the details.

```yaml
source:
  type: "iceberg"
  config:
    catalog:
      demo:
        type: "rest"
        uri: "http://iceberg-catalog-uri"
        connection:
          retry:
            backoff_factor: 0.5
            total: 3
          timeout: 120
```

#### Google BigLake REST Catalog + GCS warehouse

DataHub supports ingesting metadata from [Google BigLake](https://cloud.google.com/bigquery/docs/biglake-intro) via the Iceberg REST Catalog API.
BigLake provides unified governance and security for data across data lakes and data warehouses.

PyIceberg 0.9+ natively supports BigLake authentication using Google Cloud's Application Default Credentials (ADC).

##### Prerequisites

1. **GCP Project** with BigLake API enabled:

   ```bash
   gcloud services enable biglake.googleapis.com --project=YOUR_PROJECT_ID
   ```

2. **Service Account** with required permissions:

   - `biglake.catalogs.get`
   - `biglake.tables.get`
   - `biglake.tables.list`
   - `biglake.databases.get`
   - `biglake.databases.list`
   - Storage Object Viewer (for GCS buckets containing Iceberg data)

3. **BigLake Catalog** created in your GCP project:
   ```bash
   gcloud alpha biglake catalogs create CATALOG_NAME \
     --location=REGION \
     --project=PROJECT_ID
   ```

##### Authentication Setup

BigLake authentication uses Application Default Credentials (ADC). DataHub provides automatic OAuth scope fixing for seamless integration.

**Setup:**

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export GCP_PROJECT_ID=your-project-id
export GCS_WAREHOUSE_BUCKET=your-bucket-name
```

##### Configuration

```yaml
source:
  type: iceberg
  config:
    catalog:
      my_biglake_catalog:
        type: rest
        uri: https://biglake.googleapis.com/iceberg/v1/restcatalog
        warehouse: gs://my-bucket
        auth:
          type: google
          google:
            scopes:
              - https://www.googleapis.com/auth/cloud-platform
        header.x-goog-user-project: my-project
        header.X-Iceberg-Access-Delegation: "" # End-user credentials mode
        connection:
          timeout: 120
          retry:
            total: 5
            backoff_factor: 0.3
```

**Key Configuration Parameters:**

- `auth.type: google` - Uses Application Default Credentials
- `auth.google.scopes` - OAuth scopes required for BigLake access
- `header.x-goog-user-project` - Specifies the GCP project for billing
- `header.X-Iceberg-Access-Delegation: ""` - Uses end-user credentials mode

##### How Authentication Works

When using `auth.type: google` with explicit scopes:

1. **Discovers credentials** using Google Cloud's Application Default Credentials (ADC) chain:

   - **Environment Variable**: `GOOGLE_APPLICATION_CREDENTIALS` pointing to service account JSON (most common)
   - **gcloud CLI**: Credentials from `gcloud auth application-default login`
   - **GCE/GKE Metadata Server**: Automatic when running on Google Cloud infrastructure
   - **Workload Identity**: Automatic when using GKE Workload Identity

2. **Uses explicit OAuth scopes**: The `auth.google.scopes` configuration ensures the correct `cloud-platform` scope is used for BigLake access

   - PyIceberg passes the scopes directly to `google.auth.default()`
   - Requires `google-auth` library to be installed (included in DataHub dependencies)

3. **Handles token refresh**: Automatic token refresh with no manual management needed

##### Using Managed Ingestion with Secrets

For production environments using Managed Ingestion (via the DataHub UI), you can securely store your GCP service account credentials as DataHub secrets instead of using environment variables.

**Step 1: Create a Secret in DataHub**

1. Navigate to **Settings** > **Secrets** in the DataHub UI
2. Click **Create new secret**
3. Enter a name (e.g., `BIGLAKE_SERVICE_ACCOUNT_JSON`)
4. Paste the **entire contents** of your GCP service account JSON file as the value
5. Optionally add a description
6. Click **Create**

**Step 2: Reference the Secret in Your Recipe**

Use the `${SECRET_NAME}` syntax to reference your secret in the ingestion recipe:

```yaml
source:
  type: iceberg
  config:
    env: prod
    catalog:
      my_biglake_catalog:
        type: rest
        uri: https://biglake.googleapis.com/iceberg/v1/restcatalog
        warehouse: gs://my-bucket
        auth:
          type: google
          google:
            credentials_json: ${BIGLAKE_SERVICE_ACCOUNT_JSON}
            scopes:
              - https://www.googleapis.com/auth/cloud-platform
        header.x-goog-user-project: my-project
        header.X-Iceberg-Access-Delegation: ""

sink:
  type: datahub-rest
  config:
    server: ${DATAHUB_GMS_URL}
    token: ${DATAHUB_GMS_TOKEN}
```

The secret will be automatically resolved at runtime when the ingestion executes.

**Alternative: Using Structured Credentials**

You can also use individual secrets for each credential component, which provides better validation:

```yaml
source:
  type: iceberg
  config:
    catalog:
      my_biglake_catalog:
        type: rest
        uri: https://biglake.googleapis.com/iceberg/v1/restcatalog
        warehouse: gs://my-bucket
        auth:
          type: google
          google:
            credentials:
              project_id: ${GCP_PROJECT_ID}
              private_key_id: ${GCP_PRIVATE_KEY_ID}
              private_key: ${GCP_PRIVATE_KEY}
              client_email: ${GCP_CLIENT_EMAIL}
              client_id: ${GCP_CLIENT_ID}
            scopes:
              - https://www.googleapis.com/auth/cloud-platform
        header.x-goog-user-project: ${GCP_PROJECT_ID}
        header.X-Iceberg-Access-Delegation: ""
```

Create these individual secrets in DataHub:

- `GCP_PROJECT_ID`
- `GCP_PRIVATE_KEY_ID`
- `GCP_PRIVATE_KEY` (the private key value from your service account JSON)
- `GCP_CLIENT_EMAIL`
- `GCP_CLIENT_ID`

**Step 3: Deploy via DataHub UI**

1. Navigate to **Ingestion** > **Create new source**
2. Select **Iceberg** as the source type
3. Paste your recipe configuration with secret references
4. Configure a schedule (optional)
5. Click **Save and Run**

##### Using Vended Credentials (Optional)

**Important**: Vended credentials require your BigLake catalog to be configured with `CREDENTIAL_MODE_SERVICE_ACCOUNT`. Most BigLake catalogs use `CREDENTIAL_MODE_END_USER` by default, which **does not** support vended credentials.

If you get an error stating "X-Iceberg-Access-Delegation header must not contain vended-credentials when credential mode is CREDENTIAL_MODE_END_USER", your catalog doesn't support this feature. Use the standard configuration with `header.X-Iceberg-Access-Delegation: ""` instead.

For catalogs that support vended credentials, set `header.X-Iceberg-Access-Delegation: vended-credentials`:

```yaml
source:
  type: iceberg
  config:
    catalog:
      my_biglake_catalog:
        type: rest
        uri: https://biglake.googleapis.com/iceberg/v1/restcatalog
        warehouse: gs://my-bucket
        auth:
          type: google
          google:
            scopes:
              - https://www.googleapis.com/auth/cloud-platform
        header.x-goog-user-project: my-project
        header.X-Iceberg-Access-Delegation: vended-credentials # Only for CREDENTIAL_MODE_SERVICE_ACCOUNT
```

**When to use vended credentials:**

- Running ingestion from environments without direct GCS access
- Implementing fine-grained access control through BigLake
- Avoiding long-lived service account keys

BigLake will generate short-lived service account token scoped to the specific tables being accessed.

##### Troubleshooting

**Error: "invalid_scope: Invalid OAuth scope or ID token audience provided"**

This error occurs when OAuth scopes are not properly configured. To fix:

- Ensure `auth.google.scopes` is set to `["https://www.googleapis.com/auth/cloud-platform"]` in your configuration
- Verify `google-auth` library is installed: `pip install google-auth`
- Check that `GOOGLE_APPLICATION_CREDENTIALS` points to a valid service account JSON file

**Error: "X-Iceberg-Access-Delegation header must not contain vended-credentials when credential mode is CREDENTIAL_MODE_END_USER"**

- Your BigLake catalog uses end-user credentials mode, which doesn't support vended credentials
- **Solution**: Use `header.X-Iceberg-Access-Delegation: ""` (empty string) in your configuration
- Vended credentials only work with `CREDENTIAL_MODE_SERVICE_ACCOUNT`

**Error: "Authentication failed"**

- Verify ADC is configured: `gcloud auth application-default print-access-token`
- Check service account has required permissions
- Ensure BigLake API is enabled: `gcloud services list --enabled | grep biglake`

**Error: "Catalog not found"**

- Verify catalog exists: `gcloud alpha biglake catalogs list --location=REGION --project=PROJECT`
- Check URI format: `https://biglake.googleapis.com/v1/projects/{PROJECT}/locations/{REGION}/catalogs/{CATALOG}`

**Error: "Permission denied on GCS warehouse"**

- Grant Storage Object Viewer role to service account:
  ```bash
  gsutil iam ch serviceAccount:SA_EMAIL:roles/storage.objectViewer gs://BUCKET_NAME
  ```

**Error: "User project header required"**

- Ensure `header.x-goog-user-project` is set to your GCP project ID

#### SQL catalog + Azure DLS as the warehouse

This example targets `Postgres` as the sql-type `Iceberg` catalog and uses Azure DLS as the warehouse.

```yaml
source:
  type: "iceberg"
  config:
    catalog:
      demo:
        type: sql
        uri: postgresql+psycopg2://user:password@sqldatabase.postgres.database.azure.com:5432/icebergcatalog
        adlfs.tenant-id: <Azure tenant ID>
        adlfs.account-name: <Azure storage account name>
        adlfs.client-id: <Azure Client/Application ID>
        adlfs.client-secret: <Azure Client Secret>
```

#### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrelevant DataHub Concepts -->

| Source Concept                                                                                                                          | DataHub Concept                                                        | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `iceberg`                                                                                                                               | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md)     |                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Table                                                                                                                                   | [Dataset](docs/generated/metamodel/entities/dataset.md)                | An Iceberg table is registered inside a catalog using a name, where the catalog is responsible for creating, dropping and renaming tables. Catalogs manage a collection of tables that are usually grouped into namespaces. The name of a table is mapped to a Dataset name. If a [Platform Instance](/docs/platform-instances/) is configured, it will be used as a prefix: `<platform_instance>.my.namespace.table`. |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties)                                                | [User (a.k.a CorpUser)](docs/generated/metamodel/entities/corpuser.md) | The value of a table property can be used as the name of a CorpUser owner. This table property name can be configured with the source option `user_ownership_property`.                                                                                                                                                                                                                                                                        |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties)                                                | CorpGroup                                                              | The value of a table property can be used as the name of a CorpGroup owner. This table property name can be configured with the source option `group_ownership_property`.                                                                                                                                                                                                                                                                      |
| Table parent folders (excluding [warehouse catalog location](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties)) | Container                                                              | Available in a future release                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [Table schema](https://iceberg.apache.org/spec/#schemas-and-data-types)                                                                 | SchemaField                                                            | Maps to the fields defined within the Iceberg table schema definition.                                                                                                                                                                                                                                                                                                                                                                         |

#### DataHub Iceberg REST Catalog

DataHub also implements the Iceberg REST Catalog. See the [Iceberg Catalog documentation](docs/iceberg-catalog.md) for more details.

#### Integration Details

For advanced Iceberg behavior and tuning, refer to:

- [Iceberg catalog properties](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties)
- [Iceberg table properties](https://iceberg.apache.org/docs/latest/configuration/#table-properties)
- [Iceberg specification](https://iceberg.apache.org/spec/)
- [PyIceberg configuration](https://py.iceberg.apache.org/)

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Exceptions while increasing `processing_threads`

Each processing thread will open several files/sockets to download manifest files from blob storage. If you experience
exceptions appearing when increasing `processing_threads` configuration parameter, try to increase limit of open
files (e.g. using `ulimit` in Linux).


### Code Coordinates
- Class Name: `datahub.ingestion.source.iceberg.iceberg.IcebergSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/iceberg/iceberg.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Iceberg, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
