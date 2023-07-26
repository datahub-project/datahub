---
sidebar_position: 17
title: Google Cloud Storage
slug: /generated/ingestion/sources/gcs
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/gcs.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Google Cloud Storage

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

### Important Capabilities

| Capability                                                                       | Status | Notes              |
| -------------------------------------------------------------------------------- | ------ | ------------------ |
| Asset Containers                                                                 | ✅     | Enabled by default |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ❌     | Not supported      |
| Schema Metadata                                                                  | ✅     | Enabled by default |

This connector extracting datasets located on Google Cloud Storage. Supported file types are as follows:

- CSV
- TSV
- JSON
- Parquet
- Apache Avro

Schemas for Parquet and Avro files are extracted as provided.

Schemas for schemaless formats (CSV, TSV, JSON) are inferred. For CSV and TSV files, we consider the first 100 rows by default, which can be controlled via the `max_rows` recipe parameter (see [below](#config-details))
JSON file schemas are inferred on the basis of the entire file (given the difficulty in extracting only the first few objects of the file), which may impact performance.

This source leverages [Interoperability of GCS with S3](https://cloud.google.com/storage/docs/interoperability)
and uses DataHub S3 Data Lake integration source under the hood.

### Prerequisites

1. Create a service account with "Storage Object Viewer" Role - https://cloud.google.com/iam/docs/service-accounts-create
2. Make sure you meet following requirements to generate HMAC key - https://cloud.google.com/storage/docs/authentication/managing-hmackeys#before-you-begin
3. Create an HMAC key for service account created above - https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create .

To ingest datasets from your data lake, you need to provide the dataset path format specifications using `path_specs` configuration in ingestion recipe.
Refer section [Path Specs](/docs/generated/ingestion/sources/gcs/#path-specs) for examples.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[gcs]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: gcs
  config:
    path_specs:
      - include: gs://gcs-ingestion-bucket/parquet_example/{table}/year={partition[0]}/*.parquet
    credential:
      hmac_access_id: <hmac access id>
      hmac_access_secret: <hmac access secret>
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                                                       | Description                                                                                                                                                                                                                                                                                                                |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">credential</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">HMACKey</span></div>                                                                                       | Google cloud storage [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys)                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">hmac_access_id</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>               | Access ID                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">credential.</span><span className="path-main">hmac_access_secret</span>&nbsp;<abbr title="Required if credential is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Secret                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">max_rows</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                                                                               | Maximum number of rows to use when inferring schemas for TSV and CSV files. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div>                                                                                                                                 |
| <div className="path-line"><span className="path-main">number_of_files_to_sample</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                                                              | Number of files to list to sample for schema inference. This will be ignored if sample_files is set to False in the pathspec. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div>                                                                               |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                       | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                                     | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                       |
| <div className="path-line"><span className="path-main">path_specs</span></div> <div className="type-name-line"><span className="type-name">array(object)</span></div>                                                                                                                       |                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">path_specs.</span><span className="path-main">include</span>&nbsp;<abbr title="Required if path_specs is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                      | Path to table. Name variable `{table}` is used to mark the folder with dataset. In absence of `{table}`, file level dataset will be created. Check below examples for more details.                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">path_specs.</span><span className="path-main">default_extension</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                       | For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped.                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">path_specs.</span><span className="path-main">enable_compression</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                     | Enable or disable processing compressed files. Currently .gz and .bz files are supported. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                  |
| <div className="path-line"><span className="path-prefix">path_specs.</span><span className="path-main">exclude</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                          |                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">path_specs.</span><span className="path-main">file_types</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                       |                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">path_specs.</span><span className="path-main">sample_files</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                           | Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">path_specs.</span><span className="path-main">table_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                              | Display name of the dataset.Combination of named variables from include path and strings                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                                                                          | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                        | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                          | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                               |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "GCSSourceConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
  "type": "object",
  "properties": {
    "path_specs": {
      "title": "Path Specs",
      "description": "List of PathSpec. See [below](#path-spec) the details about PathSpec",
      "type": "array",
      "items": {
        "$ref": "#/definitions/PathSpec"
      }
    },
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "stateful_ingestion": {
      "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
    },
    "credential": {
      "title": "Credential",
      "description": "Google cloud storage [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys)",
      "allOf": [
        {
          "$ref": "#/definitions/HMACKey"
        }
      ]
    },
    "max_rows": {
      "title": "Max Rows",
      "description": "Maximum number of rows to use when inferring schemas for TSV and CSV files.",
      "default": 100,
      "type": "integer"
    },
    "number_of_files_to_sample": {
      "title": "Number Of Files To Sample",
      "description": "Number of files to list to sample for schema inference. This will be ignored if sample_files is set to False in the pathspec.",
      "default": 100,
      "type": "integer"
    }
  },
  "required": [
    "path_specs",
    "credential"
  ],
  "additionalProperties": false,
  "definitions": {
    "PathSpec": {
      "title": "PathSpec",
      "type": "object",
      "properties": {
        "include": {
          "title": "Include",
          "description": "Path to table. Name variable `{table}` is used to mark the folder with dataset. In absence of `{table}`, file level dataset will be created. Check below examples for more details.",
          "type": "string"
        },
        "exclude": {
          "title": "Exclude",
          "description": "list of paths in glob pattern which will be excluded while scanning for the datasets",
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "file_types": {
          "title": "File Types",
          "description": "Files with extenstions specified here (subset of default value) only will be scanned to create dataset. Other files will be omitted.",
          "default": [
            "csv",
            "tsv",
            "json",
            "parquet",
            "avro"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "default_extension": {
          "title": "Default Extension",
          "description": "For files without extension it will assume the specified file type. If it is not set the files without extensions will be skipped.",
          "type": "string"
        },
        "table_name": {
          "title": "Table Name",
          "description": "Display name of the dataset.Combination of named variables from include path and strings",
          "type": "string"
        },
        "enable_compression": {
          "title": "Enable Compression",
          "description": "Enable or disable processing compressed files. Currently .gz and .bz files are supported.",
          "default": true,
          "type": "boolean"
        },
        "sample_files": {
          "title": "Sample Files",
          "description": "Not listing all the files but only taking a handful amount of sample file to infer the schema. File count and file size calculation will be disabled. This can affect performance significantly if enabled",
          "default": true,
          "type": "boolean"
        }
      },
      "required": [
        "include"
      ],
      "additionalProperties": false
    },
    "DynamicTypedStateProviderConfig": {
      "title": "DynamicTypedStateProviderConfig",
      "type": "object",
      "properties": {
        "type": {
          "title": "Type",
          "description": "The type of the state provider to use. For DataHub use `datahub`",
          "type": "string"
        },
        "config": {
          "title": "Config",
          "description": "The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19)."
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": false
    },
    "StatefulStaleMetadataRemovalConfig": {
      "title": "StatefulStaleMetadataRemovalConfig",
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "The type of the ingestion state provider registered with datahub.",
          "default": false,
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "title": "Remove Stale Metadata",
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "HMACKey": {
      "title": "HMACKey",
      "type": "object",
      "properties": {
        "hmac_access_id": {
          "title": "Hmac Access Id",
          "description": "Access ID",
          "type": "string"
        },
        "hmac_access_secret": {
          "title": "Hmac Access Secret",
          "description": "Secret",
          "type": "string",
          "writeOnly": true,
          "format": "password"
        }
      },
      "required": [
        "hmac_access_id",
        "hmac_access_secret"
      ],
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

### Path Specs

**Example - Dataset per file**

Bucket structure:

```
test-gs-bucket
├── employees.csv
└── food_items.csv
```

Path specs config

```
path_specs:
    - include: gs://test-gs-bucket/*.csv

```

**Example - Datasets with partitions**

Bucket structure:

```
test-gs-bucket
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── returns
    └── year=2021
        └── month=2
            └── 1.parquet

```

Path specs config:

```
path_specs:
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

**Example - Datasets with partition and exclude**

Bucket structure:

```
test-gs-bucket
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── tmp_orders
    └── year=2021
        └── month=2
            └── 1.parquet


```

Path specs config:

```
path_specs:
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
      exclude:
        - **/tmp_orders/**
```

**Example - Datasets of mixed nature**

Bucket structure:

```
test-gs-bucket
├── customers
│   ├── part1.json
│   ├── part2.json
│   ├── part3.json
│   └── part4.json
├── employees.csv
├── food_items.csv
├── tmp_10101000.csv
└──  orders
    └── year=2022
        └── month=2
            ├── 1.parquet
            ├── 2.parquet
            └── 3.parquet

```

Path specs config:

```
path_specs:
    - include: gs://test-gs-bucket/*.csv
      exclude:
        - **/tmp_10101000.csv
    - include: gs://test-gs-bucket/{table}/*.json
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

**Valid path_specs.include**

```python
gs://my-bucket/foo/tests/bar.avro # single file table
gs://my-bucket/foo/tests/*.* # mulitple file level tables
gs://my-bucket/foo/tests/{table}/*.avro #table without partition
gs://my-bucket/foo/tests/{table}/*/*.avro #table where partitions are not specified
gs://my-bucket/foo/tests/{table}/*.* # table where no partitions as well as data type specified
gs://my-bucket/{dept}/tests/{table}/*.avro # specifying keywords to be used in display name
gs://my-bucket/{dept}/tests/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.avro # specify partition key and value format
gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.avro # specify partition value only format
gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # for all extensions
gs://my-bucket/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 2 levels down in bucket
gs://my-bucket/*/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 3 levels down in bucket
```

**Valid path_specs.exclude**

- \*\*/tests/\*\*
- gs://my-bucket/hr/\*\*
- \*_/tests/_.csv
- gs://my-bucket/foo/\*/my_table/\*\*

**Notes**

- {table} represents folder for which dataset will be created.
- include path must end with (_._ or \*.[ext]) to represent leaf level.
- if \*.[ext] is provided then only files with specified type will be scanned.
- /\*/ represents single folder.
- {partition[i]} represents value of partition.
- {partition_key[i]} represents name of the partition.
- While extracting, “i” will be used to match partition_key to partition.
- all folder levels need to be specified in include. Only exclude path can have \*\* like matching.
- exclude path cannot have named variables ( {} ).
- Folder names should not contain {, }, \*, / in their names.
- {folder} is reserved for internal working. please do not use in named variables.

If you would like to write a more complicated function for resolving file names, then a {transformer} would be a good fit.

:::caution

Specify as long fixed prefix ( with out /\*/ ) as possible in `path_specs.include`. This will reduce the scanning time and cost, specifically on Google Cloud Storage.

:::

:::caution

If you are ingesting datasets from Google Cloud Storage, we recommend running the ingestion on a server in the same region to avoid high egress costs.

:::

### Code Coordinates

- Class Name: `datahub.ingestion.source.gcs.gcs_source.GCSSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/gcs/gcs_source.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Google Cloud Storage, feel free to ping us on [our Slack](https://slack.datahubproject.io).
