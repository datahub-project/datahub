---
sidebar_position: 9
title: Delta Lake
slug: /generated/ingestion/sources/delta-lake
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/delta-lake.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Delta Lake

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

### Important Capabilities

| Capability   | Status | Notes                                        |
| ------------ | ------ | -------------------------------------------- |
| Extract Tags | ✅     | Can extract S3 object/bucket tags if enabled |

This plugin extracts:

- Column types and schema associated with each delta table
- Custom properties: number_of_files, partition_columns, table_creation_time, location, version etc.

:::caution

If you are ingesting datasets from AWS S3, we recommend running the ingestion on a server in the same region to avoid high egress costs.

:::

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[delta-lake]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: delta-lake
  config:
    env: "PROD"
    platform_instance: "my-delta-lake"
    base_path: "/path/to/data/folder"

sink:
  # sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                                            | Description                                                                                                                                                                                                                                                                                                                                               |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">base_path</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                              | Path to table (s3 or local file system). If path is not a delta table path then all subfolders will be scanned to detect and ingest delta tables.                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                     | The platform that this source connects to <div className="default-line default-line-with-docs">Default: <span className="default-value">delta-lake</span></div>                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                            | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">relative_path</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                | If set, delta-tables will be searched at location '<base_path>/<relative_path>' and URNs will be created using relative_path only.                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">require_files</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                               | Whether DeltaTable should track files. Consider setting this to `False` for large delta tables, resulting in significant memory reduction for ingestion process.When set to `False`, number_of_files in delta table can not be reported. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>  |
| <div className="path-line"><span className="path-main">version_history_lookback</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                                                    | Number of previous version histories to be ingested. Defaults to 1. If set to -1 all version history will be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">1</span></div>                                                                                                                       |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                          | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">s3</span></div> <div className="type-name-line"><span className="type-name">S3</span></div>                                                                                                                               |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">s3.</span><span className="path-main">use_s3_bucket_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                  | Whether or not to create tags in datahub from the s3 bucket <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">s3.</span><span className="path-main">use_s3_object_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                  | # Whether or not to create tags in datahub from the s3 object <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">s3.</span><span className="path-main">aws_config</span></div> <div className="type-name-line"><span className="type-name">AwsConnectionConfig</span></div>                                                              | AWS configuration                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_region</span>&nbsp;<abbr title="Required if aws_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>     | AWS region code.                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                         | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                          | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                               | Named AWS profile to use. Only used if access key / secret are unset. If not set the default will be used                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                                        |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                     | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                         | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, union(anyOf), string, AwsAssumeRoleConfig</span></div>                | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are documented at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role |
| <div className="path-line"><span className="path-prefix">s3.aws_config.aws_role.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if aws_role is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">s3.aws_config.aws_role.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | External ID to use when assuming the role.                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                      | regex patterns for tables to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                         |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                              |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                               |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                               | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                               |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "DeltaLakeSourceConfig",
  "description": "Any source that connects to a platform should inherit this class",
  "type": "object",
  "properties": {
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
    "base_path": {
      "title": "Base Path",
      "description": "Path to table (s3 or local file system). If path is not a delta table path then all subfolders will be scanned to detect and ingest delta tables.",
      "type": "string"
    },
    "relative_path": {
      "title": "Relative Path",
      "description": "If set, delta-tables will be searched at location '<base_path>/<relative_path>' and URNs will be created using relative_path only.",
      "type": "string"
    },
    "platform": {
      "title": "Platform",
      "description": "The platform that this source connects to",
      "default": "delta-lake",
      "const": "delta-lake",
      "type": "string"
    },
    "table_pattern": {
      "title": "Table Pattern",
      "description": "regex patterns for tables to filter in ingestion.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "version_history_lookback": {
      "title": "Version History Lookback",
      "description": "Number of previous version histories to be ingested. Defaults to 1. If set to -1 all version history will be ingested.",
      "default": 1,
      "type": "integer"
    },
    "require_files": {
      "title": "Require Files",
      "description": "Whether DeltaTable should track files. Consider setting this to `False` for large delta tables, resulting in significant memory reduction for ingestion process.When set to `False`, number_of_files in delta table can not be reported.",
      "default": true,
      "type": "boolean"
    },
    "s3": {
      "$ref": "#/definitions/S3"
    }
  },
  "required": [
    "base_path"
  ],
  "additionalProperties": false,
  "definitions": {
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "AwsAssumeRoleConfig": {
      "title": "AwsAssumeRoleConfig",
      "type": "object",
      "properties": {
        "RoleArn": {
          "title": "Rolearn",
          "description": "ARN of the role to assume.",
          "type": "string"
        },
        "ExternalId": {
          "title": "Externalid",
          "description": "External ID to use when assuming the role.",
          "type": "string"
        }
      },
      "required": [
        "RoleArn"
      ]
    },
    "AwsConnectionConfig": {
      "title": "AwsConnectionConfig",
      "description": "Common AWS credentials config.\n\nCurrently used by:\n    - Glue source\n    - SageMaker source\n    - dbt source",
      "type": "object",
      "properties": {
        "aws_access_key_id": {
          "title": "Aws Access Key Id",
          "description": "AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "type": "string"
        },
        "aws_secret_access_key": {
          "title": "Aws Secret Access Key",
          "description": "AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "type": "string"
        },
        "aws_session_token": {
          "title": "Aws Session Token",
          "description": "AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "type": "string"
        },
        "aws_role": {
          "title": "Aws Role",
          "description": "AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are documented at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role",
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "array",
              "items": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "$ref": "#/definitions/AwsAssumeRoleConfig"
                  }
                ]
              }
            }
          ]
        },
        "aws_profile": {
          "title": "Aws Profile",
          "description": "Named AWS profile to use. Only used if access key / secret are unset. If not set the default will be used",
          "type": "string"
        },
        "aws_region": {
          "title": "Aws Region",
          "description": "AWS region code.",
          "type": "string"
        },
        "aws_endpoint_url": {
          "title": "Aws Endpoint Url",
          "description": "The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.",
          "type": "string"
        },
        "aws_proxy": {
          "title": "Aws Proxy",
          "description": "A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details.",
          "type": "object",
          "additionalProperties": {
            "type": "string"
          }
        }
      },
      "required": [
        "aws_region"
      ],
      "additionalProperties": false
    },
    "S3": {
      "title": "S3",
      "type": "object",
      "properties": {
        "aws_config": {
          "title": "Aws Config",
          "description": "AWS configuration",
          "allOf": [
            {
              "$ref": "#/definitions/AwsConnectionConfig"
            }
          ]
        },
        "use_s3_bucket_tags": {
          "title": "Use S3 Bucket Tags",
          "description": "Whether or not to create tags in datahub from the s3 bucket",
          "default": false,
          "type": "boolean"
        },
        "use_s3_object_tags": {
          "title": "Use S3 Object Tags",
          "description": "# Whether or not to create tags in datahub from the s3 object",
          "default": false,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

## Usage Guide

If you are new to [Delta Lake](https://delta.io/) and want to test out a simple integration with Delta Lake and DataHub, you can follow this guide.

### Delta Table on Local File System

#### Step 1

Create a delta table using the sample PySpark code below if you don't have a delta table you can point to.

```python
import uuid
import random
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

def generate_data():
    return [(y, m, d, str(uuid.uuid4()), str(random.randrange(10000) % 26 + 65) * 3, random.random()*10000)
    for d in range(1, 29)
    for m in range(1, 13)
    for y in range(2000, 2021)]

jar_packages = ["org.apache.hadoop:hadoop-aws:3.2.3", "io.delta:delta-core_2.12:1.2.1"]
spark = SparkSession.builder \
    .appName("quickstart") \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join(jar_packages)) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

table_path = "quickstart/my-table"
columns = ["year", "month", "day", "sale_id", "customer", "total_cost"]
spark.sparkContext.parallelize(generate_data()).toDF(columns).repartition(1).write.format("delta").save(table_path)

df = spark.read.format("delta").load(table_path)
df.show()

```

#### Step 2

Create a datahub ingestion yaml file (delta.dhub.yaml) to ingest metadata from the delta table you just created.

```yaml
source:
  type: "delta-lake"
  config:
    base_path: "quickstart/my-table"

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

Note: Make sure you run the Spark code as well as recipe from same folder otherwise use absolute paths.

#### Step 3

Execute the ingestion recipe:

```shell
datahub ingest -c delta.dhub.yaml
```

### Delta Table on S3

#### Step 1

Set up your AWS credentials by creating an AWS credentials config file; typically in '$HOME/.aws/credentials'.

```
[my-creds]
aws_access_key_id: ######
aws_secret_access_key: ######
```

Step 2: Create a Delta Table using the PySpark sample code below unless you already have Delta Tables on your S3.

```python
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from configparser import ConfigParser
import uuid
import random
def generate_data():
    return [(y, m, d, str(uuid.uuid4()), str(random.randrange(10000) % 26 + 65) * 3, random.random()*10000)
    for d in range(1, 29)
    for m in range(1, 13)
    for y in range(2000, 2021)]

jar_packages = ["org.apache.hadoop:hadoop-aws:3.2.3", "io.delta:delta-core_2.12:1.2.1"]
spark = SparkSession.builder \
    .appName("quickstart") \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join(jar_packages)) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


config_object = ConfigParser()
config_object.read("$HOME/.aws/credentials")
profile_info = config_object["my-creds"]
access_id = profile_info["aws_access_key_id"]
access_key = profile_info["aws_secret_access_key"]

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)

table_path = "s3a://my-bucket/my-folder/sales-table"
columns = ["year", "month", "day", "sale_id", "customer", "total_cost"]
spark.sparkContext.parallelize(generate_data()).toDF(columns).repartition(1).write.format("delta").save(table_path)
df = spark.read.format("delta").load(table_path)
df.show()

```

#### Step 3

Create a datahub ingestion yaml file (delta.s3.dhub.yaml) to ingest metadata from the delta table you just created.

```yml
source:
  type: "delta-lake"
  config:
    base_path: "s3://my-bucket/my-folder/sales-table"
    s3:
      aws_config:
        aws_access_key_id: <<Access key>>
        aws_secret_access_key: <<secret key>>

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

#### Step 4

Execute the ingestion recipe:

```shell
datahub ingest -c delta.s3.dhub.yaml
```

### Note

The above recipes are minimal recipes. Please refer to [Config Details](#config-details) section for the full configuration.

### Code Coordinates

- Class Name: `datahub.ingestion.source.delta_lake.source.DeltaLakeSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/delta_lake/source.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Delta Lake, feel free to ping us on [our Slack](https://slack.datahubproject.io).
