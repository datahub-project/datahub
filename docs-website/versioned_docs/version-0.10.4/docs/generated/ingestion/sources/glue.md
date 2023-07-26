---
sidebar_position: 16
title: Glue
slug: /generated/ingestion/sources/glue
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/glue.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Glue

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                                                    |
| ---------------------------------------------------------------------------------------------------------- | ------ | -------------------------------------------------------- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Enabled by default when stateful ingestion is turned on. |
| [Domains](../../../domains.md)                                                                             | ✅     | Supported via the `domain` config field                  |
| [Platform Instance](../../../platform-instances.md)                                                        | ✅     | Enabled by default                                       |

Note: if you also have files in S3 that you'd like to ingest, we recommend you use Glue's built-in data catalog. See [here](../../../../docs/generated/ingestion/sources/s3.md) for a quick guide on how to set up a crawler on Glue and ingest the outputs with DataHub.

This plugin extracts the following:

- Tables in the Glue catalog
- Column types associated with each table
- Table metadata, such as owner, description and parameters
- Jobs and their component transformations, data sources, and data sinks

### IAM permissions

For ingesting datasets, the following IAM permissions are required:

```json
{
  "Effect": "Allow",
  "Action": ["glue:GetDatabases", "glue:GetTables"],
  "Resource": [
    "arn:aws:glue:$region-id:$account-id:catalog",
    "arn:aws:glue:$region-id:$account-id:database/*",
    "arn:aws:glue:$region-id:$account-id:table/*"
  ]
}
```

For ingesting jobs (`extract_transforms: True`), the following additional permissions are required:

```json
{
  "Effect": "Allow",
  "Action": ["glue:GetDataflowGraph", "glue:GetJobs"],
  "Resource": "*"
}
```

plus `s3:GetObject` for the job script locations.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[glue]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: glue
  config:
    # Coordinates
    aws_region: "my-aws-region"

sink:
  # sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                              | Description                                                                                                                                                                                                                                                                                                                                               |
| :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">aws_region</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                               | AWS region code.                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                              | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                               | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                    | Named AWS profile to use. Only used if access key / secret are unset. If not set the default will be used                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                                                                             |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                          | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                              | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">catalog_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                     | The aws account id where the target glue catalog lives. If None, datahub will ingest glue in aws caller's account.                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">emit_s3_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                               | Whether to emit S3-to-Glue lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">extract_owners</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                | When enabled, extracts ownership from Glue directly and overwrites existing owners. When disabled, ownership is left empty for datasets. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                  |
| <div className="path-line"><span className="path-main">extract_transforms</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                            | Whether to extract Glue transform jobs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">glue_s3_lineage_direction</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                      | If `upstream`, S3 is upstream to Glue. If `downstream` S3 is downstream to Glue. <div className="default-line default-line-with-docs">Default: <span className="default-value">upstream</span></div>                                                                                                                                                      |
| <div className="path-line"><span className="path-main">ignore_resource_links</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                         | If set to True, ignore database resource links. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">ignore_unsupported_connectors</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                 | Whether to ignore unsupported connectors. If disabled, an error will be raised. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                           |
| <div className="path-line"><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                       | The platform to use for the dataset URNs. Must be one of ['glue', 'athena']. <div className="default-line default-line-with-docs">Default: <span className="default-value">glue</span></div>                                                                                                                                                              |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                              | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">use_s3_bucket_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                            | If an S3 Buckets Tags should be created for the Tables ingested by Glue. Please Note that this will not apply tags to any folders ingested, only the files. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                              |
| <div className="path-line"><span className="path-main">use_s3_object_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                            | If an S3 Objects Tags should be created for the Tables ingested by Glue. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                            | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, union(anyOf), string, AwsAssumeRoleConfig</span></div>                                                     | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are documented at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role |
| <div className="path-line"><span className="path-prefix">aws_role.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if aws_role is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">aws_role.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | External ID to use when assuming the role.                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">database_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                     | regex patterns for databases to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                      |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                             |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                              |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                              | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div>                                                                                      | A class to store allow deny regexes                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                 |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                  |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                  | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                        | regex patterns for tables to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                         |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                 |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                 | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">GlueProfilingConfig</span></div>                                                                                         | Configs to ingest data profiles from glue table                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">column_count</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                    | The parameter name for column count in glue table.                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                             | The parameter name for the max value of a column.                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">mean</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                            | The parameter name for the mean value of a column.                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">median</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                          | The parameter name for the median value of a column.                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">min</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                             | The parameter name for the min value of a column.                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">null_count</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                      | The parameter name for the count of null values in a column.                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">null_proportion</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                 | The parameter name for the proportion of null values in a column.                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">row_count</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | The parameter name for row count in glue table.                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">stdev</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                           | The parameter name for the standard deviation of a column.                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">unique_count</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                    | The parameter name for the count of unique value in a column.                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">unique_proportion</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                               | The parameter name for the proportion of unique values in a column.                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_patterns</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                    | Regex patterns for filtering partitions for profile. The pattern should be a string like: "{'key':'value'}". <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>              |
| <div className="path-line"><span className="path-prefix">profiling.partition_patterns.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                 |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">profiling.partition_patterns.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                  |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">profiling.partition_patterns.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                  | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                                                 | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                               | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                 | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                              |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "GlueSourceConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
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
    },
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "database_pattern": {
      "title": "Database Pattern",
      "description": "regex patterns for databases to filter in ingestion.",
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
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "stateful_ingestion": {
      "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
    },
    "platform": {
      "title": "Platform",
      "description": "The platform to use for the dataset URNs. Must be one of ['glue', 'athena'].",
      "default": "glue",
      "type": "string"
    },
    "extract_owners": {
      "title": "Extract Owners",
      "description": "When enabled, extracts ownership from Glue directly and overwrites existing owners. When disabled, ownership is left empty for datasets.",
      "default": true,
      "type": "boolean"
    },
    "extract_transforms": {
      "title": "Extract Transforms",
      "description": "Whether to extract Glue transform jobs.",
      "default": true,
      "type": "boolean"
    },
    "ignore_unsupported_connectors": {
      "title": "Ignore Unsupported Connectors",
      "description": "Whether to ignore unsupported connectors. If disabled, an error will be raised.",
      "default": true,
      "type": "boolean"
    },
    "emit_s3_lineage": {
      "title": "Emit S3 Lineage",
      "description": "Whether to emit S3-to-Glue lineage.",
      "default": false,
      "type": "boolean"
    },
    "glue_s3_lineage_direction": {
      "title": "Glue S3 Lineage Direction",
      "description": "If `upstream`, S3 is upstream to Glue. If `downstream` S3 is downstream to Glue.",
      "default": "upstream",
      "type": "string"
    },
    "domain": {
      "title": "Domain",
      "description": "regex patterns for tables to filter to assign domain_key. ",
      "default": {},
      "type": "object",
      "additionalProperties": {
        "$ref": "#/definitions/AllowDenyPattern"
      }
    },
    "catalog_id": {
      "title": "Catalog Id",
      "description": "The aws account id where the target glue catalog lives. If None, datahub will ingest glue in aws caller's account.",
      "type": "string"
    },
    "ignore_resource_links": {
      "title": "Ignore Resource Links",
      "description": "If set to True, ignore database resource links.",
      "default": false,
      "type": "boolean"
    },
    "use_s3_bucket_tags": {
      "title": "Use S3 Bucket Tags",
      "description": "If an S3 Buckets Tags should be created for the Tables ingested by Glue. Please Note that this will not apply tags to any folders ingested, only the files.",
      "default": false,
      "type": "boolean"
    },
    "use_s3_object_tags": {
      "title": "Use S3 Object Tags",
      "description": "If an S3 Objects Tags should be created for the Tables ingested by Glue.",
      "default": false,
      "type": "boolean"
    },
    "profiling": {
      "title": "Profiling",
      "description": "Configs to ingest data profiles from glue table",
      "allOf": [
        {
          "$ref": "#/definitions/GlueProfilingConfig"
        }
      ]
    }
  },
  "required": [
    "aws_region"
  ],
  "additionalProperties": false,
  "definitions": {
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
    "GlueProfilingConfig": {
      "title": "GlueProfilingConfig",
      "type": "object",
      "properties": {
        "row_count": {
          "title": "Row Count",
          "description": "The parameter name for row count in glue table.",
          "type": "string"
        },
        "column_count": {
          "title": "Column Count",
          "description": "The parameter name for column count in glue table.",
          "type": "string"
        },
        "unique_count": {
          "title": "Unique Count",
          "description": "The parameter name for the count of unique value in a column.",
          "type": "string"
        },
        "unique_proportion": {
          "title": "Unique Proportion",
          "description": "The parameter name for the proportion of unique values in a column.",
          "type": "string"
        },
        "null_count": {
          "title": "Null Count",
          "description": "The parameter name for the count of null values in a column.",
          "type": "string"
        },
        "null_proportion": {
          "title": "Null Proportion",
          "description": "The parameter name for the proportion of null values in a column.",
          "type": "string"
        },
        "min": {
          "title": "Min",
          "description": "The parameter name for the min value of a column.",
          "type": "string"
        },
        "max": {
          "title": "Max",
          "description": "The parameter name for the max value of a column.",
          "type": "string"
        },
        "mean": {
          "title": "Mean",
          "description": "The parameter name for the mean value of a column.",
          "type": "string"
        },
        "median": {
          "title": "Median",
          "description": "The parameter name for the median value of a column.",
          "type": "string"
        },
        "stdev": {
          "title": "Stdev",
          "description": "The parameter name for the standard deviation of a column.",
          "type": "string"
        },
        "partition_patterns": {
          "title": "Partition Patterns",
          "description": "Regex patterns for filtering partitions for profile. The pattern should be a string like: \"{'key':'value'}\".",
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
        }
      },
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

### Concept Mapping

| Source Concept       | DataHub Concept                                           | Notes              |
| -------------------- | --------------------------------------------------------- | ------------------ |
| `"glue"`             | [Data Platform](../../metamodel/entities/dataPlatform.md) |                    |
| Glue Database        | [Container](../../metamodel/entities/container.md)        | Subtype `Database` |
| Glue Table           | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Table`    |
| Glue Job             | [Data Flow](../../metamodel/entities/dataFlow.md)         |                    |
| Glue Job Transform   | [Data Job](../../metamodel/entities/dataJob.md)           |                    |
| Glue Job Data source | [Dataset](../../metamodel/entities/dataset.md)            |                    |
| Glue Job Data sink   | [Dataset](../../metamodel/entities/dataset.md)            |                    |

### Compatibility

To capture lineage across Glue jobs and databases, a requirements must be met – otherwise the AWS API is unable to report any lineage. The job must be created in Glue Studio with the "Generate classic script" option turned on (this option can be accessed in the "Script" tab). Any custom scripts that do not have the proper annotations will not have reported lineage.

### Code Coordinates

- Class Name: `datahub.ingestion.source.aws.glue.GlueSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/aws/glue.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Glue, feel free to ping us on [our Slack](https://slack.datahubproject.io).
