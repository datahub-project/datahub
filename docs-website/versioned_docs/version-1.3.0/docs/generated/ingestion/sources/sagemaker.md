---
sidebar_position: 61
title: SageMaker
slug: /generated/ingestion/sources/sagemaker
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/sagemaker.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SageMaker
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Table-Level Lineage | ✅ | Enabled by default. |


This plugin extracts the following:

- Feature groups
- Models, jobs, and lineage between the two (e.g. when jobs output a model or a model is used by a job)


### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: sagemaker
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

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_advanced_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).  |
| <div className="path-line"><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS region code. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_retry_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "legacy", "standard", "adaptive" <div className="default-line default-line-with-docs">Default: <span className="default-value">standard</span></div> |
| <div className="path-line"><span className="path-main">aws_retry_num</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">extract_feature_groups</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to extract feature groups. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_jobs</span></div> <div className="type-name-line"><span className="type-name">One of string, boolean, null</span></div> | Whether to extract AutoML jobs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">extract_models</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to extract models. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">read_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | The timeout for reading from the connection (in seconds). <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, array, null</span></div> | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_role.</span><span className="path-main">union</span></div> <div className="type-name-line"><span className="type-name">One of string, AwsAssumeRoleConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">aws_role.union.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if union is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.  |
| <div className="path-line"><span className="path-prefix">aws_role.union.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | External ID to use when assuming the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">database_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

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
    "AwsAssumeRoleConfig": {
      "additionalProperties": true,
      "properties": {
        "RoleArn": {
          "description": "ARN of the role to assume.",
          "title": "Rolearn",
          "type": "string"
        },
        "ExternalId": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "External ID to use when assuming the role.",
          "title": "Externalid"
        }
      },
      "required": [
        "RoleArn"
      ],
      "title": "AwsAssumeRoleConfig",
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
    "aws_access_key_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
      "title": "Aws Access Key Id"
    },
    "aws_secret_access_key": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
      "title": "Aws Secret Access Key"
    },
    "aws_session_token": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
      "title": "Aws Session Token"
    },
    "aws_role": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "items": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "$ref": "#/$defs/AwsAssumeRoleConfig"
              }
            ]
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role).",
      "title": "Aws Role"
    },
    "aws_profile": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config.",
      "title": "Aws Profile"
    },
    "aws_region": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "AWS region code.",
      "title": "Aws Region"
    },
    "aws_endpoint_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.",
      "title": "Aws Endpoint Url"
    },
    "aws_proxy": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details.",
      "title": "Aws Proxy"
    },
    "aws_retry_num": {
      "default": 5,
      "description": "Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
      "title": "Aws Retry Num",
      "type": "integer"
    },
    "aws_retry_mode": {
      "default": "standard",
      "description": "Retry mode to use for failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
      "enum": [
        "legacy",
        "standard",
        "adaptive"
      ],
      "title": "Aws Retry Mode",
      "type": "string"
    },
    "read_timeout": {
      "default": 60,
      "description": "The timeout for reading from the connection (in seconds).",
      "title": "Read Timeout",
      "type": "number"
    },
    "aws_advanced_config": {
      "additionalProperties": true,
      "description": "Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).",
      "title": "Aws Advanced Config",
      "type": "object"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "database_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for databases to filter in ingestion."
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
      "description": "regex patterns for tables to filter in ingestion."
    },
    "extract_feature_groups": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Whether to extract feature groups.",
      "title": "Extract Feature Groups"
    },
    "extract_models": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Whether to extract models.",
      "title": "Extract Models"
    },
    "extract_jobs": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Whether to extract AutoML jobs.",
      "title": "Extract Jobs"
    }
  },
  "title": "SagemakerSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.aws.sagemaker.SagemakerSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/aws/sagemaker.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for SageMaker, feel free to ping us on [our Slack](https://datahub.com/slack).
