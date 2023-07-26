---
sidebar_position: 43
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

| Capability          | Status | Notes              |
| ------------------- | ------ | ------------------ |
| Table-Level Lineage | ✅     | Enabled by default |

This plugin extracts the following:

- Feature groups
- Models, jobs, and lineage between the two (e.g. when jobs output a model or a model is used by a job)

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[sagemaker]'
```

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

| Field                                                                                                                                                                                                                                                              | Description                                                                                                                                                                                                                                                                                                                                               |
| :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">aws_region</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                               | AWS region code.                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                              | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                               | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                    | Named AWS profile to use. Only used if access key / secret are unset. If not set the default will be used                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                                                                             |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                          | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                              | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">extract_feature_groups</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                        | Whether to extract feature groups. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">extract_jobs</span></div> <div className="type-name-line"><span className="type-name">One of string, boolean</span></div>                                                                                   | Whether to extract AutoML jobs. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">extract_models</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                | Whether to extract models. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                            | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, union(anyOf), string, AwsAssumeRoleConfig</span></div>                                                     | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are documented at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role |
| <div className="path-line"><span className="path-prefix">aws_role.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if aws_role is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">aws_role.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | External ID to use when assuming the role.                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">database_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                     | regex patterns for databases to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                      |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                             |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                              |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                              | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                        | regex patterns for tables to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                         |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                 |                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                 | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                               |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "SagemakerSourceConfig",
  "description": "Common AWS credentials config.\n\nCurrently used by:\n    - Glue source\n    - SageMaker source",
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
    "extract_feature_groups": {
      "title": "Extract Feature Groups",
      "description": "Whether to extract feature groups.",
      "default": true,
      "type": "boolean"
    },
    "extract_models": {
      "title": "Extract Models",
      "description": "Whether to extract models.",
      "default": true,
      "type": "boolean"
    },
    "extract_jobs": {
      "title": "Extract Jobs",
      "description": "Whether to extract AutoML jobs.",
      "default": true,
      "anyOf": [
        {
          "type": "object",
          "additionalProperties": {
            "type": "string"
          }
        },
        {
          "type": "boolean"
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
    }
  }
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.aws.sagemaker.SagemakerSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/aws/sagemaker.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for SageMaker, feel free to ping us on [our Slack](https://slack.datahubproject.io).
