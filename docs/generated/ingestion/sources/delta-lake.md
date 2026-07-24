


# Delta Lake

## Overview

Delta Lake is a data platform used to store and query analytical or operational data. Learn more in the [official Delta Lake documentation](https://delta.io/).

The DataHub integration for Delta Lake covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures tags and stateful deletion detection.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `delta-lake`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Folder. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Tags | ✅ | Can extract S3 object/bucket tags if enabled. |
| [Operation Capture](../../../api/tutorials/operations.md) | ✅ | Enabled by default from Delta table history. |

### Overview

The `delta-lake` module ingests metadata from Delta Lake into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.


### Install the Plugin
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

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">base_path</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to table (s3 or local file system). If path is not a delta table path then all subfolders will be scanned to detect and ingest delta tables.  |
| <div className="path-line"><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The platform that this source connects to <div className="default-line default-line-with-docs">Default: <span className="default-value">delta-lake</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">relative_path</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | If set, delta-tables will be searched at location '<base_path>/<relative_path>' and URNs will be created using relative_path only. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">require_files</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether DeltaTable should track files. Consider setting this to `False` for large delta tables, resulting in significant memory reduction for ingestion process.When set to `False`, number_of_files in delta table can not be reported. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">version_history_lookback</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number of previous version histories to be ingested. Defaults to 1. If set to -1 all version history will be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">1</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">azure</span></div> <div className="type-name-line"><span className="type-name">One of AzureBlob, null</span></div> | Azure configuration for `abfss://`, `abfs://`, `az://`, `adl://`, and Azure HTTPS paths. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.</span><span className="path-main">account_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Azure storage account key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.</span><span className="path-main">account_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Azure storage account name. Required for `az://` and `adl://` style paths. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.</span><span className="path-main">client_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Azure service principal client id. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.</span><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Azure service principal client secret. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.</span><span className="path-main">sas_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Azure shared access signature (SAS) token. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.</span><span className="path-main">tenant_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Azure service principal tenant id. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.</span><span className="path-main">credential</span></div> <div className="type-name-line"><span className="type-name">One of AzureCredentialConfig, null</span></div> | Unified Azure credential configuration. Mutually exclusive with `account_key`, `sas_token`, and service principal fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.credential.</span><span className="path-main">authentication_method</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "default", "service_principal", "managed_identity", "cli"  |
| <div className="path-line"><span className="path-prefix">azure.credential.</span><span className="path-main">client_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Azure Application (client) ID. Required for service_principal authentication. Find this in Azure Portal > App registrations > Your app > Overview. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.credential.</span><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Azure client secret. Required for service_principal authentication. Create in Azure Portal > App registrations > Your app > Certificates & secrets. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.credential.</span><span className="path-main">exclude_cli_credential</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When using 'default' authentication, exclude Azure CLI credential. Useful in production to avoid accidentally using developer credentials. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">azure.credential.</span><span className="path-main">exclude_environment_credential</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When using 'default' authentication, exclude environment variables. Environment variables checked: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">azure.credential.</span><span className="path-main">exclude_managed_identity_credential</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When using 'default' authentication, exclude managed identity. Useful during local development when managed identity is not available. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">azure.credential.</span><span className="path-main">managed_identity_client_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Client ID for user-assigned managed identity. Leave empty to use system-assigned managed identity. Only used when authentication_method is 'managed_identity'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">azure.credential.</span><span className="path-main">tenant_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Azure tenant (directory) ID. Required for service_principal authentication. Find this in Azure Portal > Microsoft Entra ID > Overview. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">s3</span></div> <div className="type-name-line"><span className="type-name">One of S3, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.</span><span className="path-main">use_s3_bucket_tags</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether or not to create tags in datahub from the s3 bucket <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">s3.</span><span className="path-main">use_s3_object_tags</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | # Whether or not to create tags in datahub from the s3 object <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">s3.</span><span className="path-main">aws_config</span></div> <div className="type-name-line"><span className="type-name">One of AwsConnectionConfig, null</span></div> | AWS configuration <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_advanced_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).  |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS region code. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_retry_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "legacy", "standard", "adaptive" <div className="default-line default-line-with-docs">Default: <span className="default-value">standard</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_retry_num</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">read_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | The timeout for reading from the connection (in seconds). <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.</span><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, array, null</span></div> | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">s3.aws_config.aws_role.</span><span className="path-main">union</span></div> <div className="type-name-line"><span className="type-name">One of string, AwsAssumeRoleConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">s3.aws_config.aws_role.union.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if union is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.  |
| <div className="path-line"><span className="path-prefix">s3.aws_config.aws_role.union.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | External ID to use when assuming the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful Ingestion Config with stale metadata removal <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "AwsConnectionConfig": {
      "additionalProperties": false,
      "description": "Common AWS credentials config.\n\nCurrently used by:\n    - Glue source\n    - SageMaker source\n    - dbt source",
      "properties": {
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
              "format": "password",
              "type": "string",
              "writeOnly": true
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
              "format": "password",
              "type": "string",
              "writeOnly": true
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
        }
      },
      "title": "AwsConnectionConfig",
      "type": "object"
    },
    "AzureAuthenticationMethod": {
      "description": "Supported Azure authentication methods.\n\n- DEFAULT: Uses DefaultAzureCredential which auto-detects credentials from\n  environment variables, managed identity, Azure CLI, etc.\n- SERVICE_PRINCIPAL: Uses client ID, client secret, and tenant ID\n- MANAGED_IDENTITY: Uses Azure Managed Identity (system or user-assigned)\n- CLI: Uses Azure CLI credential (requires `az login`)",
      "enum": [
        "default",
        "service_principal",
        "managed_identity",
        "cli"
      ],
      "title": "AzureAuthenticationMethod",
      "type": "string"
    },
    "AzureBlob": {
      "additionalProperties": false,
      "properties": {
        "account_name": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Azure storage account name. Required for `az://` and `adl://` style paths.",
          "title": "Account Name"
        },
        "account_key": {
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
          "description": "Azure storage account key.",
          "title": "Account Key"
        },
        "sas_token": {
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
          "description": "Azure shared access signature (SAS) token.",
          "title": "Sas Token"
        },
        "client_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Azure service principal client id.",
          "title": "Client Id"
        },
        "client_secret": {
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
          "description": "Azure service principal client secret.",
          "title": "Client Secret"
        },
        "tenant_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Azure service principal tenant id.",
          "title": "Tenant Id"
        },
        "credential": {
          "anyOf": [
            {
              "$ref": "#/$defs/AzureCredentialConfig"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Unified Azure credential configuration. Mutually exclusive with `account_key`, `sas_token`, and service principal fields."
        }
      },
      "title": "AzureBlob",
      "type": "object"
    },
    "AzureCredentialConfig": {
      "additionalProperties": false,
      "description": "Unified Azure authentication configuration.\n\nThis class provides a reusable authentication configuration that can be\ncomposed into any Azure connector's configuration. It supports multiple\nauthentication methods and returns a TokenCredential that works with\nany Azure SDK client.\n\nExample usage in a connector config:\n    class MyAzureConnectorConfig(ConfigModel):\n        credential: AzureCredentialConfig = Field(\n            default_factory=AzureCredentialConfig,\n            description=\"Azure authentication configuration\"\n        )\n        subscription_id: str = Field(...)",
      "properties": {
        "authentication_method": {
          "$ref": "#/$defs/AzureAuthenticationMethod",
          "default": "default",
          "description": "Authentication method to use. Options: 'default' (auto-detects from environment), 'service_principal' (client ID + secret + tenant), 'managed_identity' (Azure Managed Identity), 'cli' (Azure CLI credential). Recommended: Use 'default' which tries multiple methods automatically."
        },
        "client_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Azure Application (client) ID. Required for service_principal authentication. Find this in Azure Portal > App registrations > Your app > Overview.",
          "title": "Client Id"
        },
        "client_secret": {
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
          "description": "Azure client secret. Required for service_principal authentication. Create in Azure Portal > App registrations > Your app > Certificates & secrets.",
          "title": "Client Secret"
        },
        "tenant_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Azure tenant (directory) ID. Required for service_principal authentication. Find this in Azure Portal > Microsoft Entra ID > Overview.",
          "title": "Tenant Id"
        },
        "managed_identity_client_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Client ID for user-assigned managed identity. Leave empty to use system-assigned managed identity. Only used when authentication_method is 'managed_identity'.",
          "title": "Managed Identity Client Id"
        },
        "exclude_cli_credential": {
          "default": false,
          "description": "When using 'default' authentication, exclude Azure CLI credential. Useful in production to avoid accidentally using developer credentials.",
          "title": "Exclude Cli Credential",
          "type": "boolean"
        },
        "exclude_environment_credential": {
          "default": false,
          "description": "When using 'default' authentication, exclude environment variables. Environment variables checked: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID.",
          "title": "Exclude Environment Credential",
          "type": "boolean"
        },
        "exclude_managed_identity_credential": {
          "default": false,
          "description": "When using 'default' authentication, exclude managed identity. Useful during local development when managed identity is not available.",
          "title": "Exclude Managed Identity Credential",
          "type": "boolean"
        }
      },
      "title": "AzureCredentialConfig",
      "type": "object"
    },
    "S3": {
      "additionalProperties": false,
      "properties": {
        "aws_config": {
          "anyOf": [
            {
              "$ref": "#/$defs/AwsConnectionConfig"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS configuration"
        },
        "use_s3_bucket_tags": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": false,
          "description": "Whether or not to create tags in datahub from the s3 bucket",
          "title": "Use S3 Bucket Tags"
        },
        "use_s3_object_tags": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": false,
          "description": "# Whether or not to create tags in datahub from the s3 object",
          "title": "Use S3 Object Tags"
        }
      },
      "title": "S3",
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
      "default": null,
      "description": "Stateful Ingestion Config with stale metadata removal"
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
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "title": "Platform Instance"
    },
    "base_path": {
      "description": "Path to table (s3 or local file system). If path is not a delta table path then all subfolders will be scanned to detect and ingest delta tables.",
      "title": "Base Path",
      "type": "string"
    },
    "relative_path": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "If set, delta-tables will be searched at location '<base_path>/<relative_path>' and URNs will be created using relative_path only.",
      "title": "Relative Path"
    },
    "platform": {
      "const": "delta-lake",
      "default": "delta-lake",
      "description": "The platform that this source connects to",
      "title": "Platform",
      "type": "string"
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
    "version_history_lookback": {
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": 1,
      "description": "Number of previous version histories to be ingested. Defaults to 1. If set to -1 all version history will be ingested.",
      "title": "Version History Lookback"
    },
    "require_files": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Whether DeltaTable should track files. Consider setting this to `False` for large delta tables, resulting in significant memory reduction for ingestion process.When set to `False`, number_of_files in delta table can not be reported.",
      "title": "Require Files"
    },
    "s3": {
      "anyOf": [
        {
          "$ref": "#/$defs/S3"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "azure": {
      "anyOf": [
        {
          "$ref": "#/$defs/AzureBlob"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Azure configuration for `abfss://`, `abfs://`, `az://`, `adl://`, and Azure HTTPS paths."
    }
  },
  "required": [
    "base_path"
  ],
  "title": "DeltaLakeSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Delta Table on Local File System

##### Step 1

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

##### Step 2

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

##### Step 3

Execute the ingestion recipe:

```shell
datahub ingest -c delta.dhub.yaml
```

#### Delta Table on S3

##### Step 1

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

##### Step 3

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

##### Step 4

Execute the ingestion recipe:

```shell
datahub ingest -c delta.s3.dhub.yaml
```

##### Required IAM Permissions

The IAM principal used by the source needs the following S3 actions:

| Action                | Required                              | Purpose                                                                                                                    |
| --------------------- | ------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `s3:ListBucket`       | Always                                | Auto-discovering table folders under `base_path` and listing entries under `_delta_log/` to find transaction log versions. |
| `s3:GetObject`        | Always                                | Reading `_delta_log/*.json` and checkpoint parquet files to extract schema and table properties.                           |
| `s3:GetBucketTagging` | Only if `s3.use_s3_bucket_tags: true` | Fetching bucket tags to attach to the dataset.                                                                             |
| `s3:GetObjectTagging` | Only if `s3.use_s3_object_tags: true` | Fetching object tags to attach to the dataset.                                                                             |

If the bucket is encrypted with a customer-managed KMS key, the principal also needs `kms:Decrypt` on that key.

A minimal policy scoped to a single bucket and prefix looks like this:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::your-bucket",
      "Condition": { "StringLike": { "s3:prefix": ["your/delta/prefix/*"] } }
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::your-bucket/your/delta/prefix/*"
    }
  ]
}
```

#### Delta Table on Azure Data Lake Storage (ADLS Gen2 / Blob)

You can ingest Delta tables stored in Azure using `abfss://`, `abfs://`, `az://`, `adl://`, or Azure HTTPS paths.

Azure folder discovery reuses shared Azure Blob helpers from the Azure ingestion module.
As a result, recursive folder scanning requires static credentials in `source.config.azure`
(`account_key`, `sas_token`, or `client_id` + `client_secret` + `tenant_id`).

#### Example using account key

```yaml
source:
  type: "delta-lake"
  config:
    base_path: "abfss://my-container@myaccount.dfs.core.windows.net/delta/sales"
    azure:
      account_key: ${AZURE_STORAGE_ACCOUNT_KEY}

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

##### Example using service principal

```yaml
source:
  type: "delta-lake"
  config:
    base_path: "az://my-container/delta/sales"
    azure:
      account_name: "myaccount"
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

If you use `az://` or `adl://` URIs, set `azure.account_name` explicitly.

If you use `azure.credential` (unified token-based auth), use `base_path` that points directly
to a Delta table. Token-based credential is not used for recursive folder listing.

#### Assigning domains with a transformer

Delta Lake source does not provide connector-specific `domain` config.
To assign domains, use the dataset transformer `simple_add_dataset_domain`.

```yaml
source:
  type: "delta-lake"
  config:
    base_path: "abfss://my-container@myaccount.dfs.core.windows.net/delta/sales"

transformers:
  - type: "simple_add_dataset_domain"
    config:
      domains:
        - ${DATAHUB_DOMAIN_URN}
```

For additional options (`PATCH`, `replace_existing`, multiple domains), see
[Simple Add Dataset Domains](../../../../metadata-ingestion/docs/transformer/dataset_transformer.md#simple-add-dataset-domains).

### Limitations

The above recipes are minimal recipes. Please refer to [Config Details](#config-details) section for the full configuration.
Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.delta_lake.source.DeltaLakeSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/delta_lake/source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Delta Lake, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
