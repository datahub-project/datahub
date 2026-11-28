


# Elasticsearch

## Overview

Elasticsearch is a DataHub utility or metadata-focused integration.

The DataHub integration for Elasticsearch covers metadata entities and operational objects relevant to this connector. It also captures stateful deletion detection.

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `elasticsearch`
![GA](https://img.shields.io/badge/support%20status-GA-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |

### Overview

The `elasticsearch` module ingests metadata from Elasticsearch into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Metadata for indexes
- Column types associated with each index field

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Authentication

The connector supports one of the following authentication methods per recipe:

- **Basic authentication**: set `username` and `password`.
- **API key**: set `api_key` to an `[id, api_key]` pair or a base64-encoded `id:api_key` string.
- **AWS IAM (SigV4)**: for [Amazon OpenSearch Service](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/what-is.html) domains, set `aws_config`. Every request is signed with AWS Signature Version 4.

AWS IAM authentication cannot be combined with basic authentication or an API key. `aws_config.aws_region` is required and must match the domain's region. Credentials resolve from one of:

- explicit `aws_access_key_id`/`aws_secret_access_key`,
- an `aws_profile`,
- an `aws_role` to assume, using base credentials from the default AWS credential chain (environment variables, `AWS_PROFILE`, instance profile, ECS task role). `aws_role` cannot be combined with `aws_profile` or explicit keys, because the role would not be assumed; to assume a role from a profile, set `AWS_PROFILE` in the environment and configure only `aws_role`,
- or the default AWS credential chain when none of the above is set.

Assumed-role credentials are renewed automatically before they expire, so long-running ingestion does not need special handling.

Amazon OpenSearch Service domains only accept HTTPS, and signed requests must only go to a verified domain certificate, so `aws_config` requires `use_ssl: True` and `verify_certs: True` (use `ca_certs` for a custom certificate authority). Point `host` at the domain endpoint on port 443, for example `search-my-domain-abc123.us-east-1.es.amazonaws.com:443`.

The IAM principal needs `es:ESHttpGet` on the domain (for example `arn:aws:es:us-east-1:123456789012:domain/my-domain/*`); the connector only issues read requests. If the domain uses fine-grained access control, also map the principal (as a backend role) to an OpenSearch role with these permissions:

- Cluster permissions: `cluster_monitor`, `indices:admin/template/get`, `indices:admin/index_template/get` (template reads are cluster-level actions in OpenSearch security)
- Index permissions on the indices to ingest: `indices_monitor`, `indices:admin/get`, `indices:admin/aliases/get`

OpenSearch Serverless collections are not supported.


### Install the Plugin
```shell
pip install 'acryl-datahub[elasticsearch]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: "elasticsearch"
  config:
    # Coordinates
    host: 'localhost:9200'

    # Credentials: use one of basic auth, api_key, or aws_config
    username: user # optional
    password: pass # optional
    # api_key: ["id", "api_key"] # or a base64-encoded "id:api_key" string

    # AWS IAM (SigV4) auth for Amazon OpenSearch Service domains.
    # Replaces username/password and api_key. Credentials resolve from explicit keys,
    # aws_profile, aws_role, or the default AWS credential chain.
    # Requires use_ssl: True and verify_certs: True below; use host '<domain-endpoint>:443'.
    # aws_role is assumed from the default credential chain and cannot be combined
    # with aws_profile or explicit keys.
    # aws_config:
    #   aws_region: us-east-1
    #   aws_role: "arn:aws:iam::123456789012:role/DataHubOpenSearchRead" # optional

    # SSL support (use_ssl and verify_certs must be True when using aws_config)
    use_ssl: False
    verify_certs: False
    ca_certs: "./path/ca.cert"
    client_cert: "./path/client.cert"
    client_key: "./path/client.key"
    ssl_assert_hostname: False
    ssl_assert_fingerprint: "./path/cert.fingerprint"

    # Options
    url_prefix: "" # optional url_prefix
    env: "PROD"
    index_pattern:
      allow: [".*some_index_name_pattern*"]
      deny: [".*skip_index_name_pattern*"]
    ingest_index_templates: False
    index_template_pattern:
      allow: [".*some_index_template_name_pattern*"]

sink:
# sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">ca_certs</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to a certificate authority (CA) certificate. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">client_cert</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to the file containing the private key and the certificate, or cert only if using client_key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">client_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Path to the file containing the private key if using separate cert and key files. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">host</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The elastic search host URI. <div className="default-line default-line-with-docs">Default: <span className="default-value">localhost:9200</span></div> |
| <div className="path-line"><span className="path-main">ingest_index_templates</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingests ES index templates if enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | The password credential. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">ssl_assert_fingerprint</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Verify the supplied certificate fingerprint if not None. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">ssl_assert_hostname</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use hostname verification if not False. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">url_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">use_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to use SSL for the connection or not. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The username credential. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">verify_certs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to verify SSL certificates. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">One of array, string, null</span></div> | API Key authentication. Accepts either a list with id and api_key (UTF-8 representation), or a base64 encoded string of id and api_key combined by ':'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">api_key.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">aws_config</span></div> <div className="type-name-line"><span className="type-name">One of AwsConnectionConfig, null</span></div> | AWS connection settings for IAM authentication against an Amazon OpenSearch Service domain. When set, every request is signed with AWS Signature Version 4 using the resolved credentials (explicit keys, profile, role assumption, or the default credential chain). aws_region, use_ssl: True and verify_certs: True are required. Cannot be combined with username/password or api_key, and aws_role cannot be combined with aws_profile or explicit keys. OpenSearch Serverless collections are not supported. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_advanced_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html). `max_pool_connections` defaults to 25 rather than botocore's 10; raise it when more threads than that share one client, e.g. dbt `artifact_read_concurrency` above 25.  |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS region code. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_retry_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "legacy", "standard", "adaptive" <div className="default-line default-line-with-docs">Default: <span className="default-value">standard</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_retry_num</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">read_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | The timeout for reading from the connection (in seconds). <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, array, null</span></div> | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.aws_role.</span><span className="path-main">union</span></div> <div className="type-name-line"><span className="type-name">One of string, AwsAssumeRoleConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">aws_config.aws_role.union.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if union is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.  |
| <div className="path-line"><span className="path-prefix">aws_config.aws_role.union.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | External ID to use when assuming the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">collapse_urns</span></div> <div className="type-name-line"><span className="type-name">CollapseUrns</span></div> |   |
| <div className="path-line"><span className="path-prefix">collapse_urns.</span><span className="path-main">urns_suffix_regex</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to remove from the name of the URN. All of the indices before removal of URNs are considered as the same dataset. These are applied in order for each URN. <br />         The main case where you would want to have multiple of these if the name where you are trying to remove suffix from have different formats. <br />         e.g. ending with -YYYY-MM-DD as well as ending -epochtime would require you to have 2 regex patterns to remove the suffixes across all URNs.  |
| <div className="path-line"><span className="path-prefix">collapse_urns.urns_suffix_regex.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">index_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">index_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">index_template_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">index_template_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">ElasticProfiling</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to enable profiling for the elastic search source. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulIngestionConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

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
          "description": "Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html). `max_pool_connections` defaults to 25 rather than botocore's 10; raise it when more threads than that share one client, e.g. dbt `artifact_read_concurrency` above 25.",
          "title": "Aws Advanced Config",
          "type": "object"
        }
      },
      "title": "AwsConnectionConfig",
      "type": "object"
    },
    "CollapseUrns": {
      "additionalProperties": false,
      "properties": {
        "urns_suffix_regex": {
          "description": "List of regex patterns to remove from the name of the URN. All of the indices before removal of URNs are considered as the same dataset. These are applied in order for each URN.\n        The main case where you would want to have multiple of these if the name where you are trying to remove suffix from have different formats.\n        e.g. ending with -YYYY-MM-DD as well as ending -epochtime would require you to have 2 regex patterns to remove the suffixes across all URNs.",
          "items": {
            "type": "string"
          },
          "title": "Urns Suffix Regex",
          "type": "array"
        }
      },
      "title": "CollapseUrns",
      "type": "object"
    },
    "ElasticProfiling": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether to enable profiling for the elastic search source.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        }
      },
      "title": "ElasticProfiling",
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
    "StatefulIngestionConfig": {
      "additionalProperties": false,
      "description": "Basic Stateful Ingestion Specific Configuration for any source.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        }
      },
      "title": "StatefulIngestionConfig",
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
          "$ref": "#/$defs/StatefulIngestionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful Ingestion Config"
    },
    "host": {
      "default": "localhost:9200",
      "description": "The elastic search host URI.",
      "title": "Host",
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
      "description": "The username credential.",
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
      "description": "The password credential.",
      "title": "Password"
    },
    "api_key": {
      "anyOf": [
        {
          "maxItems": 2,
          "minItems": 2,
          "prefixItems": [
            {
              "type": "string"
            },
            {
              "type": "string"
            }
          ],
          "type": "array"
        },
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "API Key authentication. Accepts either a list with id and api_key (UTF-8 representation), or a base64 encoded string of id and api_key combined by ':'.",
      "title": "Api Key"
    },
    "use_ssl": {
      "default": false,
      "description": "Whether to use SSL for the connection or not.",
      "title": "Use Ssl",
      "type": "boolean"
    },
    "verify_certs": {
      "default": false,
      "description": "Whether to verify SSL certificates.",
      "title": "Verify Certs",
      "type": "boolean"
    },
    "ca_certs": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to a certificate authority (CA) certificate.",
      "title": "Ca Certs"
    },
    "client_cert": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to the file containing the private key and the certificate, or cert only if using client_key.",
      "title": "Client Cert"
    },
    "client_key": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Path to the file containing the private key if using separate cert and key files.",
      "title": "Client Key"
    },
    "ssl_assert_hostname": {
      "default": false,
      "description": "Use hostname verification if not False.",
      "title": "Ssl Assert Hostname",
      "type": "boolean"
    },
    "ssl_assert_fingerprint": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Verify the supplied certificate fingerprint if not None.",
      "title": "Ssl Assert Fingerprint"
    },
    "url_prefix": {
      "default": "",
      "description": "There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters.",
      "title": "Url Prefix",
      "type": "string"
    },
    "index_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "^_.*",
          "^ilm-history.*"
        ],
        "ignoreCase": true
      },
      "description": "regex patterns for indexes to filter in ingestion."
    },
    "ingest_index_templates": {
      "default": false,
      "description": "Ingests ES index templates if enabled.",
      "title": "Ingest Index Templates",
      "type": "boolean"
    },
    "index_template_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "^_.*"
        ],
        "ignoreCase": true
      },
      "description": "The regex patterns for filtering index templates to ingest."
    },
    "profiling": {
      "$ref": "#/$defs/ElasticProfiling",
      "description": "Configs to ingest data profiles from ElasticSearch."
    },
    "collapse_urns": {
      "$ref": "#/$defs/CollapseUrns",
      "description": "List of regex patterns to remove from the name of the URN. All of the indices before removal of URNs are considered as the same dataset. These are applied in order for each URN.\n        The main case where you would want to have multiple of these if the name where you are trying to remove suffix from have different formats.\n        e.g. ending with -YYYY-MM-DD as well as ending -epochtime would require you to have 2 regex patterns to remove the suffixes across all URNs."
    },
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
      "description": "AWS connection settings for IAM authentication against an Amazon OpenSearch Service domain. When set, every request is signed with AWS Signature Version 4 using the resolved credentials (explicit keys, profile, role assumption, or the default credential chain). aws_region, use_ssl: True and verify_certs: True are required. Cannot be combined with username/password or api_key, and aws_role cannot be combined with aws_profile or explicit keys. OpenSearch Serverless collections are not supported."
    }
  },
  "title": "ElasticsearchSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.elastic_search.ElasticsearchSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/elastic_search.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Elasticsearch, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
