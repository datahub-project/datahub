


# Amazon SQS

## Overview

[Amazon SQS](https://aws.amazon.com/sqs/) is AWS's managed message queue service. Producers send messages to a queue, consumers poll it, and messages that keep failing move to a dead-letter queue.

The `sqs` source plugin catalogs the queues of one AWS region as datasets, grouped in a region container, with queue settings as properties, AWS resource tags as DataHub tags, lineage from each queue to its dead-letter queue, and stateful deletion. Its companion, the [`sns` source](sns.md), catalogs SNS topics and adds topic-to-queue lineage; both build URNs the same way so their lineage joins.

## Concept Mapping

| Source Concept                            | DataHub Concept                                                                                   | Notes                                                                                 |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| AWS account                               | [Platform Instance](../../../platform-instances.md)                                               | Defaults to the account id read from a queue ARN                                      |
| AWS region                                | [Container](../../metamodel/entities/container.md)                                                | Subtype `Region`                                                                      |
| Queue                                     | [Dataset](../../metamodel/entities/dataset.md)                                                    | Platform `sqs`, subtype `Queue`; name `<region>.<queue name>`                         |
| Redrive policy                            | Upstream lineage (patch)                                                                          | Queue to its dead-letter queue                                                        |
| Lambda function wired to a queue          | [DataFlow](../../metamodel/entities/dataFlow.md) + [DataJob](../../metamodel/entities/dataJob.md) | Platform `lambda`; one `invoke` job per function                                      |
| EventBridge rule or Pipe touching a queue | [DataFlow](../../metamodel/entities/dataFlow.md) + [DataJob](../../metamodel/entities/dataJob.md) | Platforms `eventbridge` and `eventbridge-pipes`; event buses are `Event Bus` datasets |
| Resource tag                              | [Tag](../../metamodel/entities/tag.md)                                                            | `Key:Value`, or `Key` for an empty value                                              |


## Module `sqs`
![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Region containers. |
| Column-level Lineage | ✅ | Queue to dead-letter queue, when both have schemas. |
| Descriptions | ❌ | SQS has no description. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| Extract Tags | ✅ | From AWS resource tags, added without removing UI tags. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Defaults to the AWS account id. |
| Schema Metadata | ✅ | Opt-in: AWS Glue Schema Registry schemas listed in `glue_schema_registry.queue_schema_map`, or found by `use_naming_convention`. |
| Table-Level Lineage | ✅ | Dead-letter queues, Lambda triggers and destinations, EventBridge rules and Pipes. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `sqs` module reads the SQS control-plane APIs for one AWS region per recipe. It never receives or deletes messages, so running it has no effect on consumers.

SQS has no schema of its own; queue schemas can be attached from AWS Glue Schema Registry (opt-in). Queue settings (FIFO, visibility timeout, retention, encryption, dead-letter target, maximum receive count) are stored as custom properties, and the creation and last-modified times on the dataset properties.

SNS topics are ingested by the separate `sns` source. Run both with the same `env`, `platform_instance` and `account_platform_instance_map` so that topic-to-queue lineage written by `sns` lands on the queues this source emits.

### Prerequisites

#### AWS IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SqsRead",
      "Effect": "Allow",
      "Action": [
        "sqs:ListQueues",
        "sqs:GetQueueAttributes",
        "sqs:ListQueueTags"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GlueSchemaRegistryRead",
      "Effect": "Allow",
      "Action": ["glue:GetSchemaVersion"],
      "Resource": [
        "arn:aws:glue:<region>:<account-id>:registry/<registry-name>",
        "arn:aws:glue:<region>:<account-id>:schema/<registry-name>/*"
      ]
    },
    {
      "Sid": "LambdaRead",
      "Effect": "Allow",
      "Action": ["lambda:ListEventSourceMappings"],
      "Resource": "*"
    },
    {
      "Sid": "LambdaAsyncDestinationsRead",
      "Effect": "Allow",
      "Action": [
        "lambda:ListFunctions",
        "lambda:ListFunctionEventInvokeConfigs"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EventBridgeList",
      "Effect": "Allow",
      "Action": [
        "events:ListEventBuses",
        "events:ListRules",
        "pipes:ListPipes"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EventBridgeRead",
      "Effect": "Allow",
      "Action": ["events:ListTargetsByRule"],
      "Resource": "arn:aws:events:<region>:<account-id>:rule/*"
    },
    {
      "Sid": "PipesRead",
      "Effect": "Allow",
      "Action": ["pipes:DescribePipe"],
      "Resource": "arn:aws:pipes:<region>:<account-id>:pipe/*"
    },
    {
      "Sid": "S3ListBuckets",
      "Effect": "Allow",
      "Action": ["s3:ListAllMyBuckets"],
      "Resource": "*"
    },
    {
      "Sid": "S3NotificationsRead",
      "Effect": "Allow",
      "Action": ["s3:GetBucketNotification"],
      "Resource": "arn:aws:s3:::*"
    },
    {
      "Sid": "CloudTrailLakeQuery",
      "Effect": "Allow",
      "Action": [
        "cloudtrail:StartQuery",
        "cloudtrail:GetQueryResults",
        "cloudtrail:CancelQuery"
      ],
      "Resource": "arn:aws:cloudtrail:<region>:<account-id>:eventdatastore/<event-data-store-id>"
    },
    {
      "Sid": "TagsRead",
      "Effect": "Allow",
      "Action": ["tag:GetResources"],
      "Resource": "*"
    }
  ]
}
```

With `extract_tags` on, the connector reads every queue's tags with one paginated `tag:GetResources` call. If that call is denied it warns and falls back to `sqs:ListQueueTags` per queue, so either permission works.

#### Authentication

Credentials come from `aws_config` or the standard boto3 chain: environment variables, a shared credentials profile, an IAM role on the host, or SSO. `aws_config.aws_role` assumes a role, including one in another account.

`glue:GetSchemaVersion` is only needed with `glue_schema_registry.enabled`.

`LambdaRead` is used with `lineage.lambda_functions`, and `LambdaAsyncDestinationsRead` only with `lineage.lambda_async_destinations` too; leave it out otherwise. `EventBridgeList` is used with `lineage.eventbridge_rules` or `lineage.eventbridge_pipes`, `EventBridgeRead` with `lineage.eventbridge_rules`, and `PipesRead` with `lineage.eventbridge_pipes`. The `ListEventBuses`, `ListRules` and `ListPipes` actions don't support resource-level scoping, so they need `*`. `pipes:DescribePipe` is only called for a pipe that could get column lineage. `lineage.lambda_async_destinations` is off by default because it makes one `lambda:ListFunctionEventInvokeConfigs` call per function in the region, from both sources; it stops at the first `AccessDenied`, and so does the `events:ListTargetsByRule` loop.

`S3ListBuckets` and `S3NotificationsRead` are used only with `lineage.s3_notifications`, which is off by default because it makes one `s3:GetBucketNotification` call per bucket in the recipe's region. `s3:ListAllMyBuckets` only returns buckets the calling account owns, so buckets in other accounts are not read. To read only some buckets, list their ARNs (`arn:aws:s3:::my-bucket`) in `S3NotificationsRead`; a bucket whose notifications are denied is skipped with a warning and keeps its earlier edges.

`CloudTrailLakeQuery` is used only with `cloudtrail_producers.enabled`; scope it to the store in `cloudtrail_producers.event_data_store`. `cloudtrail:CancelQuery` stops a query the run gave up on, which would otherwise keep scanning. Matching a caller to a Lambda function also needs `lambda:ListFunctions`, so with `cloudtrail_producers.enabled` grant that action even when `LambdaAsyncDestinationsRead` is otherwise left out.


### Install the Plugin
```shell
pip install 'acryl-datahub[sqs]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: sqs
  config:
    aws_config:
      aws_region: "us-east-1"
      # aws_role: "arn:aws:iam::123456789012:role/datahub-read"
    # platform_instance defaults to the AWS account id. Use the same env and
    # platform_instance in the sns recipe so topic-to-queue lineage joins.
    env: "PROD"

    queue_pattern:
      deny:
        - ".*-tmp$"

    extract_tags: true

    # domain:
    #   "urn:li:domain:payments":
    #     allow:
    #       - "orders.*"

    # Platform instances used by other accounts' recipes, for cross-account lineage.
    # account_platform_instance_map:
    #   "210987654321": "shared-services"

    glue_schema_registry:
      enabled: false
      registry_name: "default-registry"
      queue_schema_map: {}
      #   orders: "order-v1"

    lineage:
      lambda_functions: true
      # One lambda:ListFunctionEventInvokeConfigs call per function in the region.
      lambda_async_destinations: false
      eventbridge_rules: true
      eventbridge_pipes: true
      s3_notifications: false
    # Match the s3 source recipe, for S3 notification lineage.
    # platform_map:
    #   s3:
    #     platform_instance: "data-lake"
    #     env: "PROD"
    #     convert_urns_to_lowercase: false

    cloudtrail_producers:
      enabled: false
      # event_data_store: "arn:aws:cloudtrail:us-east-1:123456789012:eventdatastore/EXAMPLE"
      # lookback_days: 7

    stateful_ingestion:
      enabled: true

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">account_platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">extract_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract AWS resource tags as DataHub tags. SNS allows 10 ListTagsForResource calls per second, so turn this off on the sns source for accounts with tens of thousands of topics. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Add field-level lineage on hops that deliver the message unchanged or with a simple field mapping, for fields present in both schemas. See the docs for which hops qualify. Schemas come from Glue Schema Registry in this run or from DataHub, one DataHub lookup per hop for a schema not read from Glue. Defaults to glue_schema_registry.enabled; set it to true to use schemas that are already in DataHub without Glue. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">remove_stale_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Remove lineage edges this source wrote on an earlier run that no longer exist in AWS, such as a removed redrive policy or subscription. Only edges from resources this run read in full are removed. Needs a DataHub graph connection (the datahub-rest sink or datahub_api); costs one DataHub lookup per queue or topic. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">aws_config</span></div> <div className="type-name-line"><span className="type-name">AwsConnectionConfig</span></div> | Common AWS credentials config. <br />  <br /> Currently used by: <br />     - Glue source <br />     - SageMaker source <br />     - dbt source  |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_advanced_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).  |
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
| <div className="path-line"><span className="path-main">cloudtrail_producers</span></div> <div className="type-name-line"><span className="type-name">CloudTrailProducersConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">cloudtrail_producers.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Find producers from CloudTrail Lake: who called SendMessage or Publish on this source's queues or topics. Needs data-event logging for SQS/SNS into an event data store. Lake queries are billed per GB scanned. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">cloudtrail_producers.</span><span className="path-main">event_data_store</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | ARN or id of the CloudTrail Lake event data store to query. With an ARN the query runs in the store's region; with a bare id, in this recipe's region. A store in another account must be shared with this role through its resource policy. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">cloudtrail_producers.</span><span className="path-main">lookback_days</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | How many days of events the producer query covers. <div className="default-line default-line-with-docs">Default: <span className="default-value">7</span></div> |
| <div className="path-line"><span className="path-prefix">cloudtrail_producers.</span><span className="path-main">query_timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | How long to wait for the CloudTrail Lake query before giving up. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">glue_schema_registry</span></div> <div className="type-name-line"><span className="type-name">QueueGlueSchemaRegistryConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">glue_schema_registry.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Attach schemas from AWS Glue Schema Registry. Needs glue:GetSchemaVersion. AWS stores no link between a queue or topic and a schema, so list them in the schema map. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">glue_schema_registry.</span><span className="path-main">queue_schema_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">glue_schema_registry.</span><span className="path-main">registry_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Glue Schema Registry registry to read schemas from. <div className="default-line default-line-with-docs">Default: <span className="default-value">default-registry</span></div> |
| <div className="path-line"><span className="path-prefix">glue_schema_registry.</span><span className="path-main">use_naming_convention</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When the schema map has no entry, look for a schema with the same name as the queue or topic. Off by default: AWS defines no such convention. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">lineage</span></div> <div className="type-name-line"><span className="type-name">AwsMessagingLineageConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">lineage.</span><span className="path-main">eventbridge_pipes</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit EventBridge Pipes whose source or target is one of this source's queues or topics. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">lineage.</span><span className="path-main">eventbridge_rules</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit EventBridge rules that target this source's queues or topics. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">lineage.</span><span className="path-main">lambda_async_destinations</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Read each function's asynchronous invocation destinations. One lambda:ListFunctionEventInvokeConfigs call per function in the region, made by both the sqs and sns sources; stops at the first AccessDenied. Stale Lambda output edges are only removed when this is on, since without it the run can't tell which outputs still exist. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">lineage.</span><span className="path-main">lambda_functions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit Lambda functions wired to this source's queues or topics (event source mappings, SNS subscriptions, failure and async destinations) as a DataFlow with one DataJob. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">lineage.</span><span className="path-main">s3_notifications</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Add S3 datasets as upstreams of the queues or topics their bucket's event notifications target: every S3 dataset already in DataHub under the notification's bucket and key prefix, up to 100. Reads the notification configuration of each bucket in the recipe's region (one call per bucket); a source only links its own queues or topics. Needs a DataHub graph connection, and s3:ListAllMyBuckets plus s3:GetBucketNotification. ListAllMyBuckets only returns buckets the recipe's account owns. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,LinkedPlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | platform_instance of that platform's own recipe. If unset, the dataset URN has no platform_instance. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">platform_map.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Set to that platform's recipe convert_urns_to_lowercase. Defaults to False, like the s3 source. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">platform_map.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | env of that platform's own recipe. If unset, inherits this recipe's env. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">queue_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">queue_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration for stale entity removal. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "AwsMessagingLineageConfig": {
      "additionalProperties": false,
      "properties": {
        "lambda_functions": {
          "default": true,
          "description": "Emit Lambda functions wired to this source's queues or topics (event source mappings, SNS subscriptions, failure and async destinations) as a DataFlow with one DataJob.",
          "title": "Lambda Functions",
          "type": "boolean"
        },
        "lambda_async_destinations": {
          "default": false,
          "description": "Read each function's asynchronous invocation destinations. One lambda:ListFunctionEventInvokeConfigs call per function in the region, made by both the sqs and sns sources; stops at the first AccessDenied. Stale Lambda output edges are only removed when this is on, since without it the run can't tell which outputs still exist.",
          "title": "Lambda Async Destinations",
          "type": "boolean"
        },
        "eventbridge_rules": {
          "default": true,
          "description": "Emit EventBridge rules that target this source's queues or topics.",
          "title": "Eventbridge Rules",
          "type": "boolean"
        },
        "eventbridge_pipes": {
          "default": true,
          "description": "Emit EventBridge Pipes whose source or target is one of this source's queues or topics.",
          "title": "Eventbridge Pipes",
          "type": "boolean"
        },
        "s3_notifications": {
          "default": false,
          "description": "Add S3 datasets as upstreams of the queues or topics their bucket's event notifications target: every S3 dataset already in DataHub under the notification's bucket and key prefix, up to 100. Reads the notification configuration of each bucket in the recipe's region (one call per bucket); a source only links its own queues or topics. Needs a DataHub graph connection, and s3:ListAllMyBuckets plus s3:GetBucketNotification. ListAllMyBuckets only returns buckets the recipe's account owns.",
          "title": "S3 Notifications",
          "type": "boolean"
        }
      },
      "title": "AwsMessagingLineageConfig",
      "type": "object"
    },
    "CloudTrailProducersConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Find producers from CloudTrail Lake: who called SendMessage or Publish on this source's queues or topics. Needs data-event logging for SQS/SNS into an event data store. Lake queries are billed per GB scanned.",
          "title": "Enabled",
          "type": "boolean"
        },
        "event_data_store": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "ARN or id of the CloudTrail Lake event data store to query. With an ARN the query runs in the store's region; with a bare id, in this recipe's region. A store in another account must be shared with this role through its resource policy.",
          "title": "Event Data Store"
        },
        "lookback_days": {
          "default": 7,
          "description": "How many days of events the producer query covers.",
          "minimum": 1,
          "title": "Lookback Days",
          "type": "integer"
        },
        "query_timeout_seconds": {
          "default": 300,
          "description": "How long to wait for the CloudTrail Lake query before giving up.",
          "minimum": 10,
          "title": "Query Timeout Seconds",
          "type": "integer"
        }
      },
      "title": "CloudTrailProducersConfig",
      "type": "object"
    },
    "LinkedPlatformDetail": {
      "additionalProperties": false,
      "properties": {
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
          "description": "platform_instance of that platform's own recipe. If unset, the dataset URN has no platform_instance.",
          "title": "Platform Instance"
        },
        "env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "env of that platform's own recipe. If unset, inherits this recipe's env.",
          "title": "Env"
        },
        "convert_urns_to_lowercase": {
          "default": false,
          "description": "Set to that platform's recipe convert_urns_to_lowercase. Defaults to False, like the s3 source.",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "LinkedPlatformDetail",
      "type": "object"
    },
    "QueueGlueSchemaRegistryConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Attach schemas from AWS Glue Schema Registry. Needs glue:GetSchemaVersion. AWS stores no link between a queue or topic and a schema, so list them in the schema map.",
          "title": "Enabled",
          "type": "boolean"
        },
        "registry_name": {
          "default": "default-registry",
          "description": "Glue Schema Registry registry to read schemas from.",
          "title": "Registry Name",
          "type": "string"
        },
        "use_naming_convention": {
          "default": false,
          "description": "When the schema map has no entry, look for a schema with the same name as the queue or topic. Off by default: AWS defines no such convention.",
          "title": "Use Naming Convention",
          "type": "boolean"
        },
        "queue_schema_map": {
          "additionalProperties": {
            "type": "string"
          },
          "description": "Queue name to schema name in `registry_name`.",
          "title": "Queue Schema Map",
          "type": "object"
        }
      },
      "title": "QueueGlueSchemaRegistryConfig",
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
      "description": "Stateful ingestion configuration for stale entity removal."
    },
    "aws_config": {
      "$ref": "#/$defs/AwsConnectionConfig",
      "description": "AWS connection config (credentials, region, role assumption, endpoint override). When omitted, boto3 resolves everything from the standard credential chain. One recipe covers one region."
    },
    "extract_tags": {
      "default": true,
      "description": "Extract AWS resource tags as DataHub tags. SNS allows 10 ListTagsForResource calls per second, so turn this off on the sns source for accounts with tens of thousands of topics.",
      "title": "Extract Tags",
      "type": "boolean"
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "description": "Domain urn or id to regex patterns on resource names; a matching queue or topic is assigned that domain. Domain names are not resolved: use the domain's urn or id.",
      "title": "Domain",
      "type": "object"
    },
    "account_platform_instance_map": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "AWS account id to platform_instance, for lineage to resources in other accounts whose own recipes set a platform_instance other than the account id. Accounts not listed use their account id.",
      "title": "Account Platform Instance Map",
      "type": "object"
    },
    "platform_map": {
      "additionalProperties": {
        "$ref": "#/$defs/LinkedPlatformDetail"
      },
      "description": "Per platform (`s3`), the platform_instance, env and convert_urns_to_lowercase of that platform's own recipe, so lineage finds the dataset URNs it emits. A platform not listed uses no platform_instance, this recipe's env and no lowercasing.",
      "propertyNames": {
        "const": "s3"
      },
      "title": "Platform Map",
      "type": "object"
    },
    "cloudtrail_producers": {
      "$ref": "#/$defs/CloudTrailProducersConfig",
      "description": "Producer lineage from CloudTrail Lake (off by default)."
    },
    "lineage": {
      "$ref": "#/$defs/AwsMessagingLineageConfig",
      "description": "Lineage through Lambda and EventBridge."
    },
    "remove_stale_lineage": {
      "default": true,
      "description": "Remove lineage edges this source wrote on an earlier run that no longer exist in AWS, such as a removed redrive policy or subscription. Only edges from resources this run read in full are removed. Needs a DataHub graph connection (the datahub-rest sink or datahub_api); costs one DataHub lookup per queue or topic.",
      "title": "Remove Stale Lineage",
      "type": "boolean"
    },
    "glue_schema_registry": {
      "$ref": "#/$defs/QueueGlueSchemaRegistryConfig",
      "description": "Queue schemas from AWS Glue Schema Registry (off by default)."
    },
    "include_column_lineage": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Add field-level lineage on hops that deliver the message unchanged or with a simple field mapping, for fields present in both schemas. See the docs for which hops qualify. Schemas come from Glue Schema Registry in this run or from DataHub, one DataHub lookup per hop for a schema not read from Glue. Defaults to glue_schema_registry.enabled; set it to true to use schemas that are already in DataHub without Glue.",
      "title": "Include Column Lineage"
    },
    "queue_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns on queue names to include."
    }
  },
  "title": "SqsSourceConfig",
  "type": "object"
}
```





### Capabilities

#### Platform instance and URNs

When `platform_instance` is not set, the connector reads the account id from a queue ARN and uses it. Dataset names include the region, so several regions of one account share the platform instance without colliding. The `sns` source names topics the same way.

#### Dead-letter queue lineage

Each queue with a redrive policy becomes an upstream of its dead-letter queue, with a `relationship: dead_letter` property on the edge. Edges are written as patches, so lineage that the `sns` source or another account's recipe adds to the same queue is kept. The edge is only added when the dead-letter queue was ingested in the same run; one excluded by `queue_pattern` gets none. Skipped edges are listed in the report under `lineage_edges_skipped`.

When a redrive policy is removed or pointed at another queue, the next run removes the old `dead_letter` edge (`remove_stale_lineage`, on by default). This reads the queue's current lineage from DataHub, so it needs a graph connection (the `datahub-rest` sink or `datahub_api`); without one, old edges stay. Only `dead_letter` edges from queues read in full this run are removed, so a queue whose attributes could not be read keeps its edges. Edges with other relationships, such as the `sns` source's subscription edges, and edges from queues that other recipes ingest are never touched.

#### Schemas from Glue Schema Registry

SQS stores opaque messages, so there is no schema by default. If producers register message schemas in [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html), set `glue_schema_registry.enabled` and list each queue and its schema in `glue_schema_registry.queue_schema_map`. AWS keeps no link between a queue and a schema, so the map is the reliable way; `use_naming_convention` looks for a schema named like the queue when the map has no entry. AVRO, JSON and PROTOBUF schemas are supported, read with `glue:GetSchemaVersion`. PROTOBUF also needs the `kafka` extra, for example `pip install 'acryl-datahub[sqs,kafka]'`.

#### Lambda and EventBridge

With the `lineage` options on (the default), the connector follows the services around its queues:

| Edge                                                   | Read from                                                                                                                                |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Queue to Lambda function                               | `lambda:ListEventSourceMappings`                                                                                                         |
| Lambda function to queue                               | event source mapping `OnFailure` destination; asynchronous invocation destinations (`lineage.lambda_async_destinations`, off by default) |
| Event bus (none for a scheduled rule) to rule to queue | `events:ListRules`, `events:ListTargetsByRule`                                                                                           |
| Pipe from or to a queue                                | `pipes:ListPipes`                                                                                                                        |

- A Lambda function is a DataFlow (platform `lambda`) with one `invoke` DataJob, the shape the `kinesis` source uses for Firehose. Functions appear only when one of these edges involves one of the run's queues, and only for the recipe's own account and region.
- A rule (platform `eventbridge`) is a DataFlow with one `route` DataJob from its event bus, emitted as an `Event Bus` dataset, to its targets. A scheduled rule has no event bus input, so its job has outputs only. A pipe (platform `eventbridge-pipes`) is a DataFlow with one `pipe` DataJob from source to target. A rule or pipe that targets a function becomes an upstream job of that function.
- The `sqs` and `sns` sources each add only the edges whose dataset end they own, as patches, so a rule targeting both a queue and a topic gets both outputs once both have run.
- Some pipes also get field-level lineage; see the table below.

#### When column lineage is emitted

Field-level lineage is only added where a message reaches the next hop unchanged (or, for a pipe, where its input template maps body fields to keys), for fields present in both schemas (matched case-insensitively on the simple field path). Schemas come from Glue Schema Registry in the same run or from DataHub. Without a schema on both sides, the edge stays table-level. Most deployments have no schema on one side and see table-level lineage only. The report counts the reason for each hop it skips under `column_lineage_skipped`; with `include_column_lineage` off, nothing is attempted or counted, and existing field edges are left alone.

| Hop                                                                                                        | Column lineage                            | Why                                                                                               |
| ---------------------------------------------------------------------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Queue to its dead-letter queue (`sqs`)                                                                     | Yes, field to same field                  | SQS moves the failed message itself                                                               |
| Topic to queue, raw message delivery (`sns`, with `include_subscription_attributes`)                       | Yes, field to same field                  | The queue receives the published message as is                                                    |
| Topic to queue, without raw delivery (`sns`)                                                               | No                                        | SNS wraps the message in its JSON envelope                                                        |
| Topic to a subscription's dead-letter queue (`sns`)                                                        | No                                        | AWS doesn't document whether that queue holds the raw message or the envelope                     |
| Pipe from a queue whose input template only has `<$.body.path>` values                                     | Yes, each template key from its body path | Pipes decodes the SQS body and fills each key from one field                                      |
| Pipe without an input template, with an enrichment step, or with literals or concatenation in the template | No                                        | Without a template the target gets the SQS record (message id, attributes, body), not the message |
| Any hop through a Lambda function                                                                          | No                                        | The function can change anything                                                                  |

`include_column_lineage` defaults to `glue_schema_registry.enabled`. With it on, each queue with a dead-letter queue costs a DataHub lookup for any schema not read from Glue this run. Set it to `true` without Glue to use schemas already in DataHub, or `false` to turn it off.

Field edges are retracted along with table-level ones (`remove_stale_lineage`): when a field is renamed or dropped, field edges from that upstream that this run no longer emits are removed. A hop whose field lineage was skipped this run (no schema on one side, a Glue schema that couldn't be read, or `include_column_lineage` off) keeps its field edges.

#### S3 event notifications

`lineage.s3_notifications` (off by default) reads the notification configuration of each bucket in the recipe's region and adds the matching S3 datasets as upstreams of the queues each notification targets; the bucket itself is not linked. The `s3` source names datasets after a path_spec's table folder (`landing/incoming/orders` for `s3://landing/incoming/{table}/*.parquet`), while a notification only has a key prefix filter such as `incoming/`. A prefix ending in `/` matches the dataset `<bucket>/incoming` and every dataset under `<bucket>/incoming/`; any other prefix matches dataset names that start with `<bucket>/<prefix>`, and no prefix matches every dataset in the bucket. A suffix filter such as `.csv` is checked against datasets named after a file the `s3` source reads (`csv`, `tsv`, `json`, `jsonl`, `parquet` or `avro`, optionally compressed, such as `<bucket>/incoming/2024.csv.gz`); datasets named after a folder, including dotted ones such as `orders.v2`, still match, since their files aren't known. Up to 100 datasets per notification are linked, the first by name (a warning says when that cap is hit). No S3 datasets are created, and soft-deleted ones are not linked. The lookup needs a DataHub graph connection (the `datahub-rest` sink or `datahub_api`); without one no S3 upstreams are added and the report lists the skipped edges. Run the `s3` source first and set `platform_map.s3` (`platform_instance`, `env`, `convert_urns_to_lowercase`) to match that recipe. While a notification matches no S3 dataset in DataHub, earlier S3 edges into its queue are kept. Only edges from buckets whose notifications this run read are removed. `s3:ListAllMyBuckets` only returns buckets the recipe's account owns, so an edge from a bucket in another account is kept. This makes one `s3:GetBucketNotification` call per bucket in the recipe's region. Filtering buckets by region needs botocore 1.35.42 or later; with an older botocore the connector warns and reads every bucket's notification configuration.

Each source only links its own queues, so turn this on in the `sqs` recipe if buckets notify queues and in the `sns` recipe if they notify topics, including when one bucket notifies both. Each source reads every bucket's notification configuration. When the bucket listing succeeds, an S3 edge whose notification was removed is removed on the next run (`remove_stale_lineage`) if its bucket's notifications were read; a bucket whose notifications couldn't be read keeps its edges, and a failed listing removes none; this reads the current lineage of each of the source's queues from DataHub.

#### Producers from CloudTrail Lake

Code that sends with the AWS SDK leaves no trace in any configuration API, so most queues have no upstream. If you log SQS and SNS data events to a [CloudTrail Lake](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-lake.html) event data store, set `cloudtrail_producers.enabled` and `cloudtrail_producers.event_data_store`. The connector runs one query per run over the last `lookback_days` of `SendMessage` and `SendMessageBatch` events and groups them by caller and queue.

- With `lineage.lambda_functions` on, a caller that is a Lambda function in the recipe's account and region becomes an upstream job of the queue. The match relies on Lambda naming the execution role session after the function, and the caller's role must be that function's execution role. That convention has not yet been checked against a real trail for every runtime and invocation path, so treat these edges as best effort.
- Any other caller (an ECS task or EC2 role, an IAM user) is listed by caller ARN (the role ARN for an assumed role, the user ARN for an IAM user) in the queue's `observed_producers` property, not drawn as lineage, because a role or user is not a DataHub entity.
- The query only covers events in the recipe's region, and SQS targets are matched on the region and account in each queue URL (or queue ARN, which `SendMessage` also accepts), so a multi-region trail doesn't mix up same-named queues.
- Give `event_data_store` as an ARN when the store is in another region or another account: the query runs in the store's region, and the owner's account id comes from the ARN. A store in another account must also be shared with the ingestion role through its resource policy.
- If the query fails, the queue's existing `observed_producers` value is read back from DataHub and kept, which needs a DataHub connection (the `datahub-rest` sink provides one). Without one it is cleared until the next successful run, with a warning. If the query succeeds and no role sent to the queue in the lookback window, the value is removed. A Lambda producer edge that the window no longer shows is removed with the other stale Lambda output edges (`remove_stale_lineage`, with `lineage.lambda_async_destinations` on); a failed query keeps them.
- CloudTrail Lake bills queries by data scanned, and data events must be enabled for the queues, which also costs. Both are off by default. Each run scans the whole `lookback_days` window (default 7), so a daily schedule scans each day about seven times; lower `lookback_days` to match the schedule if cost matters. If the query fails, each queue also costs one DataHub read of its `datasetProperties` to keep `observed_producers`.

#### Tags

AWS resource tags become DataHub tags named `Key:Value` (or `Key` when the value is empty), the same format as the `kinesis` source. They are added to the queue rather than replacing its tags, so tags added in the DataHub UI stay. The flip side is that a tag removed in AWS stays in DataHub until removed there. To turn an `owner` tag into ownership, add the `extract_ownership_from_tags` transformer to the recipe.

#### Domains

`domain` maps a domain urn or id to regex patterns on queue names, as in the `glue` source. A queue matching a pattern is assigned that domain.

### Limitations

- One region per recipe. Run one recipe per region for multi-region accounts.
- Schema metadata comes from Glue Schema Registry (`queue_schema_map` or `use_naming_convention`) or from schemas already in DataHub; SQS itself stores opaque message bodies.
- Queue depth (`ApproximateNumberOfMessages` and similar) is not ingested; it changes every few seconds.

- Lambda functions, rules, pipes and event buses can be emitted by both the `sqs` and `sns` sources, so neither source soft-deletes them through stale-entity removal: one source dropping a function it no longer references would delete it while the other still does. A function, rule or pipe deleted in AWS stays in DataHub until removed there. Its edges to this source's queues are still removed on the next run (`remove_stale_lineage`), so it stops showing up in their lineage. Edges are only removed for a kind of resource that was read in full: a failed or turned-off listing keeps them, and Lambda output edges are only removed with `lineage.lambda_async_destinations` on, since some outputs come from those destinations. Only jobs in the recipe's own account and region are checked: an edge another account's recipe wrote is left for that recipe. A pipe's field edges are removed with its output edge, or when its input template no longer maps that field.
- The `lambda`, `eventbridge` and `eventbridge-pipes` region containers are shared with the other source in the same way.
- Producers that send from application code are not visible in these APIs, so many queues and topics have no upstream.

### Troubleshooting

- **"Listing denied" failure**: the role lacks `sqs:ListQueues`. Add the statement above.
- **"Listing aborted mid-pagination" failure**: AWS throttled or failed partway through the listing. Re-run before relying on stale-entity removal, since queues past that point were not seen.
- **"Could not read a resource ARN to find the AWS account id" error**: the first `sqs:ListQueues` call failed, so the run stopped rather than write queues under URNs without the account id. Re-run, or set `platform_instance` explicitly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.aws_messaging.sqs_source.SqsSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/aws_messaging/sqs_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Amazon SQS, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
