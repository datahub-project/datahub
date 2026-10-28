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
      "Resource": "*"
    },
    {
      "Sid": "LambdaRead",
      "Effect": "Allow",
      "Action": [
        "lambda:ListEventSourceMappings",
        "lambda:ListFunctions",
        "lambda:ListFunctionEventInvokeConfigs"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EventBridgeRead",
      "Effect": "Allow",
      "Action": [
        "events:ListEventBuses",
        "events:ListRules",
        "events:ListTargetsByRule",
        "pipes:ListPipes",
        "pipes:DescribePipe"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3NotificationsRead",
      "Effect": "Allow",
      "Action": ["s3:ListAllMyBuckets", "s3:GetBucketNotification"],
      "Resource": "*"
    },
    {
      "Sid": "CloudTrailLakeQuery",
      "Effect": "Allow",
      "Action": ["cloudtrail:StartQuery", "cloudtrail:GetQueryResults"],
      "Resource": "*"
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

`LambdaRead` is used with `lineage.lambda_functions` (`lambda:ListFunctions` and `lambda:ListFunctionEventInvokeConfigs` only with `lineage.lambda_async_destinations` too), and `EventBridgeRead` with `lineage.eventbridge_rules` or `lineage.eventbridge_pipes`. `pipes:DescribePipe` is only called for a pipe that could get column lineage.

`S3NotificationsRead` is used only with `lineage.s3_notifications`. By default only the bucket owner can read a bucket's notification configuration.

`CloudTrailLakeQuery` is used only with `cloudtrail_producers.enabled`.
