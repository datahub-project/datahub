### Overview

The `sns` module reads the SNS control-plane APIs for one AWS region per recipe. It never publishes or reads messages.

SNS has no schema of its own; topic schemas can be attached from AWS Glue Schema Registry (opt-in). Topic settings (display name, FIFO, KMS key, archive policy) and subscription counts per protocol are stored as custom properties.

SQS queues are ingested by the separate `sqs` source. Topic-to-queue lineage is written by this source onto the queue URNs, so run both with the same `env`, `platform_instance` and `account_platform_instance_map`, or the edges will point at queue URNs the `sqs` source doesn't emit.

### Prerequisites

#### AWS IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SnsRead",
      "Effect": "Allow",
      "Action": [
        "sns:ListTopics",
        "sns:GetTopicAttributes",
        "sns:ListTagsForResource",
        "sns:ListSubscriptions",
        "sns:GetSubscriptionAttributes",
        "sns:GetDataProtectionPolicy"
      ],
      "Resource": "*"
    },
    {
      "Sid": "QueueExistence",
      "Effect": "Allow",
      "Action": ["sqs:ListQueues"],
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
      "Sid": "TagsRead",
      "Effect": "Allow",
      "Action": ["tag:GetResources"],
      "Resource": "*"
    }
  ]
}
```

- `sqs:ListQueues` tells the connector which queues in its own account and region exist, so a subscription to a deleted queue doesn't create an empty queue entity. Without it, edges to local queues are skipped with a warning.
- Tags are read with one `tag:GetResources` call. If it is denied the connector warns and calls `sns:ListTagsForResource` per topic, which SNS limits to 10 calls per second.
- `sns:GetSubscriptionAttributes` is only called when `include_subscription_attributes` is on, and `sns:GetDataProtectionPolicy` only when `data_protection.enabled` is on.

#### Authentication

Credentials come from `aws_config` or the standard boto3 chain: environment variables, a shared credentials profile, an IAM role on the host, or SSO. `aws_config.aws_role` assumes a role, including one in another account.

`glue:GetSchemaVersion` is only needed with `glue_schema_registry.enabled`.

`LambdaRead` is used with `lineage.lambda_functions` (`lambda:ListFunctions` and `lambda:ListFunctionEventInvokeConfigs` only with `lineage.lambda_async_destinations` too), and `EventBridgeRead` with `lineage.eventbridge_rules` or `lineage.eventbridge_pipes`. `pipes:DescribePipe` is only called for a pipe that could get column lineage.

`S3NotificationsRead` is used only with `lineage.s3_notifications`. By default only the bucket owner can read a bucket's notification configuration.
