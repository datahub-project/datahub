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
        "sns:ListSubscriptionsByTopic",
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

- `sqs:ListQueues` tells the connector which queues in its own account and region exist, so a subscription to a deleted queue doesn't create an empty queue entity. Without it, edges to local queues are skipped with a warning.
- Tags are read with one `tag:GetResources` call. If it is denied the connector warns and calls `sns:ListTagsForResource` per topic, which SNS limits to 10 calls per second.
- Subscriptions are listed per topic with `sns:ListSubscriptionsByTopic`, because `sns:ListSubscriptions` only returns the caller's own subscriptions and would miss ones other accounts created on your topics. That is one call per topic, and SNS allows 30 per second.
- `sns:GetSubscriptionAttributes` is only called when `include_subscription_attributes` is on, and `sns:GetDataProtectionPolicy` only when `data_protection.enabled` is on.

#### Authentication

Credentials come from `aws_config` or the standard boto3 chain: environment variables, a shared credentials profile, an IAM role on the host, or SSO. `aws_config.aws_role` assumes a role, including one in another account.

`glue:GetSchemaVersion` is only needed with `glue_schema_registry.enabled`.

`LambdaRead` is used with `lineage.lambda_functions`, and `LambdaAsyncDestinationsRead` only with `lineage.lambda_async_destinations` too; leave it out otherwise. `EventBridgeList` is used with `lineage.eventbridge_rules` or `lineage.eventbridge_pipes`, `EventBridgeRead` with `lineage.eventbridge_rules`, and `PipesRead` with `lineage.eventbridge_pipes`. The `ListEventBuses`, `ListRules` and `ListPipes` actions don't support resource-level scoping, so they need `*`. `pipes:DescribePipe` is only called for a pipe that could get column lineage. `lineage.lambda_async_destinations` is off by default because it makes one `lambda:ListFunctionEventInvokeConfigs` call per function in the region, from both sources; it stops at the first `AccessDenied`, and so does the `events:ListTargetsByRule` loop.

`S3ListBuckets` and `S3NotificationsRead` are used only with `lineage.s3_notifications`, which is off by default because it makes one `s3:GetBucketNotification` call per bucket in the recipe's region. `s3:ListAllMyBuckets` only returns buckets the calling account owns, so buckets in other accounts are not read. To read only some buckets, list their ARNs (`arn:aws:s3:::my-bucket`) in `S3NotificationsRead`; a bucket whose notifications are denied is skipped with a warning and keeps its earlier edges.

`CloudTrailLakeQuery` is used only with `cloudtrail_producers.enabled`; scope it to the store in `cloudtrail_producers.event_data_store`. `cloudtrail:CancelQuery` stops a query the run gave up on, which would otherwise keep scanning. Matching a caller to a Lambda function also needs `lambda:ListFunctions`, so with `cloudtrail_producers.enabled` grant that action even when `LambdaAsyncDestinationsRead` is otherwise left out.
