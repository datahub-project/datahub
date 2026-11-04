### Overview

The `sns` module reads the SNS control-plane APIs for one AWS region per recipe. It never publishes or reads messages.

SNS has no schema, so topics carry no schema fields. Topic settings (display name, FIFO, KMS key, archive policy) and subscription counts per protocol are stored as custom properties.

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
