### Overview

The `sqs` module reads the SQS control-plane APIs for one AWS region per recipe. It never receives or deletes messages, so running it has no effect on consumers.

SQS has no schema, so queues carry no schema fields. Queue settings (FIFO, visibility timeout, retention, encryption, dead-letter target, maximum receive count) are stored as custom properties, and the creation and last-modified times on the dataset properties.

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
    }
  ]
}
```

`sqs:ListQueueTags` is only called when `extract_tags` is on.

#### Authentication

Credentials come from `aws_config` or the standard boto3 chain: environment variables, a shared credentials profile, an IAM role on the host, or SSO. `aws_config.aws_role` assumes a role, including one in another account.
