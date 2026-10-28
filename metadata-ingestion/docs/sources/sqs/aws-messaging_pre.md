### Overview

The `aws-messaging` module reads the SQS and SNS control-plane APIs for one AWS region per recipe. It never reads, receives, or deletes messages, so running it has no effect on consumers.

SQS and SNS have no schema, so datasets carry no schema fields. Queue settings (FIFO, visibility timeout, retention, encryption, dead-letter target) and topic settings (display name, FIFO, encryption, subscription counts) are stored as custom properties.

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
      "Sid": "SnsRead",
      "Effect": "Allow",
      "Action": [
        "sns:ListTopics",
        "sns:GetTopicAttributes",
        "sns:ListTagsForResource",
        "sns:ListSubscriptions",
        "sns:GetSubscriptionAttributes"
      ],
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
        "pipes:ListPipes"
      ],
      "Resource": "*"
    }
  ]
}
```

- `sqs:ListQueueTags` and `sns:ListTagsForResource` are only called when `extract_tags` is on.
- `sns:GetSubscriptionAttributes` is only called when `include_subscription_attributes` is on.
- `LambdaRead` is used when `lineage.lambda_functions` is on; `lambda:ListFunctions` and `lambda:ListFunctionEventInvokeConfigs` only when `lineage.lambda_async_destinations` is also on.
- `EventBridgeRead` is used when `lineage.eventbridge_rules` or `lineage.eventbridge_pipes` is on.
- If a listing call is denied on its first page, that service is skipped with a warning and the other service still runs.

#### Authentication

Credentials come from `aws_config` or the standard boto3 chain: environment variables, a shared credentials profile, an IAM role on the host, or SSO. `aws_config.aws_role` assumes a role, including one in another account.
