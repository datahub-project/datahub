## Overview

[Amazon SQS](https://aws.amazon.com/sqs/) is AWS's managed message queue service, and [Amazon SNS](https://aws.amazon.com/sns/) is its managed pub/sub service. Applications usually use them together: an SNS topic fans messages out to SQS queues, and queues hand failed messages to dead-letter queues.

The `aws-messaging` source plugin catalogs both services in one ingestion recipe. Queues are emitted under the `sqs` platform and topics under the `sns` platform, each grouped in a region container, with AWS resource tags as DataHub tags and stateful deletion of queues and topics that no longer exist.

## Concept Mapping

| Source Concept               | DataHub Concept                                     | Notes                                                                               |
| ---------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------- |
| AWS account                  | [Platform Instance](../../../platform-instances.md) | Defaults to the account id read from a resource ARN                                 |
| AWS region                   | [Container](../../metamodel/entities/container.md)  | Subtype `Region`, one per platform                                                  |
| SQS queue                    | [Dataset](../../metamodel/entities/dataset.md)      | Platform `sqs`, subtype `Queue`; name `<region>.<queue name>`                       |
| SNS topic                    | [Dataset](../../metamodel/entities/dataset.md)      | Platform `sns`, subtype `Topic`; name `<region>.<topic name>`                       |
| Subscription, redrive policy | Upstream lineage (patch)                            | Topic to queue, queue to dead-letter queue, topic to subscription dead-letter queue |
| Resource tag                 | [Tag](../../metamodel/entities/tag.md)              | `Key:Value`, or `Key` for an empty value                                            |
