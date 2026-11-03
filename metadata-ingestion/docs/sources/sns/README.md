## Overview

[Amazon SNS](https://aws.amazon.com/sns/) is AWS's managed pub/sub service. Publishers send messages to a topic and SNS delivers a copy to each subscriber: SQS queues, Lambda functions, Firehose streams, HTTP endpoints, email or SMS.

The `sns` source plugin catalogs the topics of one AWS region as datasets, grouped in a region container, with topic settings and subscription counts as properties, AWS resource tags as DataHub tags, lineage from each topic to its subscribed SQS queues and subscription dead-letter queues, optional tags or glossary terms from message data protection policies, and stateful deletion. Its companion, the [`sqs` source](sqs.md), catalogs the queues; both build URNs the same way so their lineage joins.

## Concept Mapping

| Source Concept                   | DataHub Concept                                                                                     | Notes                                                         |
| -------------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| AWS account                      | [Platform Instance](../../../platform-instances.md)                                                 | Defaults to the account id read from a topic ARN              |
| AWS region                       | [Container](../../metamodel/entities/container.md)                                                  | Subtype `Region`                                              |
| Topic                            | [Dataset](../../metamodel/entities/dataset.md)                                                      | Platform `sns`, subtype `Topic`; name `<region>.<topic name>` |
| SQS subscription, redrive policy | Upstream lineage (patch) on the queue                                                               | Topic to queue, topic to subscription dead-letter queue       |
| Resource tag                     | [Tag](../../metamodel/entities/tag.md)                                                              | `Key:Value`, or `Key` for an empty value                      |
| Data protection identifier       | [Tag](../../metamodel/entities/tag.md) or [Glossary Term](../../metamodel/entities/glossaryTerm.md) | Opt-in; terms only for identifiers you map                    |
