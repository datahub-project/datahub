## Overview

[Amazon SQS](https://aws.amazon.com/sqs/) is AWS's managed message queue service. Producers send messages to a queue, consumers poll it, and messages that keep failing move to a dead-letter queue.

The `sqs` source plugin catalogs the queues of one AWS region as datasets, grouped in a region container, with queue settings as properties, AWS resource tags as DataHub tags, lineage from each queue to its dead-letter queue, and stateful deletion. Its companion, the [`sns` source](sns.md), catalogs SNS topics and adds topic-to-queue lineage; both build URNs the same way so their lineage joins.

## Concept Mapping

| Source Concept | DataHub Concept                                     | Notes                                                         |
| -------------- | --------------------------------------------------- | ------------------------------------------------------------- |
| AWS account    | [Platform Instance](../../../platform-instances.md) | Defaults to the account id read from a queue ARN              |
| AWS region     | [Container](../../metamodel/entities/container.md)  | Subtype `Region`                                              |
| Queue          | [Dataset](../../metamodel/entities/dataset.md)      | Platform `sqs`, subtype `Queue`; name `<region>.<queue name>` |
| Redrive policy | Upstream lineage (patch)                            | Queue to its dead-letter queue                                |
| Resource tag   | [Tag](../../metamodel/entities/tag.md)              | `Key:Value`, or `Key` for an empty value                      |
