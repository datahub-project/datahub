### Capabilities

#### Platform instance and URNs

When `platform_instance` is not set, the connector reads the account id from a topic ARN and uses it. Dataset names include the region. The `sqs` source names queues the same way, through the same shared code.

#### Subscription lineage

| Edge                                        | Read from                                                              |
| ------------------------------------------- | ---------------------------------------------------------------------- |
| Topic to SQS queue                          | `sns:ListSubscriptions`, protocol `sqs`                                |
| Topic to a subscription's dead-letter queue | the subscription's `RedrivePolicy` (`include_subscription_attributes`) |

Edges are upstream lineage patches on the queue, so the queue keeps its dead-letter lineage from the `sqs` source and edges from other accounts' recipes. Each edge carries a `relationship` property (`subscription` or `subscription_dead_letter`).

A queue in the recipe's own account and region gets an edge only if `sqs:ListQueues` shows it exists. A queue in another account or region gets an edge under that account's platform instance: the account id, or the value in `account_platform_instance_map`. Unconfirmed subscriptions are skipped. Skipped edges are listed in the report under `lineage_edges_skipped`.

Topics record how many subscriptions each protocol has, and the hosts of HTTP(S) endpoints. Email and SMS endpoints and full endpoint URLs are never stored.

#### Schemas from Glue Schema Registry

SNS stores opaque messages, so there is no schema by default. If producers register message schemas in [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html), set `glue_schema_registry.enabled` and list each topic and its schema in `glue_schema_registry.topic_schema_map`. AWS keeps no link between a topic and a schema, so the map is the reliable way; `use_naming_convention` looks for a schema named like the topic when the map has no entry. AVRO, JSON and PROTOBUF schemas are supported, read with `glue:GetSchemaVersion`.

#### Column-level lineage

For a queue subscribed with raw message delivery, the message reaches the queue unchanged, so the connector adds field-level lineage for every field present in both schemas (matched case-insensitively). Without raw delivery SNS wraps the message in a JSON envelope and only table-level lineage is added. Both schemas must be available: the topic's from Glue Schema Registry in this run or from DataHub, the queue's from DataHub (for example from the `sqs` source with its own schema map). Most deployments have no schemas on one side and get table-level lineage only. `column_lineage_skipped` in the report counts each reason.

#### Lambda and EventBridge

With the `lineage` options on (the default), the connector follows the services around its topics:

| Edge                           | Read from                                                                                                                |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| Topic to Lambda function       | SNS subscription, protocol `lambda`                                                                                      |
| Lambda function to topic       | event source mapping `OnFailure` destination; asynchronous invocation destinations (`lineage.lambda_async_destinations`) |
| Event bus to rule to topic     | `events:ListRules`, `events:ListTargetsByRule`                                                                           |
| Pipe from or to a topic        | `pipes:ListPipes`                                                                                                        |
| Topic to Firehose delivery job | SNS subscription, protocol `firehose` (`firehose_lineage`)                                                               |

- A Lambda function is a DataFlow (platform `lambda`) with one `invoke` DataJob, the shape the `kinesis` source uses for Firehose. Functions appear only when one of these edges involves one of the run's topics, and only for the recipe's own account and region.
- A rule (platform `eventbridge`) is a DataFlow with one `route` DataJob from its event bus, emitted as an `Event Bus` dataset, to its targets. A pipe (platform `eventbridge-pipes`) is a DataFlow with one `pipe` DataJob from source to target. A rule or pipe that targets a function becomes an upstream job of that function.
- The `sqs` and `sns` sources each add only the edges whose dataset end they own, as patches, so a rule targeting both a queue and a topic gets both outputs once both have run.
- A pipe with no enrichment step and no input template passes messages through unchanged, so it gets field-level lineage for fields present in both the source's and the target's schemas (from Glue Schema Registry or DataHub). Most pipes have no schema on one side and get table-level lineage only.
- The Firehose delivery job belongs to the `kinesis` source, so the topic is added to its inputs only when the job already exists in DataHub. That check needs a DataHub connection (the `datahub-rest` sink provides one). Set `kinesis_platform_instance` if that source's `platform_instance` isn't the account id.

#### S3 event notifications

`lineage.s3_notifications` (off by default) reads each bucket's notification configuration and adds the bucket as an upstream of the topics it notifies. S3 datasets are named by bucket and path, while a notification only has a key prefix filter, so the connector looks for an existing S3 dataset named `<bucket>/<prefix>`, then `<bucket>`, and adds no edge if neither is in DataHub. Run the `s3` source first and set `s3_platform_instance` to match its `platform_instance`. This makes one `s3:GetBucketNotification` call per bucket in the recipe's region.

#### Producers from CloudTrail Lake

Code that sends with the AWS SDK leaves no trace in any configuration API, so most topics have no upstream. If you log SQS and SNS data events to a [CloudTrail Lake](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-lake.html) event data store, set `cloudtrail_producers.enabled` and `cloudtrail_producers.event_data_store`. The connector runs one query per run over the last `lookback_days` of `Publish` and `PublishBatch` events and groups them by caller and topic.

- A caller that is a Lambda function in the recipe's account and region becomes an upstream job of the topic. The match relies on Lambda naming the execution role session after the function. That convention has not yet been checked against a real trail for every runtime and invocation path, so treat these edges as best effort.
- Any other caller (an ECS task or EC2 role, an IAM user) is listed by role ARN in the topic's `observed_producers` property, not drawn as lineage, because a role is not a DataHub entity.
- CloudTrail Lake bills queries by data scanned, and data events must be enabled for the topics, which also costs. Both are off by default.

#### Tags and data protection

AWS resource tags become DataHub tags named `Key:Value` (or `Key` for an empty value), the same format as the `kinesis` and `sqs` sources. They are added to the topic rather than replacing its tags, so tags added in the DataHub UI stay; a tag removed in AWS stays in DataHub until removed there. Use the `extract_ownership_from_tags` transformer to turn an `owner` tag into ownership.

With `data_protection.enabled`, each topic's [message data protection](https://docs.aws.amazon.com/sns/latest/dg/sns-message-data-protection.html) policy is read and every data identifier it names (for example `EmailAddress`) becomes a tag `aws_data_identifier:EmailAddress`. Map identifiers to existing glossary terms with `data_protection.glossary_term_map` to attach terms as well; no terms are created.

#### Domains

`domain` maps a domain urn or id to regex patterns on topic names, as in the `glue` source.

### Limitations

- One region per recipe.
- Schema metadata needs Glue Schema Registry and an explicit map; SNS itself doesn't store messages.
- Lineage to queues excluded by the `sqs` recipe's `queue_pattern` is still written, because this source can't see that recipe's filters; the queue then appears with only its lineage.
- Subscriptions to HTTP, email and SMS endpoints are counted but not drawn as lineage.

- Lambda functions, rules, pipes and event buses can be emitted by both the `sqs` and `sns` sources, so neither source soft-deletes them through stale-entity removal: one source dropping a function it no longer references would delete it while the other still does. A function, rule or pipe deleted in AWS stays in DataHub until removed there.
- Producers that send from application code are not visible in these APIs, so many queues and topics have no upstream.

### Troubleshooting

- **"Listing denied" warning on `sns:ListTopics`**: add the statement above.
- **Edges skipped with "not found in this region"**: the queue doesn't exist, or `sqs:ListQueues` was denied.
- **Lineage doesn't join with the `sqs` source**: check that both recipes use the same `env`, `platform_instance` and `account_platform_instance_map`.
