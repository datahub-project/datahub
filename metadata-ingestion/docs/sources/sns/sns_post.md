### Capabilities

#### Platform instance and URNs

When `platform_instance` is not set, the connector reads the account id from a topic ARN and uses it. Dataset names include the region. The `sqs` source names queues the same way, through the same shared code.

#### Subscription lineage

| Edge                                        | Read from                                                              |
| ------------------------------------------- | ---------------------------------------------------------------------- |
| Topic to SQS queue                          | `sns:ListSubscriptionsByTopic`, protocol `sqs`                         |
| Topic to a subscription's dead-letter queue | the subscription's `RedrivePolicy` (`include_subscription_attributes`) |

Edges are upstream lineage patches on the queue, so the queue keeps its dead-letter lineage from the `sqs` source and edges from other accounts' recipes. Each edge carries a `relationship` property (`subscription` or `subscription_dead_letter`).

When a subscription is removed, the next run removes its edge (`remove_stale_lineage`, on by default). This reads each topic's current lineage from DataHub, so it needs a graph connection (the `datahub-rest` sink or `datahub_api`); without one, old edges stay. Only `subscription` edges are removed, plus `subscription_dead_letter` edges when `include_subscription_attributes` is on, and only from topics whose subscriptions were all read this run. Edges the `sqs` source or other accounts' recipes write are never touched.

`include_subscription_attributes` is off by default: it makes one `sns:GetSubscriptionAttributes` call per subscription, of every protocol, on every run. The count is in the report as `subscription_attribute_calls`. Turn it on if subscriptions have dead-letter queues you want in lineage.

A queue in the recipe's own account and region gets an edge only if `sqs:ListQueues` shows it exists. A queue in another account or region gets an edge under that account's platform instance: the account id, or the value in `account_platform_instance_map`. Unconfirmed and deleted subscriptions, which SNS lists with a placeholder such as `PendingConfirmation` or `Deleted` instead of an ARN, are skipped. Skipped edges are listed in the report under `lineage_edges_skipped`.

Topics record how many subscriptions each protocol has, and the hosts of HTTP(S) endpoints (`web_endpoint_hosts`). Email and SMS endpoints and full endpoint URLs are never stored.

#### Schemas from Glue Schema Registry

SNS stores opaque messages, so there is no schema by default. If producers register message schemas in [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html), set `glue_schema_registry.enabled` and list each topic and its schema in `glue_schema_registry.topic_schema_map`. AWS keeps no link between a topic and a schema, so the map is the reliable way; `use_naming_convention` looks for a schema named like the topic when the map has no entry. AVRO, JSON and PROTOBUF schemas are supported, read with `glue:GetSchemaVersion`. PROTOBUF also needs the `kafka` extra, for example `pip install 'acryl-datahub[sns,kafka]'`.

#### When column lineage is emitted

Field-level lineage is only added where a message reaches the next hop unchanged, for fields present in both schemas (matched case-insensitively on the simple field path). Schemas come from Glue Schema Registry in the same run or from DataHub. Without a schema on both sides, the edge stays table-level. Most deployments have no schema on one side and see table-level lineage only. The report counts each skip reason under `column_lineage_skipped`.

| Hop                                                 | Column lineage           | Why                                                                           |
| --------------------------------------------------- | ------------------------ | ----------------------------------------------------------------------------- |
| Queue to its dead-letter queue (`sqs`)              | Yes, field to same field | SQS moves the failed message itself                                           |
| Topic to queue, raw message delivery (`sns`)        | Yes, field to same field | The queue receives the published message as is                                |
| Topic to queue, without raw delivery (`sns`)        | No                       | SNS wraps the message in its JSON envelope                                    |
| Topic to a subscription's dead-letter queue (`sns`) | No                       | AWS doesn't document whether that queue holds the raw message or the envelope |

`column_lineage` defaults to `glue_schema_registry.enabled` and needs `include_subscription_attributes` to see raw delivery. With it on, each raw-delivery subscription costs a DataHub lookup for any schema not read from Glue this run. Set it to `true` without Glue to use schemas already in DataHub, or `false` to turn it off.

Field edges are retracted along with table-level ones (`remove_stale_lineage`): when a field is renamed or dropped, or a subscription turns raw delivery off, field edges from that upstream that this run no longer emits are removed.

#### Tags and data protection

AWS resource tags become DataHub tags named `Key:Value` (or `Key` for an empty value), the same format as the `kinesis` and `sqs` sources. They are added to the topic rather than replacing its tags, so tags added in the DataHub UI stay; a tag removed in AWS stays in DataHub until removed there. Use the `extract_ownership_from_tags` transformer to turn an `owner` tag into ownership.

With `data_protection.enabled`, each topic's [message data protection](https://docs.aws.amazon.com/sns/latest/dg/sns-message-data-protection.html) policy is read and every data identifier it names (for example `EmailAddress`) becomes a tag `aws_data_identifier:EmailAddress`. Map identifiers to existing glossary terms with `data_protection.glossary_term_map` to attach terms as well; no terms are created. These tags and terms are added the same way as resource tags: if a topic's policy stops covering an identifier, its tag and term stay until removed in DataHub, so check them before relying on them as a current classification.

#### Domains

`domain` maps a domain urn or id to regex patterns on topic names, as in the `glue` source.

### Limitations

- One region per recipe.
- Schema metadata needs Glue Schema Registry and an explicit map; SNS itself doesn't store messages.
- Lineage to a queue that the `sqs` recipe's `queue_pattern` excludes is still written, because the queue exists and its URN follows from its ARN. This source doesn't read the `sqs` recipe's filters. The queue then appears in DataHub with only its lineage.
- Subscriptions to Lambda, Firehose and HTTP endpoints are counted but not yet drawn as lineage.

### Troubleshooting

- **"Listing denied" failure on `sns:ListTopics`**: add the statement above.
- **"Could not read a resource ARN to find the AWS account id" error**: the first `sns:ListTopics` call failed, so the run stopped rather than write topics under URNs without the account id. Re-run, or set `platform_instance` explicitly.
- **Edges skipped with "not found in this region"**: the queue doesn't exist, or `sqs:ListQueues` was denied.
- **Lineage doesn't join with the `sqs` source**: check that both recipes use the same `env`, `platform_instance` and `account_platform_instance_map`.
