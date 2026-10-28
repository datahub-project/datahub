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

#### Tags and data protection

AWS resource tags become DataHub tags named `Key:Value` (or `Key` for an empty value), the same format as the `kinesis` and `sqs` sources. They are added to the topic rather than replacing its tags, so tags added in the DataHub UI stay; a tag removed in AWS stays in DataHub until removed there. Use the `extract_ownership_from_tags` transformer to turn an `owner` tag into ownership.

With `data_protection.enabled`, each topic's [message data protection](https://docs.aws.amazon.com/sns/latest/dg/sns-message-data-protection.html) policy is read and every data identifier it names (for example `EmailAddress`) becomes a tag `aws_data_identifier:EmailAddress`. Map identifiers to existing glossary terms with `data_protection.glossary_term_map` to attach terms as well; no terms are created.

#### Domains

`domain` maps a domain urn or id to regex patterns on topic names, as in the `glue` source.

### Limitations

- One region per recipe.
- Schema metadata needs Glue Schema Registry and an explicit map; SNS itself doesn't store messages.
- Lineage to queues excluded by the `sqs` recipe's `queue_pattern` is still written, because this source can't see that recipe's filters; the queue then appears with only its lineage.
- Subscriptions to Lambda, Firehose and HTTP endpoints are counted but not yet drawn as lineage.

### Troubleshooting

- **"Listing denied" warning on `sns:ListTopics`**: add the statement above.
- **Edges skipped with "not found in this region"**: the queue doesn't exist, or `sqs:ListQueues` was denied.
- **Lineage doesn't join with the `sqs` source**: check that both recipes use the same `env`, `platform_instance` and `account_platform_instance_map`.
