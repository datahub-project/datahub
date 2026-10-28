### Capabilities

#### Platform instance and URNs

When `platform_instance` is not set, the connector reads the account id from a queue ARN and uses it. Dataset names include the region, so several regions of one account share the platform instance without colliding. The `sns` source names queues the same way.

#### Dead-letter queue lineage

Each queue with a redrive policy becomes an upstream of its dead-letter queue, with a `relationship: dead_letter` property on the edge. Edges are written as patches, so lineage that the `sns` source or another account's recipe adds to the same queue is kept. The edge is only added when the dead-letter queue was ingested in the same run; one excluded by `queue_pattern` gets none. Skipped edges are listed in the report under `lineage_edges_skipped`.

#### Schemas from Glue Schema Registry

SQS stores opaque messages, so there is no schema by default. If producers register message schemas in [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html), set `glue_schema_registry.enabled` and list each queue and its schema in `glue_schema_registry.queue_schema_map`. AWS keeps no link between a queue and a schema, so the map is the reliable way; `use_naming_convention` looks for a schema named like the queue when the map has no entry. AVRO, JSON and PROTOBUF schemas are supported, read with `glue:GetSchemaVersion`.

#### When column lineage is emitted

Field-level lineage is only added where a message reaches the next hop unchanged, for fields present in both schemas (matched case-insensitively on the simple field path). Schemas come from Glue Schema Registry in the same run or from DataHub. Without a schema on both sides, the edge stays table-level. Most deployments have no schema on one side and see table-level lineage only. The report counts each skip reason under `column_lineage_skipped`.

| Hop                                                 | Column lineage           | Why                                                                           |
| --------------------------------------------------- | ------------------------ | ----------------------------------------------------------------------------- |
| Queue to its dead-letter queue (`sqs`)              | Yes, field to same field | SQS moves the failed message itself                                           |
| Topic to queue, raw message delivery (`sns`)        | Yes, field to same field | The queue receives the published message as is                                |
| Topic to queue, without raw delivery (`sns`)        | No                       | SNS wraps the message in its JSON envelope                                    |
| Topic to a subscription's dead-letter queue (`sns`) | No                       | AWS doesn't document whether that queue holds the raw message or the envelope |

Turn it off with `column_lineage` (on by default).

#### Tags

AWS resource tags become DataHub tags named `Key:Value` (or `Key` when the value is empty), the same format as the `kinesis` source. They are added to the queue rather than replacing its tags, so tags added in the DataHub UI stay. The flip side is that a tag removed in AWS stays in DataHub until removed there. To turn an `owner` tag into ownership, add the `extract_ownership_from_tags` transformer to the recipe.

#### Domains

`domain` maps a domain urn or id to regex patterns on queue names, as in the `glue` source. A queue matching a pattern is assigned that domain.

### Limitations

- One region per recipe. Run one recipe per region for multi-region accounts.
- Schema metadata needs Glue Schema Registry and an explicit map; SQS itself stores opaque message bodies.
- Queue depth (`ApproximateNumberOfMessages` and similar) is not ingested; it changes every few seconds.

### Troubleshooting

- **"Listing denied" warning**: the role lacks `sqs:ListQueues`. Add the statement above.
- **"Listing aborted mid-pagination" failure**: AWS throttled or failed partway through the listing. Re-run before relying on stale-entity removal, since queues past that point were not seen.
- **URNs without an account id**: the account id could not be read (no queues, or the first call failed). Set `platform_instance` explicitly.
