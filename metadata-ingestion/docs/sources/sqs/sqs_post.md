### Capabilities

#### Platform instance and URNs

When `platform_instance` is not set, the connector reads the account id from a queue ARN and uses it. Dataset names include the region, so several regions of one account share the platform instance without colliding. The `sns` source names queues the same way.

#### Dead-letter queue lineage

Each queue with a redrive policy becomes an upstream of its dead-letter queue, with a `relationship: dead_letter` property on the edge. Edges are written as patches, so lineage that the `sns` source or another account's recipe adds to the same queue is kept. The edge is only added when the dead-letter queue was ingested in the same run; one excluded by `queue_pattern` gets none. Skipped edges are listed in the report under `lineage_edges_skipped`.

#### Schemas from Glue Schema Registry

SQS stores opaque messages, so there is no schema by default. If producers register message schemas in [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html), set `glue_schema_registry.enabled` and list each queue and its schema in `glue_schema_registry.queue_schema_map`. AWS keeps no link between a queue and a schema, so the map is the reliable way; `use_naming_convention` looks for a schema named like the queue when the map has no entry. AVRO, JSON and PROTOBUF schemas are supported, read with `glue:GetSchemaVersion`.

#### Lambda and EventBridge

With the `lineage` options on (the default), the connector follows the services around its queues:

| Edge                       | Read from                                                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Queue to Lambda function   | `lambda:ListEventSourceMappings`                                                                                         |
| Lambda function to queue   | event source mapping `OnFailure` destination; asynchronous invocation destinations (`lineage.lambda_async_destinations`) |
| Event bus to rule to queue | `events:ListRules`, `events:ListTargetsByRule`                                                                           |
| Pipe from or to a queue    | `pipes:ListPipes`                                                                                                        |

- A Lambda function is a DataFlow (platform `lambda`) with one `invoke` DataJob, the shape the `kinesis` source uses for Firehose. Functions appear only when one of these edges involves one of the run's queues, and only for the recipe's own account and region.
- A rule (platform `eventbridge`) is a DataFlow with one `route` DataJob from its event bus, emitted as an `Event Bus` dataset, to its targets. A pipe (platform `eventbridge-pipes`) is a DataFlow with one `pipe` DataJob from source to target. A rule or pipe that targets a function becomes an upstream job of that function.
- The `sqs` and `sns` sources each add only the edges whose dataset end they own, as patches, so a rule targeting both a queue and a topic gets both outputs once both have run.
- Some rules and pipes also get field-level lineage; see the table below.

#### When column lineage is emitted

Field-level lineage is only added where a message reaches the next hop unchanged, for fields present in both schemas (matched case-insensitively on the simple field path). Schemas come from Glue Schema Registry in the same run or from DataHub. Without a schema on both sides, the edge stays table-level. Most deployments have no schema on one side and see table-level lineage only. The report counts each skip reason under `column_lineage_skipped`.

| Hop                                                                                                        | Column lineage                                                                                           | Why                                                                                                               |
| ---------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Queue to its dead-letter queue (`sqs`)                                                                     | Yes, field to same field                                                                                 | SQS moves the failed message itself                                                                               |
| Topic to queue, raw message delivery (`sns`)                                                               | Yes, field to same field                                                                                 | The queue receives the published message as is                                                                    |
| Topic to queue, without raw delivery (`sns`)                                                               | No                                                                                                       | SNS wraps the message in its JSON envelope                                                                        |
| Topic to a subscription's dead-letter queue (`sns`)                                                        | No                                                                                                       | AWS doesn't document whether that queue holds the raw message or the envelope                                     |
| Event bus to queue or topic through a rule, no input transformer or constant                               | Yes: the whole event, or the subtree at a simple `InputPath` such as `$.detail` (`detail.x` maps to `x`) | The target gets the event or that subtree as is; the upstream schema is the event bus dataset's schema in DataHub |
| Rule target with an input transformer, a constant input, or a complex `InputPath`                          | No                                                                                                       | The payload is rebuilt                                                                                            |
| Pipe from a queue whose input template only has `<$.body.path>` values                                     | Yes, each template key from its body path                                                                | Pipes decodes the SQS body and fills each key from one field                                                      |
| Pipe without an input template, with an enrichment step, or with literals or concatenation in the template | No                                                                                                       | Without a template the target gets the SQS record (message id, attributes, body), not the message                 |
| Any hop through a Lambda function                                                                          | No                                                                                                       | The function can change anything                                                                                  |

Turn it off with `column_lineage` (on by default).

#### S3 event notifications

`lineage.s3_notifications` (off by default) reads each bucket's notification configuration and adds the bucket as an upstream of the queues it notifies. S3 datasets are named by bucket and path, while a notification only has a key prefix filter, so the connector looks for an existing S3 dataset named `<bucket>/<prefix>`, then `<bucket>`, and adds no edge if neither is in DataHub. Run the `s3` source first and set `s3_platform_instance` to match its `platform_instance`. This makes one `s3:GetBucketNotification` call per bucket in the recipe's region.

#### Tags

AWS resource tags become DataHub tags named `Key:Value` (or `Key` when the value is empty), the same format as the `kinesis` source. They are added to the queue rather than replacing its tags, so tags added in the DataHub UI stay. The flip side is that a tag removed in AWS stays in DataHub until removed there. To turn an `owner` tag into ownership, add the `extract_ownership_from_tags` transformer to the recipe.

#### Domains

`domain` maps a domain urn or id to regex patterns on queue names, as in the `glue` source. A queue matching a pattern is assigned that domain.

### Limitations

- One region per recipe. Run one recipe per region for multi-region accounts.
- Schema metadata needs Glue Schema Registry and an explicit map; SQS itself stores opaque message bodies.
- Queue depth (`ApproximateNumberOfMessages` and similar) is not ingested; it changes every few seconds.

- Lambda functions, rules, pipes and event buses can be emitted by both the `sqs` and `sns` sources, so neither source soft-deletes them through stale-entity removal: one source dropping a function it no longer references would delete it while the other still does. A function, rule or pipe deleted in AWS stays in DataHub until removed there.
- Producers that send from application code are not visible in these APIs, so many queues and topics have no upstream.

### Troubleshooting

- **"Listing denied" warning**: the role lacks `sqs:ListQueues`. Add the statement above.
- **"Listing aborted mid-pagination" failure**: AWS throttled or failed partway through the listing. Re-run before relying on stale-entity removal, since queues past that point were not seen.
- **URNs without an account id**: the account id could not be read (no queues, or the first call failed). Set `platform_instance` explicitly.
