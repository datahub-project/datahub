### Capabilities

#### Platform instance and URNs

When `platform_instance` is not set, the connector reads the account id from a queue ARN and uses it. Dataset names include the region, so several regions of one account share the platform instance without colliding. The `sns` source names queues the same way.

#### Dead-letter queue lineage

Each queue with a redrive policy becomes an upstream of its dead-letter queue, with a `relationship: dead_letter` property on the edge. Edges are written as patches, so lineage that the `sns` source or another account's recipe adds to the same queue is kept. The edge is only added when the dead-letter queue was ingested in the same run; one excluded by `queue_pattern` gets none. Skipped edges are listed in the report under `lineage_edges_skipped`.

When a redrive policy is removed or pointed at another queue, the next run removes the old `dead_letter` edge (`remove_stale_lineage`, on by default). This reads the queue's current lineage from DataHub, so it needs a graph connection (the `datahub-rest` sink or `datahub_api`); without one, old edges stay. Only `dead_letter` edges from queues read in full this run are removed, so a queue whose attributes could not be read keeps its edges, and edges written by the `sns` source or other recipes are never touched.

#### Schemas from Glue Schema Registry

SQS stores opaque messages, so there is no schema by default. If producers register message schemas in [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html), set `glue_schema_registry.enabled` and list each queue and its schema in `glue_schema_registry.queue_schema_map`. AWS keeps no link between a queue and a schema, so the map is the reliable way; `use_naming_convention` looks for a schema named like the queue when the map has no entry. AVRO, JSON and PROTOBUF schemas are supported, read with `glue:GetSchemaVersion`. PROTOBUF also needs the `kafka` extra, for example `pip install 'acryl-datahub[sqs,kafka]'`.

#### Lambda and EventBridge

With the `lineage` options on (the default), the connector follows the services around its queues:

| Edge                       | Read from                                                                                                                                |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Queue to Lambda function   | `lambda:ListEventSourceMappings`                                                                                                         |
| Lambda function to queue   | event source mapping `OnFailure` destination; asynchronous invocation destinations (`lineage.lambda_async_destinations`, off by default) |
| Event bus to rule to queue | `events:ListRules`, `events:ListTargetsByRule`                                                                                           |
| Pipe from or to a queue    | `pipes:ListPipes`                                                                                                                        |

- A Lambda function is a DataFlow (platform `lambda`) with one `invoke` DataJob, the shape the `kinesis` source uses for Firehose. Functions appear only when one of these edges involves one of the run's queues, and only for the recipe's own account and region.
- A rule (platform `eventbridge`) is a DataFlow with one `route` DataJob from its event bus, emitted as an `Event Bus` dataset, to its targets. A pipe (platform `eventbridge-pipes`) is a DataFlow with one `pipe` DataJob from source to target. A rule or pipe that targets a function becomes an upstream job of that function.
- The `sqs` and `sns` sources each add only the edges whose dataset end they own, as patches, so a rule targeting both a queue and a topic gets both outputs once both have run.
- Some pipes also get field-level lineage; see the table below.

#### When column lineage is emitted

Field-level lineage is only added where a message reaches the next hop unchanged, for fields present in both schemas (matched case-insensitively on the simple field path). Schemas come from Glue Schema Registry in the same run or from DataHub. Without a schema on both sides, the edge stays table-level. Most deployments have no schema on one side and see table-level lineage only. The report counts each skip reason under `column_lineage_skipped`.

| Hop                                                                                                        | Column lineage                            | Why                                                                                               |
| ---------------------------------------------------------------------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Queue to its dead-letter queue (`sqs`)                                                                     | Yes, field to same field                  | SQS moves the failed message itself                                                               |
| Topic to queue, raw message delivery (`sns`)                                                               | Yes, field to same field                  | The queue receives the published message as is                                                    |
| Topic to queue, without raw delivery (`sns`)                                                               | No                                        | SNS wraps the message in its JSON envelope                                                        |
| Topic to a subscription's dead-letter queue (`sns`)                                                        | No                                        | AWS doesn't document whether that queue holds the raw message or the envelope                     |
| Pipe from a queue whose input template only has `<$.body.path>` values                                     | Yes, each template key from its body path | Pipes decodes the SQS body and fills each key from one field                                      |
| Pipe without an input template, with an enrichment step, or with literals or concatenation in the template | No                                        | Without a template the target gets the SQS record (message id, attributes, body), not the message |
| Any hop through a Lambda function                                                                          | No                                        | The function can change anything                                                                  |

`column_lineage` defaults to `glue_schema_registry.enabled`. With it on, each queue with a dead-letter queue costs a DataHub lookup for any schema not read from Glue this run. Set it to `true` without Glue to use schemas already in DataHub, or `false` to turn it off.

Field edges are retracted along with table-level ones (`remove_stale_lineage`): when a field is renamed or dropped, or a subscription turns raw delivery off, field edges from that upstream that this run no longer emits are removed.

#### Tags

AWS resource tags become DataHub tags named `Key:Value` (or `Key` when the value is empty), the same format as the `kinesis` source. They are added to the queue rather than replacing its tags, so tags added in the DataHub UI stay. The flip side is that a tag removed in AWS stays in DataHub until removed there. To turn an `owner` tag into ownership, add the `extract_ownership_from_tags` transformer to the recipe.

#### Domains

`domain` maps a domain urn or id to regex patterns on queue names, as in the `glue` source. A queue matching a pattern is assigned that domain.

### Limitations

- One region per recipe. Run one recipe per region for multi-region accounts.
- Schema metadata needs Glue Schema Registry and an explicit map; SQS itself stores opaque message bodies.
- Queue depth (`ApproximateNumberOfMessages` and similar) is not ingested; it changes every few seconds.

- Lambda functions, rules, pipes and event buses can be emitted by both the `sqs` and `sns` sources, so neither source soft-deletes them through stale-entity removal: one source dropping a function it no longer references would delete it while the other still does. A function, rule or pipe deleted in AWS stays in DataHub until removed there. Its edges to this source's queues are still removed on the next run (`remove_stale_lineage`), so it stops showing up in their lineage. Edges are only removed for a kind of resource that was read in full: a failed or turned-off listing keeps them, and Lambda output edges are only removed with `lineage.lambda_async_destinations` on, since some outputs come from those destinations.
- The `lambda`, `eventbridge` and `eventbridge-pipes` region containers are shared with the other source in the same way.
- Producers that send from application code are not visible in these APIs, so many queues and topics have no upstream.

### Troubleshooting

- **"Listing denied" failure**: the role lacks `sqs:ListQueues`. Add the statement above.
- **"Listing aborted mid-pagination" failure**: AWS throttled or failed partway through the listing. Re-run before relying on stale-entity removal, since queues past that point were not seen.
- **"Could not read a resource ARN to find the AWS account id" error**: the first `sqs:ListQueues` call failed, so the run stopped rather than write queues under URNs without the account id. Re-run, or set `platform_instance` explicitly.
- **URNs without an account id**: the region has no queues, so there was no ARN to read the account from. Set `platform_instance` explicitly.
