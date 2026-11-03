### Capabilities

#### Platform instance and URNs

When `platform_instance` is not set, the connector reads the account id from a queue ARN and uses it. Dataset names include the region, so several regions of one account share the platform instance without colliding. The `sns` source names queues the same way.

#### Dead-letter queue lineage

Each queue with a redrive policy becomes an upstream of its dead-letter queue, with a `relationship: dead_letter` property on the edge. Edges are written as patches, so lineage that the `sns` source or another account's recipe adds to the same queue is kept. The edge is only added when the dead-letter queue was ingested in the same run; one excluded by `queue_pattern` gets none. Skipped edges are listed in the report under `lineage_edges_skipped`.

When a redrive policy is removed or pointed at another queue, the next run removes the old `dead_letter` edge (`remove_stale_lineage`, on by default). This reads the queue's current lineage from DataHub, so it needs a graph connection (the `datahub-rest` sink or `datahub_api`); without one, old edges stay. Only `dead_letter` edges from queues read in full this run are removed, so a queue whose attributes could not be read keeps its edges, and edges written by the `sns` source or other recipes are never touched.

#### Tags

AWS resource tags become DataHub tags named `Key:Value` (or `Key` when the value is empty), the same format as the `kinesis` source. They are added to the queue rather than replacing its tags, so tags added in the DataHub UI stay. The flip side is that a tag removed in AWS stays in DataHub until removed there. To turn an `owner` tag into ownership, add the `extract_ownership_from_tags` transformer to the recipe.

#### Domains

`domain` maps a domain urn or id to regex patterns on queue names, as in the `glue` source. A queue matching a pattern is assigned that domain.

### Limitations

- One region per recipe. Run one recipe per region for multi-region accounts.
- No schema metadata: SQS stores opaque message bodies.
- Queue depth (`ApproximateNumberOfMessages` and similar) is not ingested; it changes every few seconds.

### Troubleshooting

- **"Listing denied" failure**: the role lacks `sqs:ListQueues`. Add the statement above.
- **"Listing aborted mid-pagination" failure**: AWS throttled or failed partway through the listing. Re-run before relying on stale-entity removal, since queues past that point were not seen.
- **"Could not read a resource ARN to find the AWS account id" error**: the first `sqs:ListQueues` call failed, so the run stopped rather than write queues under URNs without the account id. Re-run, or set `platform_instance` explicitly.
- **URNs without an account id**: the region has no queues, so there was no ARN to read the account from. Set `platform_instance` explicitly.
