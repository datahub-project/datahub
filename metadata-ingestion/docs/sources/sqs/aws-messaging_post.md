### Capabilities

#### Platform instance and URNs

When `platform_instance` is not set, the connector reads the account id from the first topic or queue ARN and uses it. Dataset names include the region, so several regions of one account share the platform instance without colliding.

#### Lineage

The connector adds upstream lineage to queues:

| Edge                                            | Read from                                                              |
| ----------------------------------------------- | ---------------------------------------------------------------------- |
| SNS topic to SQS queue                          | `sns:ListSubscriptions`, protocol `sqs`                                |
| SQS queue to its dead-letter queue              | the queue's `RedrivePolicy`                                            |
| SNS topic to a subscription's dead-letter queue | the subscription's `RedrivePolicy` (`include_subscription_attributes`) |

Edges are written as patches, so lineage added by another account's recipe (for example a topic there subscribing a queue here) is kept. Each edge carries a `relationship` property: `subscription`, `dead_letter` or `subscription_dead_letter`.

A queue in the recipe's own account and region gets an edge only if this run ingested it, so a queue excluded by `queue_pattern` (or every queue, with `include_sqs: false`) gets none. Queues in other accounts or regions get an edge under that account's platform instance: the account id, or the value in `account_platform_instance_map`. Unconfirmed subscriptions are skipped. Skipped edges are listed in the report under `lineage_edges_skipped`.

#### Lambda, EventBridge and Firehose

With the `lineage` options on (the default), the connector also follows the services around the queues and topics:

| Edge                                | Read from                                                                                                                    |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Queue to Lambda function            | `lambda:ListEventSourceMappings`                                                                                             |
| Lambda function to queue or topic   | event source mapping `OnFailure` destination, and asynchronous invocation destinations (`lineage.lambda_async_destinations`) |
| Topic to Lambda function            | SNS subscription, protocol `lambda`                                                                                          |
| Event bus to rule to queue or topic | `events:ListRules`, `events:ListTargetsByRule`                                                                               |
| Pipe source to target               | `pipes:ListPipes`                                                                                                            |
| Topic to Firehose delivery job      | SNS subscription, protocol `firehose` (`lineage.firehose`)                                                                   |

- Lambda functions appear only when one of these edges involves a queue or topic in the run, and only for the recipe's own account and region. A function is a DataFlow with one `invoke` DataJob, the same shape the `kinesis` source uses for Firehose. A rule or pipe that targets a function becomes an upstream job of that function.
- Rules are emitted only when at least one target is a queue or topic, and pipes only when their source or target is one. A pipe's enrichment function is recorded as a property.
- The Firehose delivery job belongs to the `kinesis` source, so the topic is added to its inputs only when the job already exists in DataHub. That check needs a DataHub connection (the `datahub-rest` sink provides one). `kinesis_platform_instance` must match that source's `platform_instance` if it isn't the account id.

Topics record how many subscriptions each protocol has, and the hosts of HTTP(S) endpoints. Email and SMS endpoints and full endpoint URLs are never stored.

#### Tags

AWS resource tags become DataHub tags. To turn an `owner` tag into ownership, add the `extract_ownership_from_tags` transformer to the recipe.

### Limitations

- One region per recipe. Run one recipe per region for multi-region accounts.
- No schema metadata: SQS and SNS store opaque message bodies.
- SNS allows 10 `ListTagsForResource` calls per second, so tag extraction alone takes about 17 minutes for 10,000 topics. Set `extract_tags: false` or narrow `topic_pattern` on very large accounts.
- Producers that send from application code are not visible in these APIs, so many queues and topics have no upstream.
- Lambda functions in other accounts or regions get no edges; ingest that account and region with its own recipe.
- `ApproximateNumberOfMessages` and other queue depth metrics are not ingested; they change every few seconds.

### Troubleshooting

- **"Listing denied" warning**: the role lacks `sqs:ListQueues` or `sns:ListTopics`. Add the statement above, or turn the service off with `include_sqs` / `include_sns`.
- **"Listing aborted mid-pagination" failure**: AWS throttled or failed partway through a listing. Re-run before relying on stale-entity removal, since queues or topics past that point were not seen.
- **URNs without an account id**: the account id could not be read (no queues or topics, or the first call failed). Set `platform_instance` explicitly.
