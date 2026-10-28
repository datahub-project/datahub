### Capabilities

#### Platform instance and URNs

When `platform_instance` is not set, the connector reads the account id from the first topic or queue ARN and uses it. Dataset names include the region, so several regions of one account share the platform instance without colliding.

#### Tags

AWS resource tags become DataHub tags. To turn an `owner` tag into ownership, add the `extract_ownership_from_tags` transformer to the recipe.

### Limitations

- One region per recipe. Run one recipe per region for multi-region accounts.
- No schema metadata: SQS and SNS store opaque message bodies.
- SNS allows 10 `ListTagsForResource` calls per second, so tag extraction alone takes about 17 minutes for 10,000 topics. Set `extract_tags: false` or narrow `topic_pattern` on very large accounts.
- `ApproximateNumberOfMessages` and other queue depth metrics are not ingested; they change every few seconds.

### Troubleshooting

- **"Listing denied" warning**: the role lacks `sqs:ListQueues` or `sns:ListTopics`. Add the statement above, or turn the service off with `include_sqs` / `include_sns`.
- **"Listing aborted mid-pagination" failure**: AWS throttled or failed partway through a listing. Re-run before relying on stale-entity removal, since queues or topics past that point were not seen.
- **URNs without an account id**: the account id could not be read (no queues or topics, or the first call failed). Set `platform_instance` explicitly.
