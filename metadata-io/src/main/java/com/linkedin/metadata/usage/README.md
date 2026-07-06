# API usage aggregation (`com.linkedin.metadata.usage`)

In-memory rollup for **GMS API traffic** — operational Micrometer metrics and optional commercial
extensions under `usage.billing`.

## Package layout

| Package                     | Role                                                                 |
| --------------------------- | -------------------------------------------------------------------- |
| `usage.store`               | In-memory aggregation (`UsageAggregationStore`, flush window state)  |
| `usage.flush`               | Flush batch DTOs, sinks, coordinator                                 |
| `usage.registry.operations` | `usage_operations.yaml` runtime registry (`UsageOperationsRegistry`) |
| `usage.registry.metrics`    | `usage_metric_registry.yaml` runtime registry + increment resolver   |
| `usage.registry.graphql`    | GraphQL classification registry builder                              |
| `usage.instrumentation`     | HTTP session enrichment, auth-channel resolution                     |
| `usage.identity`            | Actor-class classification                                           |
| `usage.billing`             | Commercial metric contributor and flush sink (SaaS)                  |

Configuration YAML loaders and manifests live in `com.linkedin.metadata.config.usage.*`
(`manifest`, `loader`, `overlay`, `graphql`, `metric`).

## Enable flags

- `USAGE_AGGREGATION_ENABLED` — GMS aggregation + flush coordinator
- `USAGE_AGGREGATION_MICROMETER_EXPORT_ENABLED` — Micrometer sink (default on)

## Not this tree

**Product usage events** from integrations (MCP, LLM tokens, etc.) use the separate
`com.linkedin.metadata.billing.rollup` package (SaaS), not this tree.

**Queue-path ingest** (`metadata_ingest` with `request_api=messaging`) is recorded on the MCE
consumer when `USAGE_AGGREGATION_ENABLED=true` there — see `UsageQueueIngestRecorder` and
`MceUsageAggregationFactory`. Async REST ingest dedup uses MCP `headers` (`X-DataHub-Usage-PreRecorded`)
via `UsageMetadataChangeProposalEnricher` on publish.

See [`store/README.md`](store/README.md) for store-level types.
