# API usage aggregation (`com.linkedin.metadata.usage.store`)

In-memory rollup for **GMS API traffic** — operational Micrometer metrics and optional
commercial flush sinks registered via Spring extension points.

## Types

| Type                            | Role                                                             |
| ------------------------------- | ---------------------------------------------------------------- |
| `UsageAggregationStore`         | Interface: `recordRequest` / `recordResponse` / `flush`          |
| `InMemoryUsageAggregationStore` | Default in-memory implementation                                 |
| `DistinctIdentitySet`           | In-memory unique identity set per `(metric, actor_class)` bucket |
| `DistinctUsageSnapshot`         | Immutable flush payload: metric, actor class, identity list      |
| `UsageStoreKeys`                | `AdditiveRollupKey`, `DistinctRollupKey`                         |

Types were named `*UsageRollupStore` initially but were **renamed to `*UsageAggregationStore`**
to avoid colliding with the legacy product-usage rollup package in this module.

## Entry points

- `UsageMetricsSessionEnricher` — GMS HTTP session lifecycle (`recordRequest` / `recordResponse`)
- `UsageQueueIngestRecorder` — MCE Kafka/pgQueue MCP consumption (`metadata_ingest`, `request_api=messaging`); skips when MCP `headers` contain `X-DataHub-Usage-PreRecorded`
- `UsageMetadataChangeProposalEnricher` — stamps that header on GMS async MCP publish (OpenAPI/Rest.li `metadata_ingest`)
- `UsageRecordingFilter` — response byte capture (GMS only)
- Tagged controllers (`withUsageOperation`) + GraphQL classification

## Configuration

- `usage_operations.yaml` — operation taxonomy and `default_cost_units`
- `usage_metric_registry.yaml` — metric definitions

## Enable flags

- `USAGE_AGGREGATION_ENABLED` — GMS aggregation + flush coordinator
- `USAGE_AGGREGATION_MICROMETER_EXPORT_ENABLED` — Micrometer sink (default on)
