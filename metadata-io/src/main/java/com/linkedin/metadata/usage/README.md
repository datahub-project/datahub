# API usage aggregation (`com.linkedin.metadata.usage`)

In-memory rollup for **GMS API traffic** — operational Micrometer metrics and optional
commercial extension overlays.

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

Configuration YAML loaders and manifests live in `com.linkedin.metadata.config.usage.*`
(`manifest`, `loader`, `overlay`, `graphql`, `metric`).

## Enable flags

- `USAGE_AGGREGATION_ENABLED` — GMS aggregation + flush coordinator
- `USAGE_AGGREGATION_MICROMETER_EXPORT_ENABLED` — Micrometer sink (default on)

See [`store/README.md`](store/README.md) for store-level types.
