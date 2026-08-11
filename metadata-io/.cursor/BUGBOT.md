# metadata-io Bugbot rules

If a PR adds a new `@ConfigurationProperties` field, env-backed setting, or other
config property that can surface via system info, then:

- Flag missing classification in
  `PropertiesCollectorConfigurationTest` (sensitive vs non-sensitive lists).
- High security: unclassified properties may leak secrets in system-info APIs.
- Title: "Classify new config property"

## Embedding / semantic backfills

If loaders call embedding providers in a loop, then:

- Separate systemic vs per-item failure; do not abort the corpus on one poison doc.
- Wrap credential/token supply in the same exception normalization as HTTP calls.

## Kafka / oversized aspects

If consumer or emission code handles oversized aspects / `RecordTooLargeException`,
then:

- High when the failure crashes the consumer pod instead of DLQ, skip, or
  validation reject.
