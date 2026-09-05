## Overview

DataHub Analytics Compaction is a DataHub maintenance utility for the pgAnalytics
store. It is intended for operators running DataHub with
`DATAHUB_USAGE_EVENTS_IMPLEMENTATION=postgres`.

The DataHub integration does not ingest external catalog metadata. It posts to
GMS `POST /openapi/operations/analytics/compact` so sealed hourly usage metrics
can be compacted into day and month rollups used by GraphQL analytics.

## Concept Mapping

| Source Concept                        | DataHub Concept                  | Notes                                                     |
| ------------------------------------- | -------------------------------- | --------------------------------------------------------- |
| Compact API invocation                | Operational maintenance signal   | Side-effect source; emits no metadata workunits.          |
| Hour seals / day and month rollups    | pgAnalytics compacted aggregates | Produced by GMS compaction for GraphQL analytics queries. |
| Optional lookback / wall-clock budget | Source config overrides          | Forwarded as compact request body fields when set.        |

The default SYSTEM source is bootstrapped with `changeType: UPSERT`. System-update skips that
template until its bootstrap version or `DATAHUB_ANALYTICS_COMPACTION_BOOTSTRAP_REVISION`
changes. Persist schedule or recipe overrides with
`DATAHUB_ANALYTICS_COMPACTION_BOOTSTRAP_VALUES` rather than the UI. Details:
[pgAnalytics design](../../../../docs/pganalytics-design.md).
