### Overview

The `datahub-analytics-compaction` module is a thin SYSTEM source that triggers
one GMS analytics compaction call per run. DataHub bootstraps a scheduled
ingestion source for this module when pgAnalytics compaction should run on a
cron (hourly by default).

### Prerequisites

- GMS with an analytics compaction backend registered (pgAnalytics).
- Authentication that can call `POST /openapi/operations/analytics/compact`
  (the SYSTEM executor uses the DataHub system client).
- Network connectivity from the actions/executor runtime to GMS.
