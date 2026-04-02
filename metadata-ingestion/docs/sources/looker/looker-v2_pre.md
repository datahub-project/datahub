### Overview

`looker-v2` is a single ingestion source for Looker that covers what the older `looker` and
`lookml` sources covered separately: dashboards, charts, explores, and LookML views in one run.

Both `looker` and `lookml` are deprecated and will emit warnings when used. See the
[migration guide](https://datahubproject.io/docs/generated/ingestion/sources/looker) if you're
moving from either.

### Prerequisites

You need a Looker API client ID and secret. The client must have:

- `access_data` — to list dashboards, looks, and explores
- `explore` — for SQL-based lineage (only needed if `enable_api_sql_lineage: true`)
- `develop` — to read LookML model metadata

If you're enabling `extract_looks: true`, you also need `stateful_ingestion.enabled: true`.
Without stateful ingestion, Looks deleted in Looker will pile up as stale entities in DataHub.
