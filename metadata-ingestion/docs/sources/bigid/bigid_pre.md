### Overview

The `bigid` module ingests classification and governance metadata from BigID into DataHub. It reads BigID's data catalog, business glossary, classifications, and IDSoR correlation results, then enriches matching DataHub datasets with GlossaryTerms, Tags, risk scores, and profiles.

By default this connector runs in **pure enrichment mode** (`create_datasets: false`): it never emits structural aspects and only augments datasets that already exist in DataHub. Enable `create_datasets` to also emit `DatasetProperties` and `SchemaMetadata` for sources that BigID knows about but DataHub does not.

### Prerequisites

Before running ingestion, ensure you have:

1. **Network connectivity** to your BigID instance over HTTPS.
2. **A BigID token** — either a long-lived `user_token` (exchanged for a short-lived access token at startup) or a short-lived `access_token`. The token needs read access to the catalog, classifications, business glossary, and (if used) IDSoR/results-tuning APIs.
3. **Datasets already present in DataHub** for the sources BigID scans, unless you enable `create_datasets`.

#### Connection-to-Platform Resolution

BigID connection `type` values are mapped to DataHub platform names automatically (for example `rdb-postgresql` → `postgres`, `snowflake` → `snowflake`). Two levers let you override this:

- `datasource_platform_mapping` — per-connection overrides of platform, `env`, and `platform_instance`. Required when a connection's type has no built-in mapping, or when a dataset's URN must match a specific platform instance created by a native connector.
- `connection_pattern` — regex allow/deny patterns matched against the BigID connection name. Use this to scope ingestion to a subset of connections in large deployments that expose hundreds of data sources.

#### Confidence Filtering

Classification findings carry a BigID confidence rank. Ranks map to `HIGH = 0.75`, `MEDIUM = 0.50`, `LOW = 0.25`. Set `minimum_confidence_threshold` (0.0–1.0) to drop low-confidence findings.
