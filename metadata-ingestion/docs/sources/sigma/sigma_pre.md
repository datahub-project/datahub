### Overview

The `sigma` module ingests metadata from Sigma into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- Workspaces and workbooks within that workspaces as Container.
- Sigma Datasets as Datahub Datasets.
- Pages as Datahub dashboards and elements present inside pages as charts.
- Sigma Data Models as Containers, with each element as a Dataset
  inside the Container (opt-in via `ingest_data_models: true`; default
  `false`). `UpstreamLineage` is emitted on each element for intra-DM,
  cross-DM, external (warehouse / Sigma Dataset), and workbook→DM
  references. Personal-space DMs referenced from ingested DMs are
  discovered on demand and gated by `ingest_shared_entities` and
  `data_model_pattern`. Report counters under
  `data_model_*` / `element_dm_*` surface per-shape resolution outcomes.

  Notes:
  - Element Dataset URNs are keyed by the immutable Data Model UUID
    (`urn:li:dataset:(sigma,<dataModelId>.<elementId>,env)`) so
    attachments survive Sigma slug rotation; the slug is captured on
    `customProperties.dataModelUrlId`.
  - Column types are emitted as `String` — Sigma's `/columns` API does
    not return per-column types.
  - Column-level lineage is not emitted yet; `SchemaMetadata` is
    present, so CLL can be added later without re-ingestion.
  - Unresolved upstream references (Sigma Dataset not ingested, DM not
    reachable, element name not matched) are suppressed rather than
    fabricated as dangling URNs. Re-run with broader patterns to
    materialize them.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer [doc](https://help.sigmacomputing.com/docs/generate-api-client-credentials) to generate an API client credentials.
2. Provide the generated Client ID and Secret in Recipe.

We have observed issues with the Sigma API, where certain API endpoints do not return the expected results, even when the user is an admin. In those cases, a workaround is to manually add the user associated with the Client ID/Secret to each workspace with missing metadata.
Empty workspaces are listed in the ingestion report in the logs with the key `empty_workspaces`.
