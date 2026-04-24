### Overview

The `sigma` module ingests metadata from Sigma into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- Workspaces and workbooks within that workspaces as Container.
- Sigma Datasets as Datahub Datasets.
- Pages as Datahub dashboards and elements present inside pages as charts.
- Sigma Data Models as Containers, with each element as a Dataset inside the
  Container. Opt-in via `ingest_data_models: true` (default `false`). Each
  element Dataset carries `SchemaMetadata` and `UpstreamLineage` for intra-DM
  element references and external upstreams (Sigma Datasets ingested in the
  same run). Cross-DM lineage and workbook-to-DM element links are not emitted
  in this release and will arrive in a follow-up.

  Notes:

  - Element Dataset URNs are keyed by the immutable Data Model UUID
    (`urn:li:dataset:(sigma,<dataModelId>.<elementId>,env)`) so
    attachments survive Sigma slug rotation; the slug is captured on
    `customProperties.dataModelUrlId`.
  - Column types are emitted as `String` — Sigma's `/columns` API does
    not return per-column types.
  - Column-level lineage is not emitted yet; `SchemaMetadata` is
    present, so CLL can be added later without re-ingestion.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer [doc](https://help.sigmacomputing.com/docs/generate-api-client-credentials) to generate an API client credentials.
2. Provide the generated Client ID and Secret in Recipe.

We have observed issues with the Sigma API, where certain API endpoints do not return the expected results, even when the user is an admin. In those cases, a workaround is to manually add the user associated with the Client ID/Secret to each workspace with missing metadata.
Empty workspaces are listed in the ingestion report in the logs with the key `empty_workspaces`.
