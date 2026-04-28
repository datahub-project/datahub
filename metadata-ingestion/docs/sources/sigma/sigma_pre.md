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
  - Column types are emitted as `NullType` with `nativeDataType: "unknown"` —
    Sigma's `/columns` API does not return per-column types today. Earlier
    pre-releases hardcoded `String`; that was a lie (no Sigma side confirms
    it) and has been softened so downstream type-aware features can tell
    "unknown" from "actually a string."
  - Column-level lineage is not emitted yet; `SchemaMetadata` is
    present, so CLL can be added later without re-ingestion.
  - External upstreams to Sigma Datasets only resolve when the
    referenced dataset is ingested in the same recipe run. Splitting
    Sigma Datasets and Data Models into separate recipes will leave
    those upstreams unresolved. The report tracks these under
    `data_model_element_upstreams_unresolved_external` (split out from
    `data_model_element_upstreams_unknown_shape`, which counts source_id
    shapes this release does not parse — e.g. cross-DM refs). The
    aggregate `data_model_element_upstreams_unresolved` is kept for
    dashboards that already read it. Keep Sigma Datasets and Data Models
    in the same recipe, or tolerate the gap, until a follow-up adds an
    opt-in URN-pattern fallback.
  - Setting `ingest_data_models: true` issues `/dataModels/{id}/elements`
    and `/columns` calls per DM unconditionally, but the per-DM
    `/lineage` call is gated on `extract_lineage: true`. If you opt out
    of lineage at the workbook surface, the DM connector also stops
    hitting any `/lineage` endpoint — DM Containers, element Datasets,
    and `SchemaMetadata` are still emitted, but without `UpstreamLineage`.
  - The DM Container URN is keyed on `platform` and
    `platform_instance` but not `env`. Multi-environment deployments
    against the same tenant should set a distinct `platform_instance`
    per env so DM Containers do not collide on a single URN. Element
    Datasets are already env-scoped.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer [doc](https://help.sigmacomputing.com/docs/generate-api-client-credentials) to generate an API client credentials.
2. Provide the generated Client ID and Secret in Recipe.

We have observed issues with the Sigma API, where certain API endpoints do not return the expected results, even when the user is an admin. In those cases, a workaround is to manually add the user associated with the Client ID/Secret to each workspace with missing metadata.
Empty workspaces are listed in the ingestion report in the logs with the key `empty_workspaces`.
