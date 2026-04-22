### Overview

The `sigma` module ingests metadata from Sigma into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- Workspaces and workbooks within that workspaces as Container.
- Sigma Datasets as Datahub Datasets.
- Pages as Datahub dashboards and elements present inside pages as charts.
- Sigma Data Models as Datahub Containers, with each Data Model element
  emitted as a Datahub Dataset inside that Container. The following
  `UpstreamLineage` shapes are wired on each element Dataset:
  - **Intra-Data-Model** (element → sibling element in the same Data
    Model).
  - **External upstreams** — warehouse tables and Sigma Datasets.
  - **Cross-Data-Model** (element → element in a *different* Data Model,
    e.g. when one Data Model imports a table from another). Resolved via
    the same name-based bridge used for workbook references; surfaced
    through the counters
    `data_model_element_cross_dm_upstreams_resolved`,
    `..._ambiguous`, `..._name_unmatched_but_dm_known`, and
    `..._dm_unknown`.

    Cross-Data-Model references whose producer Data Model is not returned by
    `/v2/dataModels` (typically a personal-space DM with `path: "My Documents"`
    and no workspace) are **discovered on demand** via a direct
    `GET /v2/dataModels/{urlId}` call. Discovered personal-space DMs are
    emitted as full Container + element Dataset entities and flagged with
    `customProperties.isPersonalDataModel = "true"`. If the service-account
    client cannot reach the personal-space DM (HTTP 403 / 404), the edge is
    suppressed and counted under `data_model_external_reference_unresolved`.

  Workbook chart elements that reference a Data Model element (via the
  Sigma app's "use data model" action) resolve to the specific element
  Dataset URN by name. If the DM element name cannot be matched, **no
  lineage edge is emitted** for that reference — `ChartInfo.inputs`
  accepts only Dataset URNs, so a Container URN cannot substitute. The
  occurrence is surfaced through the report counters
  `element_dm_edge_name_unmatched_but_dm_known` (DM was ingested but the
  specific element name did not match) and `element_dm_edge_unresolved`
  (DM itself is not tracked in this run). Set `ingest_data_models: false`
  to skip Data Model ingestion, and use `data_model_pattern` to allow/deny
  individual Data Models by name.

  Column-level lineage (CLL / `FineGrainedLineage`) is **not yet emitted**
  on Data Model element `UpstreamLineage`. Each element Dataset already
  carries its own `SchemaMetadata`, so adding CLL in a follow-up release
  will not require re-ingestion. Sigma column `formula` values are in
  Sigma's own expression DSL (not SQL); name-matched CLL for cross-DM and
  Sigma-Dataset upstreams, and later formula-parsed CLL for intra-DM
  transformations, are tracked as staged follow-ups.

  Element Dataset URNs are keyed by the **immutable** Data Model UUID
  (`urn:li:dataset:(sigma,<dataModelId>.<elementId>,env)`) so DataHub-side
  attachments (owners, glossary terms, lineage, docs) survive a Sigma
  `urlId` slug rotation. The human-readable slug is still captured on the
  Container's and element's `customProperties` as `dataModelUrlId`.

  A note on external upstreams: when a Data Model's `/lineage` API reports
  a `type: dataset` entry whose Sigma Dataset is **not** fetched in this
  ingestion run (e.g. filtered out by `workspace_pattern` or
  `ingest_shared_entities=False`), the DM element's `UpstreamLineage`
  still emits a synthesized Sigma Dataset URN for that reference. The
  URN itself will have no `DatasetProperties`/`Status` in this run, so
  it will appear as a link-only target in the graph until a subsequent
  run ingests the owning workspace. If you prefer to drop such edges
  entirely, restrict your Data Model and workspace patterns to the same
  scope.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer [doc](https://help.sigmacomputing.com/docs/generate-api-client-credentials) to generate an API client credentials.
2. Provide the generated Client ID and Secret in Recipe.

We have observed issues with the Sigma API, where certain API endpoints do not return the expected results, even when the user is an admin. In those cases, a workaround is to manually add the user associated with the Client ID/Secret to each workspace with missing metadata.
Empty workspaces are listed in the ingestion report in the logs with the key `empty_workspaces`.
