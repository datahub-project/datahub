### Overview

The `sigma` module ingests metadata from Sigma into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- Workspaces and workbooks within that workspaces as Container.
- Sigma Datasets as Datahub Datasets.
- Pages as Datahub dashboards and elements present inside pages as charts.
- Sigma Data Models as Datahub Containers, with each Data Model element
  emitted as a Datahub Dataset inside that Container. Intra-Data-Model
  lineage (element→element) and external upstreams (warehouse tables /
  Sigma Datasets) are wired via `UpstreamLineage` on each element Dataset.
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
