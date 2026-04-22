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
  Dataset URN by name; if the DM element name cannot be resolved the edge
  falls back to the Data Model Container URN. Set `ingest_data_models:
  false` to skip Data Model ingestion, and use `data_model_pattern` to
  allow/deny individual Data Models by name.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer [doc](https://help.sigmacomputing.com/docs/generate-api-client-credentials) to generate an API client credentials.
2. Provide the generated Client ID and Secret in Recipe.

We have observed issues with the Sigma API, where certain API endpoints do not return the expected results, even when the user is an admin. In those cases, a workaround is to manually add the user associated with the Client ID/Secret to each workspace with missing metadata.
Empty workspaces are listed in the ingestion report in the logs with the key `empty_workspaces`.
