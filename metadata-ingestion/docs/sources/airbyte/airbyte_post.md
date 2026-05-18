### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Lineage

Column-level lineage is extracted from Airbyte's sync catalog when field mapping information is available in the connection configuration. Table-level lineage is always captured between source and destination datasets.

#### Job History

Connection job execution history is ingested as `DataProcessInstance` entities, capturing run status, start time, and duration for each sync job.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by Airbyte.

- Schema information is only available for sources that expose a sync catalog. Sources without schema discovery will produce datasets without schema metadata.
- Column-level lineage requires field mapping to be configured in the Airbyte connection.
- Job history depth is limited by the Airbyte API's pagination and retention settings.
- The Airbyte Public API only supports `limit` + `offset` pagination on list endpoints; cursor pagination is not exposed. Ingestion runs against an actively-mutating Airbyte instance may therefore skip or double-count entries inserted or deleted mid-scan. Schedule ingestion during quiet periods if exactness is required.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Authentication Errors

Verify that your OAuth2 client credentials are correct and have not expired. For OSS deployments, confirm the API is reachable at the `/api/public/v1` path prefix.

#### Missing Schema Metadata

If datasets are ingested without schema information, confirm that the Airbyte source supports schema discovery and that the sync catalog is populated in the connection settings.
