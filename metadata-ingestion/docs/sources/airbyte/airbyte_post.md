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

#### Missing or Ambiguous Stream Namespaces

A `Stream Metadata Unavailable` warning means the `/streams` endpoint answered 404, so stream
namespaces and column-level lineage could not be read. Older Airbyte versions have no such
endpoint; on versions that do, the same status means the source is not accessible to the
credentials in the recipe, so check permissions.

A `Stream Namespaces Not Reported` warning means `/streams` described the source's streams but
gave no namespace for any of them, and nothing else supplied a schema for the streams it names,
so their dataset URNs carry no schema tier. Airbyte only reports stream namespaces from 1.7.0
onwards. Either upgrade Airbyte, or set the schema for that source in the recipe:

```yaml
sources_to_platform_instance:
  <airbyte-source-id>:
    platform: mssql
    default_schema: dbo
```

`default_schema` is only used when neither Airbyte nor the connector's own configuration reveals
a schema, so it is safe to leave in place after an upgrade — and once it applies, the warning
stops for the streams it covers.

An `Ambiguous Stream Namespace` warning means several schemas expose a stream with the same
name and Airbyte does not say which configured stream belongs to which schema. DataHub leaves
the namespace unset rather than guessing. Set the schema explicitly on the affected connection
in Airbyte to resolve it.

#### Missing Schema Metadata

If datasets are ingested without schema information, confirm that the Airbyte source supports schema discovery and that the sync catalog is populated in the connection settings.
