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

#### Missing Connections

By default only enabled Airbyte connections are ingested. Disabled connections are
skipped with no warning. If a connection is missing from DataHub, check whether it
is disabled in Airbyte (or whether its Public API `status` is `"inactive"`). To
ingest disabled connections as well, set `include_inactive_connections: true`.

A successful `GET /api/public/v1/connections/{connectionId}` does not mean DataHub
will ingest that connection. Ingestion only walks connections returned by
`GET /api/public/v1/connections?workspaceId=...` for each workspace allowed by
`workspace_pattern`. Confirm the connection's `workspaceId` matches the recipe,
that `connection_pattern` / `source_pattern` / `destination_pattern` allow it
(those skips are silent), and that it appears in the list response — not only in
a by-id GET. Offset pagination can also skip entries on a mutating Airbyte
instance; see Limitations above.

#### Authentication Errors

Verify that your OAuth2 client credentials are correct and have not expired. For OSS deployments, confirm the API is reachable at the `/api/public/v1` path prefix.

#### Missing or Ambiguous Stream Namespaces

A `Stream Metadata Unavailable` warning means the `/streams` endpoint could not be read (404,
5xx after retries, or a connection error), so stream namespaces and column-level lineage are
skipped for that source. The connection itself is still ingested. For streams without a
namespace already present in the connection catalog, a per-table schema in the connector
config, or a configured `default_schema`, dataset lineage is also skipped — emitting URNs
from the connector-wide schema key during a transient `/streams` blip would leave phantom
datasets and edges that stale-entity removal never reconciles. The warning's context carries
the HTTP status when Airbyte returned one, or notes a network/connection error when there was
no status. On versions that expose `/streams`, a 404 usually means the source is not
accessible to the credentials in the recipe, and a 5xx means Airbyte failed while describing
the source.

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

`default_schema` is only overridden by a namespace reported by Airbyte or a per-table schema in the
connector's own configuration, so it is safe to leave in place after an upgrade — and once it
applies, the warning stops for the streams it covers. It deliberately outranks the connector-wide
schema key, which sometimes holds a database name rather than a schema.

A `Stream Namespace Missing` warning names streams that Airbyte left without a namespace on a
source where it reported one for others, so the Airbyte version is not the cause. Set the schema
for those tables in the Airbyte connector's own configuration, or `default_schema` as above if
every stream on that source shares one schema.

A `Stream Schema Guessed` warning means the same thing happened on a source that replicates
several schemas. One name cannot be right for all of them, so every stream gets the same schema
tier and the streams living elsewhere point at another table's URN. `default_schema` cannot fix
this — only Airbyte knows which stream came from which schema, and it only says so from 1.7.0
onwards, so upgrading is the resolution. Ignore the warning if every stream really does live in
the one schema named in the warning's context.

An `Ambiguous Stream Namespace` warning means several schemas expose a stream with the same
name and Airbyte does not say which configured stream belongs to which schema. DataHub leaves
the namespace unset rather than guessing. Set the schema explicitly on the affected connection
in Airbyte to resolve it.

#### Missing Schema Metadata

If datasets are ingested without schema information, confirm that the Airbyte source supports schema discovery and that the sync catalog is populated in the connection settings.
