### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Connector lineage and siblings

Siblings and `UpstreamLineage` to the underlying connector platform (e.g. Oracle, StarRocks, Hive, Iceberg) are emitted only for connectors in the supported mapping, and they rely on the generated dataset URN matching the native platform's own ingestion URN exactly:

- **Identifier case must match.** Trino lowercases identifiers in `information_schema`, so mixed-case (quoted) source identifiers — for example quoted Oracle table/view names — produce lowercased Trino URNs that will not match the native platform's case-preserving URNs. Unquoted (uppercase) Oracle names match correctly.
- **Two-tier vs three-tier.** Oracle defaults to a two-tier (`schema.table`) URN. If you ingest Oracle with `add_database_name_to_urn: true` (three-tier `database.schema.table`), set `connector_database` for that catalog in `catalog_to_connector_details` so the generated URN matches.
- **StarRocks via the mysql connector.** Trino has no native StarRocks connector; StarRocks is typically reached through the mysql connector (reported as `connector_name=mysql`). Set `connector_platform: starrocks` and `connector_database: <starrocks catalog>` on that catalog to link to native `starrocks` datasets.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
