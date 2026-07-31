### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

- Only models which are used in a Story or an Application will be ingested because there is no dedicated API to retrieve models (only for Stories and Applications).
- Browse Paths for models cannot be created because the folder where the models are saved is not returned by the API.
- Schema metadata is only ingested for Import Data Models because there is no possibility to get the schema metadata of the other model types.
- Lineages for Import Data Models cannot be ingested because the API is not providing any information about it.
- SAP BW, SAP HANA, and SAP Datasphere (Data Warehouse Cloud / `DWC` connections) are supported for ingesting the upstream lineages of Live Data Models - a warning is logged for all other connection types, please feel free to open an [issue on GitHub](https://github.com/datahub-project/datahub/issues/new/choose) with the warning message to have this fixed.
- For SAP Datasphere-backed Live Data Models, SAC exposes the underlying object's name but not its Datasphere **space**. Configure the space per connection via `connection_mapping.<connection_id>.datasphere_space` so the upstream `sap-datasphere` dataset urn (`<space>.<model_name>`) can be built. Set `connection_mapping.<connection_id>.convert_urns_to_lowercase` (default `true`) to match the casing used by your SAP Datasphere connector recipe. Models on connections without a configured `datasphere_space` are skipped with a warning (or set `resolve_datasphere_lineage: false` to disable this entirely).
- **Column-level lineage** to SAP Datasphere is emitted when `resolve_datasphere_column_lineage` is enabled (default `true`). SAC does not expose columns for Live Data Models, so the field list is resolved from the upstream SAP Datasphere dataset's schema in DataHub — this requires a `datahub_api`/graph connection and that **SAP Datasphere has already been ingested**. Because the live model is a passthrough, that schema is mirrored onto the SAC dataset and each field is mapped to itself. When the graph or upstream schema is unavailable it falls back to table-level lineage only.
- For some models (e.g., builtin models) it cannot be detected whether the models are Live Data or Import Data Models. Therefore, these models will be ingested only with the `Story` subtype.

#### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Compatibility across tenant generations

The connector reads stories, applications, and models from the `Resources` OData data endpoints (for example `api/v1/Resources`) directly, rather than discovering them from the tenant's `$metadata` document. This keeps ingestion working across SAP Analytics Cloud tenant generations: newer (CAP-based) tenants no longer describe the `Resources` entity set in `$metadata` (it is replaced there by a non-queryable `RESOURCES_INDEX` catalog), but the `Resources` data endpoints remain available and are what the connector uses.
