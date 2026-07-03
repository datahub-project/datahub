### Capabilities

#### Foreign keys

Navigation properties are emitted as foreign keys only when a referential constraint is available to map the participating fields — inline on the property for OData V4, or via the referenced `Association` for OData V2. Navigation properties without a resolvable target entity set or referential constraint are counted in the report (`foreign_keys_unresolved`) and skipped rather than guessed.

#### Cross-platform lineage (DRF)

When `drf.enabled` is set, the connector reads the SAP **Data Replication Framework** model to emit lineage from each MDG entity set to the datasets it is replicated to in downstream systems. The replication model (tables `DRFC_APPL` and `DRFC_APPL_SYS`) links a governed **data model** (`USMD_MODEL`, e.g. `BP`) to the target **business systems**; `drf.service_to_data_model` associates each ingested OData service with its data model, and `logical_system_to_platform` resolves each target business system to a downstream DataHub platform (with its `platform_instance`, `env`, and dataset-name casing).

The edge is emitted as a **PATCH** onto the downstream dataset (MDG is the upstream, since it governs and replicates the master data out), so it is added alongside — never overwriting — lineage the downstream's own connector sets. Target business systems without a platform mapping are reported (`unresolved_target_systems`) and skipped rather than guessed.

These tables have no standard OData service, so they are read through a customer-exposed generic table-reader OData service (`drf.table_read_service`, e.g. an `RFC_READ_TABLE`-backed SEGW service) that returns each table as an entity set of JSON rows keyed by column.

#### Column-level lineage

With `drf.emit_column_lineage`, the downstream dataset's schema is read from DataHub and a field-level edge is emitted for each field whose name matches a property of the MDG entity. Matching is case-insensitive (the downstream platform often cases fields differently from MDG, e.g. `PartnerId` vs `partnerid`), while the emitted `schemaField` URNs preserve each side's real field path. This keeps column lineage correct by construction even though MDG cannot see the target's physical schema. It requires a DataHub graph/sink to be available; when absent, dataset-level lineage is still emitted.

### Limitations

- Only metadata declared in the OData `$metadata` document is ingested; row-level data and profiling are not collected.
- Entity types that are not exposed through an entity set are skipped by default. Set `emit_entity_types_without_sets` to `true` to emit them as datasets.
- Foreign keys are resolved within a single service; relationships that span services are not linked.
- DRF lineage requires a generic table-reader service for the DRF customizing tables and assumes the replicated object keeps its name on the target platform (override casing/instance/env per system via `logical_system_to_platform`). Column-level lineage is limited to fields whose names match (case-insensitively) between MDG and the downstream dataset.

### Troubleshooting

#### No datasets are produced

Confirm the `$metadata` document is reachable at `<base_url>/<service>/$metadata` with the supplied credentials and `sap_client`, and that the configured `entity_set_pattern` is not filtering out every entity set. The ingestion report lists scanned, filtered, and failed services.
