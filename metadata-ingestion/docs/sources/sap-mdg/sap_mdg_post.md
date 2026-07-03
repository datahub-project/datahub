### Capabilities

#### Foreign keys

Navigation properties are emitted as foreign keys only when a referential constraint is available to map the participating fields — inline on the property for OData V4, or via the referenced `Association` for OData V2. Navigation properties without a resolvable target entity set or referential constraint are counted in the report (`foreign_keys_unresolved`) and skipped rather than guessed.

#### Cross-platform target mapping

`logical_system_to_platform` maps an MDG logical-system / business-system code to the downstream DataHub platform it corresponds to, including its `platform_instance`, `env`, and dataset-name casing. Codes not listed fall back to a small set of well-known SAP platforms. This mapping resolves the correct downstream dataset urns for cross-platform lineage; see the limitation below on lineage edge emission.

### Limitations

- Only metadata declared in the OData `$metadata` document is ingested; row-level data and profiling are not collected.
- Entity types that are not exposed through an entity set are skipped by default. Set `emit_entity_types_without_sets` to `true` to emit them as datasets.
- Foreign keys are resolved within a single service; relationships that span services are not linked.
- Cross-platform lineage: the `logical_system_to_platform` mapping resolves downstream targets, but lineage **edges** are not emitted yet. MDG replication/distribution relationships live in the Data Replication Framework (DRF) and Key Mapping services — not in `$metadata` — so edge emission depends on connecting that endpoint.

### Troubleshooting

#### No datasets are produced

Confirm the `$metadata` document is reachable at `<base_url>/<service>/$metadata` with the supplied credentials and `sap_client`, and that the configured `entity_set_pattern` is not filtering out every entity set. The ingestion report lists scanned, filtered, and failed services.
