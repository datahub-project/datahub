## Overview

The Open Data Contract Standard (ODCS) is the Linux Foundation / Bitol open YAML
standard for data contracts. It defines a vendor-neutral schema covering a contract's
fundamentals, schema, data quality rules, servers, ownership, tags, SLA, support, and
custom properties. Learn more at the audience-friendly [Bitol home page](https://bitol.io/)
and the canonical [open-data-contract-standard repository](https://github.com/bitol-io/open-data-contract-standard)
(spec text and JSON Schemas).

ODCS v3 describes a **producer-published dataset specification** — what a dataset _should_
look like — rather than a bilateral agreement between a specific producer and consumer.
The DataHub `odcs` source therefore models each contract as a **logical dataset** on the
`odcs` platform (a Logical Model); a native `dataContract` entity referencing its assertions is
emitted alongside it by default. A single ODCS file may
describe multiple tables: each `schema[]` entry becomes its own logical `odcs` Dataset
carrying dataset properties, canonical schema metadata, ownership, tags, a link to the
source document, one Assertion per `quality[]` rule, and one schema-compliance assertion —
all attached to the **logical** dataset. When a `schema[]` entry resolves to a physical
dataset (derived from the contract's typed `servers[]`), the source also links the
physical dataset to the logical one with `logicalParent` (the `PhysicalInstanceOf`
relationship). Propagation of the contract's expectations onto physical instances is
handled by DataHub through that relationship — the source itself never writes assertions
against physical datasets.

These ODCS datasets live on a data platform explicitly marked as logical.

:::info Logical Models are in private beta

Logical Models render in the DataHub UI only when the `LOGICAL_MODELS_ENABLED` feature flag
is on (off by default). The logical `odcs` datasets — and the assertions attached to them —
are ingested either way, but are not displayed until the flag is enabled. See
[Limitations](#limitations) on the module page for the recommended workflow.

:::

## Concept Mapping

The table below lists the ODCS fields this source maps. Fields not in the table are not mapped;
see [Limitations](#limitations) on the module page for which of them are reported and which are
not. ODCS v3.2 support is partial: v3.2 contracts are accepted and validated, and only the v3.2
additions listed here are mapped.

| Source Concept                                                                | DataHub Concept                                                                  | Notes                                                                                                                                                                                                        |
| ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `id` + `schema[].name`                                                        | Logical `odcs` Dataset URN                                                       | One logical dataset per `schema[]` entry. URN name defaults to `{contract_id}.{schema_name}` (configurable via `logical_dataset_name_template`).                                                             |
| `name` + `schema[].name`                                                      | `datasetProperties.name`                                                         | Display name `"<contract.name> — <schema.name>"` when both are present.                                                                                                                                      |
| `id`, `version`, `apiVersion`, `status`, `domain`, …                          | `datasetProperties.customProperties` (`odcs.*`)                                  | Provenance keys: `odcs.id`, `odcs.version`, `odcs.apiVersion`, `odcs.status`, `odcs.schemaName`, `odcs.physicalName`, `odcs.sourceFile`, `odcs.qualityRuleCount`.                                            |
| `schema[].description`, `dataGranularityDescription`, contract `description`  | `datasetProperties.description`                                                  | Joined in that order: the table description, `**Grain:**`, then the contract's `purpose` / `usage` / `limitations`.                                                                                          |
| `schema[].properties[]` (`name`, `logicalType`, `physicalType`)               | `schemaMetadata.fields[]`                                                        | Canonical schema on the logical dataset. Types map to a `SchemaFieldDataType`; unmapped types fall back to `NullType` and are reported.                                                                      |
| Property `required` / `primaryKey`                                            | `schemaField.nullable` / `isPartOfKey`                                           | `nullable = not required`. Nested `properties[]` use dotted field paths (`address.city`).                                                                                                                    |
| Property `description` / `tags[]`                                             | `schemaField.description` / `globalTags`                                         | `tag_prefix` is prepended to tags if configured.                                                                                                                                                             |
| v3.2 `context` (contract, schema object)                                      | `datasetProperties.description`                                                  | Appended as a `**Guidance for AI and analysts:**` section: instructions, `**Constraints:**`, `**Verified Q&A:**`.                                                                                            |
| v3.2 property `enum`                                                          | `schemaField.description`                                                        | Appended as an `**Allowed values:**` list with each value's `label` and `description`.                                                                                                                       |
| Property `transformDescription` / `transformLogic`                            | `schemaField.description`                                                        | Appended as a `**Derivation:**` section, the logic in a SQL code block.                                                                                                                                      |
| Top-level `tags[]`                                                            | `globalTags` on the logical dataset                                              | Emitted when `replicate_contract_metadata=true` (default).                                                                                                                                                   |
| `team[]` entries (`role`, `username`)                                         | `ownership` on the logical dataset                                               | `role` maps to an OwnershipType; roles the source does not know default to Technical Owner and are reported as an info. Members with `dateOut` are skipped. Emitted when `replicate_contract_metadata=true`. |
| `authoritativeDefinitions[]` (root, schema, and property level)               | `institutionalMemory`                                                            | Author-provided URLs linked from the logical dataset.                                                                                                                                                        |
| Typed `servers[]` entry                                                       | `logicalParent` on the physical dataset                                          | The `PhysicalInstanceOf` relationship from physical to logical. Platform derives from the spec-required `servers[].type`; names are fully qualified per platform.                                            |
| v3.2 `logicalType: vector`                                                    | `ArrayType` schema field                                                         | Vector dimensions, element type and model are not recorded.                                                                                                                                                  |
| v3.2 `deprecated` (schema object, property)                                   | `deprecation` on the logical dataset / schema field                              | Explicit `false` clears it; an absent key emits nothing.                                                                                                                                                     |
| v3.2 property `synonyms[]`                                                    | `aiContext.synonyms` on the schema field                                         | Replaces the field's synonyms. `status: deprecated` entries skipped; `locale` / `source` dropped.                                                                                                            |
| v3.2 `semanticType: measure`                                                  | Kept in `schemaMetadata`, left out of the schema-compliance assertion            | A measure is an aggregate, not a physical column. No metric entity is created.                                                                                                                               |
| `schema[].properties[]` (entire declared schema)                              | `SchemaAssertionInfo` (`DATA_SCHEMA` assertion) on the logical dataset           | Pins the contract's schema so drift is an evaluable violation. Compatibility defaults to `SUPERSET` (configurable).                                                                                          |
| v3.1 `metric: nullValues` with `mustBe: 0`                                    | `FieldAssertionInfo` + `FieldValuesAssertion` (`NOT_NULL`)                       | Targets the logical dataset.                                                                                                                                                                                 |
| v3.1 `metric: nullValues` with another threshold                              | `FieldAssertionInfo` + `FieldMetricAssertion` (`NULL_COUNT` / `NULL_PERCENTAGE`) | `unit: percent` selects the percentage metric.                                                                                                                                                               |
| v3.1 `metric: duplicateValues` / v3.0 `rule: duplicateCount` with `mustBe: 0` | `FieldAssertionInfo` + `FieldMetricAssertion` (`UNIQUE_PERCENTAGE` == 100)       | Zero duplicates ⇔ fully unique column.                                                                                                                                                                       |
| v3.1 `metric: invalidValues` / v3.0 `rule: validValues`                       | `FieldAssertionInfo` + `FieldValuesAssertion` (`IN` or `REGEX_MATCH`)            | Allowed values from `arguments.validValues` (or the v3.0 direct `validValues` list); `arguments.pattern` maps to `REGEX_MATCH`.                                                                              |
| `metric: rowCount` (v3.0 + v3.1)                                              | `VolumeAssertionInfo`                                                            | `unit: rows` only; percent-based row counts are preserved as custom assertions.                                                                                                                              |
| `type: sql` with a `query` and a mappable threshold                           | `SqlAssertionInfo`                                                               | The query's metric is compared with the `mustBe*` operator.                                                                                                                                                  |
| All other `quality[]` rules                                                   | `CustomAssertionInfo`                                                            | Original rule intent preserved verbatim as `logic`. See [Quality rule mapping](#quality-rule-mapping).                                                                                                       |
