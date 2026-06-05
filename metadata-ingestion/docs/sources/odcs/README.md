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
`odcs` platform (a Logical Model), not as a `dataContract` entity. A single ODCS file may
describe multiple tables: each `schema[]` entry becomes its own logical `odcs` Dataset
carrying dataset properties, canonical schema metadata, ownership, tags, and a link to the
source document. When a `schema[]` entry resolves to a **physical** dataset (via
`servers_to_platform` or `physical_urn_overrides`), the source also links the physical
dataset to the logical one with `logicalParent` and materializes the contract's `quality[]`
rules as Assertions against the physical dataset.

:::info Logical Models are in private beta

Logical Models render in the DataHub UI only when the `LOGICAL_MODELS_ENABLED` feature flag
is on (off by default). For a standalone ODCS file with no physical table yet, a default OSS
deployment will ingest the logical `odcs` dataset but not display it until the flag is
enabled, and — under strict gating — it will emit no assertions. See
[Limitations](#limitations) for the recommended "ingest ODCS + the physical platform, enable
the flag" workflow.

:::

## Concept Mapping

The table below lists the ODCS fields covered. Anything not in this table is deliberately
out of scope — see [Limitations](#limitations) on the module page.

| Source Concept                                                  | DataHub Concept                                 | Notes                                                                                                                                                             |
| --------------------------------------------------------------- | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id` + `schema[].name`                                          | Logical `odcs` Dataset URN                      | One logical dataset per `schema[]` entry. URN name defaults to `{contract_id}.{schema_name}` (configurable via `logical_dataset_name_template`).                  |
| `name` + `schema[].name`                                        | `datasetProperties.name`                        | Display name `"<contract.name> — <schema.name>"` when both are present.                                                                                           |
| `id`, `version`, `apiVersion`, `status`, `domain`, …            | `datasetProperties.customProperties` (`odcs.*`) | Provenance keys: `odcs.id`, `odcs.version`, `odcs.apiVersion`, `odcs.status`, `odcs.schemaName`, `odcs.physicalName`, `odcs.sourceFile`, `odcs.qualityRuleCount`. |
| `description.purpose` / `description.usage`                     | `datasetProperties.description`                 | Concatenated when both are present.                                                                                                                               |
| `schema[].properties[]` (`name`, `logicalType`, `physicalType`) | `schemaMetadata.fields[]`                       | Canonical schema on the logical dataset. Types map to a `SchemaFieldDataType`; unmapped types fall back to `NullType` and are reported.                           |
| Property `required` / `primaryKey`                              | `schemaField.nullable` / `isPartOfKey`          | `nullable = not required`. Nested `properties[]` use dotted field paths (`address.city`).                                                                         |
| Property `description` / `tags[]`                               | `schemaField.description` / `globalTags`        | `tag_prefix` is prepended to tags if configured.                                                                                                                  |
| Top-level `tags[]`                                              | `globalTags` on the logical dataset             | Emitted when `replicate_contract_metadata=true` (default).                                                                                                        |
| `team[]` entries (`role`, `username`)                           | `ownership` on the logical dataset              | `role` maps to an OwnershipType; `group:<name>` targets a corpGroup (a DataHub extension, see below). Emitted when `replicate_contract_metadata=true`.            |
| `schema[].authoritativeDefinitions[]`                           | `institutionalMemory`                           | Author-provided URLs linked from the logical dataset.                                                                                                             |
| Physical binding                                                | `logicalParent` on the physical dataset         | The `PhysicalInstanceOf` relationship from physical to logical. Controlled by `emit_logical_parent` (default true).                                               |
| `quality[]` rule (library `unique`/`notNull` with a column)     | `FieldAssertionInfo` + `FieldValuesAssertion`   | **Only when a physical binding resolves.** Targets the physical dataset.                                                                                          |
| `quality[]` rule (library `rowCount`)                           | `VolumeAssertionInfo`                           | Only when a physical binding resolves.                                                                                                                            |
| `quality[]` rule (`type: sql` with `query`)                     | `SqlAssertionInfo`                              | Only when a physical binding resolves.                                                                                                                            |
| All other `quality[]` rules                                     | `CustomAssertionInfo`                           | Original rule body preserved verbatim. See [Quality rule mapping](#quality-rule-mapping).                                                                         |

#### The `group:` ownership prefix is a DataHub extension

ODCS has no group principal — `team[].username` is documented as "username or email" only.
DataHub adds an extension: a `team[].username` of the form `group:<name>` is mapped to a
corpGroup owner (`urn:li:corpGroup:<name>`) rather than a corpUser. This is a DataHub-side
convenience, not part of the ODCS spec; plain usernames and emails continue to map to
corpUser owners.
