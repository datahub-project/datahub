## Overview

The Open Data Contract Standard (ODCS) is the Linux Foundation / Bitol open YAML
standard for data contracts. It defines a vendor-neutral schema covering a contract's
fundamentals, schema, data quality rules, servers, ownership, tags, SLA, support, and
custom properties. Learn more at the audience-friendly [Bitol home page](https://bitol.io/)
and the canonical [open-data-contract-standard repository](https://github.com/bitol-io/open-data-contract-standard)
(spec text and JSON Schemas).

The DataHub `odcs` source ingests ODCS v3.0 and v3.1 YAML files from a path, directory, or
glob and emits the contract's content as DataHub metadata. A single ODCS file may describe
multiple tables: each `schema[]` entry fans out to its own DataHub Dataset URN (resolved via
`servers_to_platform` + `physicalName`, or `dataset_urn_overrides`) paired with its own
DataContract URN. Each fanned-out dataset receives dataset properties, ownership, top-level
and column-level tags, editable schema metadata, assertions derived from `quality[]` rules,
and a `DataContractProperties` aspect that carries the raw YAML for audit and round-tripping.

## Concept Mapping

The table below lists the ODCS fields covered in MVP. Anything not in this table is
deliberately out of scope for MVP — see [Limitations](#limitations) on the module page.

| Source Concept                                                         | DataHub Concept                                      | Notes                                                                                                                                                                                |
| ---------------------------------------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `id` + `version` (or `dataset_urn_overrides`)                          | `DataContract` URN + `DataContractProperties.entity` | One DataContract URN is emitted per `schema[]` entry, keyed `{entity: dataset_urn, odcs_id: contract_id}`. `entity` is the per-table Dataset URN bound from that schema entry.       |
| `schema[].physicalName`                                                | Dataset URN (table name component)                   | Used as the table name in the bound Dataset URN; required for binding. Schemas without `physicalName` are skipped with a warning, but other schemas in the same file still emit.     |
| `description.purpose` / `description.usage`                            | `datasetProperties.description`                      | Concatenated when both are present. Replicated to every fanned-out dataset when `replicate_contract_metadata=true` (default).                                                        |
| Top-level `tags[]`                                                     | `globalTags` on the Dataset                          | `tag_prefix` is prepended if configured. Replicates to every fanned-out dataset when `replicate_contract_metadata=true` (default).                                                   |
| `team[]` entries with `role` of `owner`/`dataOwner`                    | `ownership`                                          | Other roles are out of MVP and ignored. Replicates to every fanned-out dataset when `replicate_contract_metadata=true` (default).                                                    |
| `schema[].properties[]` (`name`, `logicalType`, `description`)         | `editableSchemaMetadata.editableSchemaFieldInfo`     | Always emitted as `EditableSchemaMetadata` and scoped to the specific dataset URN that the parent `schema[]` entry binds to — field paths are not shared across fanned-out datasets. |
| Property `primaryKey` / `required`                                     | `schemaMetadata.fields[].nullable` + `primaryKeys`   | Binds via `physicalName`; logical-name-only properties cannot bind and are skipped.                                                                                                  |
| Property `tags[]` / `classification`                                   | Column `globalTags` on the `schemaField` URN         | `classification` is currently emitted as a tag, not a Glossary Term (out of MVP).                                                                                                    |
| Full ODCS YAML (verbatim)                                              | `DataContractProperties.rawContract`                 | Capped at `raw_contract_size_limit_bytes` (default 1 MB); larger contracts emit a warning and omit this field. The same raw YAML is attached to each fanned-out DataContract URN.    |
| `quality[]` rule under a `schema[]` entry (library `unique`/`notNull`) | `FieldAssertionInfo` + `FieldValuesAssertion`        | Requires a `column` field on the rule. Bound to the dataset URN of the parent `schema[]` entry only.                                                                                 |
| `quality[]` rule under a `schema[]` entry (library `rowCount`)         | `VolumeAssertionInfo`                                | Bound to the dataset URN of the parent `schema[]` entry.                                                                                                                             |
| `quality[]` rule under a `schema[]` entry (`type: sql` with `query`)   | `SqlAssertionInfo`                                   | Bound to the dataset URN of the parent `schema[]` entry.                                                                                                                             |
| Contract-level `quality[]` rule (no `schema[]` parent)                 | Assertion replicated to every fanned-out dataset     | Each replicated assertion carries `odcs.scope=contract` in `customProperties` so consumers can distinguish per-table from contract-wide rules.                                       |
| All other `quality[]` rules                                            | `CustomAssertionInfo`                                | Original rule body preserved verbatim. See the [Quality rule mapping](#quality-rule-mapping) section for routing rules and structured counters.                                      |
