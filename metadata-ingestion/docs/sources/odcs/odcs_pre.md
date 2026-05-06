### Overview

The `odcs` module ingests Open Data Contract Standard (ODCS) v3.0 and v3.1 YAML files from
a path, directory, or glob, and emits each contract as DataHub metadata: dataset properties,
ownership, top-level and column-level tags, editable schema metadata, assertions derived from
the contract's `quality[]` rules, and a `DataContractProperties` aspect carrying the raw YAML
for audit and round-tripping. ODCS is governed by the Linux Foundation under the Bitol project;
see [bitol.io](https://bitol.io/) and the
[open-data-contract-standard repository](https://github.com/bitol-io/open-data-contract-standard).

:::info Looking for the deprecated `*.dhub.dc.yaml` CLI?

The `datahub datacontract upsert` flow that consumes `*.dhub.dc.yaml` files is unrelated to
this source and is being phased out. To ingest ODCS YAML, use `source.type: odcs` via
`datahub ingest -c <recipe.yml>` instead.

:::

### Prerequisites

**A single ODCS file may describe multiple tables.** Each `schema[]` entry becomes its own
DataHub Dataset (URN derived from `physicalName` plus the platform mapping); each gets its
own DataContract URN; contract-level metadata (description, owners, top-level tags)
replicates to every fanned-out dataset by default. See the Concept Mapping table on the
platform Overview above for per-row replication semantics, and the [Limitations](#limitations)
section below for how to disable replication.

- ODCS v3.0 and v3.1 are supported. Contracts whose `apiVersion` reports v2.x are skipped
  with a warning.
- Each `schema[]` entry must set `physicalName`. Per-entry binding works as follows: the
  `physicalName` on the schema becomes the table name component of that entry's Dataset
  URN; `physicalName` on individual `properties[]` then binds each property to a column on
  that dataset. A `schema[]` entry without `physicalName` is skipped with a warning, but
  other `schema[]` entries in the same file still emit. Logical-name-only contracts (no
  `physicalName` anywhere) produce no datasets.
- Plan a `servers_to_platform` mapping that covers every distinct `servers[].server` value
  present across your contracts. Alternatively, set per-contract `dataset_urn_overrides` to
  bypass server lookup entirely. Contracts that resolve to no platform are reported as
  warnings and skipped.
- Files are loaded leniently by default (`strict_validation: false`) so that contracts using
  deprecated-but-real ODCS forms (e.g. legacy library `rule:` key, top-level `quality[]`)
  are accepted. Unknown YAML fields surface as warnings via `report.unknown_fields_count`
  rather than blocking ingestion. Set `strict_validation: true` to fail on JSON Schema
  violations — recommended for multi-tenant or untrusted directories.
- **Stateful ingestion is scoped to ODCS-owned entities only.** When you remove a `schema[]`
  entry from a contract file, ODCS marks the corresponding `DataContract` and `Assertion`
  URNs as removed. **It does NOT mark the underlying Dataset URN as removed** — the Dataset
  is owned by its platform-of-record source (postgres / snowflake / etc.). Removing a table
  from your ODCS file does not delete the table's Dataset entity from DataHub. When you
  enable `state_file_path` for the first time, the initial run establishes a baseline — no
  soft-deletes are emitted on that run, even if you have removed `schema[]` entries since
  the last unscoped ingestion. Soft-delete behavior activates from the second run onward.
- Symlinks are not followed by default (`follow_symlinks: false`). If your directory layout
  organises contracts using symlinks, set `follow_symlinks: true` deliberately. The default
  is conservative because following symlinks in a shared directory can disclose files
  outside the configured root.
- Files larger than `max_input_file_bytes` (default 5 MB) are skipped with a warning before
  parsing.
