### Overview

The `odcs` module ingests Open Data Contract Standard (ODCS) v3.0 and v3.1 YAML files from
a path, directory, or glob, and models each contract as a **logical dataset** on the `odcs`
platform: dataset properties, canonical schema metadata, ownership, top-level and
column-level tags, and a link to the source document. When a `schema[]` entry resolves to a
physical dataset, the source also emits a `logicalParent` link from the physical dataset and
materializes the contract's `quality[]` rules as Assertions against it. ODCS is governed by
the Linux Foundation under the Bitol project; see [bitol.io](https://bitol.io/) and the
[open-data-contract-standard repository](https://github.com/bitol-io/open-data-contract-standard).

:::info Looking for the deprecated `*.dhub.dc.yaml` CLI?

The `datahub datacontract upsert` flow that consumes `*.dhub.dc.yaml` files is unrelated to
this source and is being phased out. To ingest ODCS YAML, use `source.type: odcs` via
`datahub ingest -c <recipe.yml>` instead.

:::

### Prerequisites

**A single ODCS file may describe multiple tables.** Each `schema[]` entry becomes its own
logical `odcs` Dataset. Contract-level metadata (description, owners, top-level tags)
applies to every logical dataset by default. See the Concept Mapping table on the platform
Overview above for per-row semantics, and the [Limitations](#limitations) section below for
how to disable replication.

- ODCS v3.0 and v3.1 are supported (any `3.0.x` patch level validates against the same
  v3.0.2 JSON Schema; `3.1.0` against the v3.1 schema). Contracts whose `apiVersion` reports
  v2.x — or any value outside `odcs_versions` — are skipped with a warning.
- **A physical binding is optional but recommended.** Map every distinct `servers[].server`
  value in your contracts to a DataHub platform via `servers_to_platform`, or set
  per-contract `physical_urn_overrides` (a list of physical Dataset URNs, one per `schema[]`
  entry). When a `schema[]` entry binds to a physical dataset, the source emits a
  `logicalParent` link and the contract's quality assertions against that dataset. When it
  does **not** bind, only the logical dataset and its schema are emitted — **no assertions
  and no `logicalParent` link** (strict gating). The logical dataset still records the rule
  count via `odcs.qualityRuleCount`.
- **Logical Models are in private beta.** The logical `odcs` datasets this source emits
  render in the UI only when `LOGICAL_MODELS_ENABLED` is enabled (off by default). For the
  common contract-first case (an ODCS file with no physical table yet), a default OSS
  deployment ingests the logical dataset but does not display it and emits no assertions. The
  recommended workflow is: ingest the physical platform source (postgres / snowflake / …),
  ingest ODCS with a matching `servers_to_platform` mapping, and enable
  `LOGICAL_MODELS_ENABLED` to see the logical models and their physical links.
- Files are loaded leniently by default (`strict_validation: false`) so that contracts using
  deprecated-but-real ODCS forms (e.g. legacy library `rule:` key, top-level `quality[]`)
  are accepted. Unknown YAML fields surface as warnings via `report.unknown_fields_count`
  rather than blocking ingestion. Set `strict_validation: true` to fail on JSON Schema
  violations — recommended for multi-tenant or untrusted directories.
- **Stateful ingestion is scoped to ODCS-owned entities only.** When you remove a `schema[]`
  entry from a contract file, ODCS marks the corresponding logical `odcs` Dataset and
  Assertion URNs as removed. **It does NOT mark physical datasets as removed** — those are
  owned by their platform-of-record source, and the `logicalParent` link is a non-destructive
  enrichment. When you enable `state_file_path` for the first time, the initial run
  establishes a baseline — no soft-deletes are emitted on that run. Soft-delete behavior
  activates from the second run onward.
- Symlinks are not followed by default (`follow_symlinks: false`). If your directory layout
  organises contracts using symlinks, set `follow_symlinks: true` deliberately. The default
  is conservative because following symlinks in a shared directory can disclose files
  outside the configured root.
- Files larger than `max_input_file_bytes` (default 5 MB) are skipped with a warning before
  parsing.
