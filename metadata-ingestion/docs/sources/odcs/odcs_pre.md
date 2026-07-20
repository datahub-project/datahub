### Overview

The `odcs` module ingests Open Data Contract Standard (ODCS) v3.0 and v3.1 YAML files from
a path, directory, or glob, and models each contract as a **logical dataset** on the `odcs`
platform: dataset properties, canonical schema metadata, ownership, top-level and
column-level tags, a link to the source document, the contract's `quality[]` rules as
Assertions attached to the logical dataset, and a FRESHNESS assertion derived from the
contract's `slaProperties[]` `frequency` SLA. When a `schema[]` entry resolves to a physical
dataset (derived from the contract's typed `servers[]`), the source also emits a
`logicalParent` link from the physical dataset to the logical one. ODCS is governed by
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
- **Assertions always attach to the logical dataset.** Quality rules, the
  schema-compliance assertion, and the freshness SLA assertion are emitted whether or not a
  physical table exists yet, so contract-first workflows keep their expectations.
  Propagation of those expectations onto bound physical datasets is handled by DataHub via
  the `PhysicalInstanceOf` relationship — not by this source.
- **Freshness comes from the `frequency` SLA.** A contract-level `slaProperties[]` entry
  with `property: frequency` (e.g. `value: 1`, `unit: d`) becomes a `DATASET_CHANGE`
  freshness assertion on a fixed-interval schedule, referenced by the native data contract.
  Other SLA dimensions (latency, retention, availability windows) are not modeled, and a
  `frequency` whose `unit` is not a calendar interval is skipped rather than guessed.
- **Physical binding is derived from the contract itself.** The spec requires
  `servers[].type`; the source maps supported types (postgres, mysql, snowflake, bigquery,
  redshift, databricks, sqlserver, trino) to DataHub platforms and composes fully-qualified
  dataset names from the server's own fields (e.g. `database.schema.table`). Use
  `server_overrides` to refine `env` / `platform_instance`, or `physical_urn_overrides`
  (keyed by contract id, then `schema[]` entry name) for explicit URNs. Binding affects only
  the `logicalParent` link.
- **Derived physical URNs are verified by default.** With a DataHub graph available
  (datahub-rest sink), a derived URN that does not exist in DataHub is left unbound with a
  warning instead of creating a stub dataset (`verify_physical_urns_exist: false` opts out).
  With a file sink there is no graph, and links are emitted without verification.
- **Logical Models are in private beta.** The logical `odcs` datasets this source emits —
  and the assertions attached to them — render in the UI only when `LOGICAL_MODELS_ENABLED`
  is enabled (off by default). The metadata is ingested either way. The recommended workflow
  is: ingest the physical platform source (postgres / snowflake / …), ingest ODCS, and
  enable `LOGICAL_MODELS_ENABLED` to see the logical models, their assertions, and their
  physical links.
- Files are loaded leniently by default (`strict_validation: false`) so that contracts with
  extra or non-conformant fields are accepted with warnings. Spec-valid fields the source
  does not map (SLA, support, pricing, relationships, …) are summarized once per file as an
  info; genuinely unknown fields warn individually. Set `strict_validation: true` to fail on
  JSON Schema violations — recommended for multi-tenant or untrusted directories.
- **Stale-metadata removal uses standard stateful ingestion.** Enable it via the
  `stateful_ingestion` block (server-side checkpoints; requires a DataHub graph). When you
  remove a `schema[]` entry from a contract file, the corresponding logical `odcs` Dataset
  and Assertion URNs are marked removed on the next run. Physical datasets and their
  `logicalParent` links are **never** marked removed — those are owned by their
  platform-of-record source. The `fail_safe_threshold` guard (default 75%) blocks mass
  deletions caused by config or naming changes.
- Symlinks are not followed by default (`follow_symlinks: false`). If your directory layout
  organises contracts using symlinks, set `follow_symlinks: true` deliberately. The default
  is conservative because following symlinks in a shared directory can disclose files
  outside the configured root.
- Files larger than `max_input_file_bytes` (default 5 MB) are skipped with a warning before
  parsing.
