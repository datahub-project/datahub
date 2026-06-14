### Capabilities

Use the **Important Capabilities** table above as the source of truth for which DataHub
features this source emits. The notes below cover ODCS-specific behavior that the table
does not capture.

#### Quality rule mapping

When a `schema[]` entry resolves to a physical dataset, the source translates each entry in
the contract's `quality[]` array into a DataHub assertion **against the physical dataset**.
Only the rules listed below map to typed assertions; rules that carry intent but no
DataHub-modeled type are routed to `CustomAssertionInfo` with their original logic preserved.
Rules that carry no executable intent at all are skipped (never fabricated). When no physical
binding resolves, no assertions are emitted at all (strict gating) — the rule count is still
recorded on the logical dataset via `odcs.qualityRuleCount`.

| ODCS `quality[]` rule                | DataHub aspect                                                             |
| ------------------------------------ | -------------------------------------------------------------------------- |
| Library `notNull` (with `column`)    | `FieldAssertionInfo` + `FieldValuesAssertion` (`NOT_NULL`)                 |
| Library `unique` (with `column`)     | `FieldAssertionInfo` + `FieldMetricAssertion` (`UNIQUE_PERCENTAGE` == 100) |
| Library `rowCount`                   | `VolumeAssertionInfo`                                                      |
| `type: sql` with a non-empty `query` | `SqlAssertionInfo`                                                         |
| Anything else with operator or body  | `CustomAssertionInfo` (logic preserved)                                    |
| Anything else with no operator/body  | Skipped with a warning                                                     |

:::warning Routing is explicit; nothing is fabricated

Rules are routed in three buckets, by design:

- **Skipped** (added to `report.rules_skipped_no_threshold`): rules with **no operator
  AND no body** (no `query`, no `implementation`, no description-as-logic). The source
  emits a warning and does not fabricate an operator or threshold.
- **Routed to `CustomAssertionInfo`** (added to `report.rules_routed_to_custom`): rules
  with an operator OR a body but no DataHub-modeled type. The custom assertion's `logic`
  field is set in priority order from `query`, then `implementation`, then `description`.
  This bucket includes:
  - `range`, `freshness`, and any other library rule not in the typed list
  - Vendor-specific rules (Soda, Great Expectations, dbt test names, etc.)
  - A `unique` rule that omits `column` (cannot bind to a single field)
  - A `sql` rule that omits `query` — `logic` falls back to `implementation` or `description`
  - `mustNotBeBetween` — `logic` is set explicitly to `f"value not between {low} and {high}"`
- **Routed to a typed assertion**: only the rules listed in the table above.

ODCS does not constrain library rule names, so the universe of rule shapes is open. The
typed allowlist is intentionally small to avoid silent misinterpretation. If you expected
your rule to land as a typed assertion and it did not, check this list first.

:::

:::tip Verify your contract's rules round-tripped

Compare your contract's rule list to `report.assertions_emitted`,
`report.rules_skipped_no_threshold`, and `report.rules_routed_to_custom` to verify every
rule's intent was preserved. If `report.physical_bindings_resolved` is `0`, no assertions
are emitted regardless of the rule content — add a `servers_to_platform` mapping or a
`physical_urn_overrides` entry to bind the contract to a physical dataset.

:::

#### Schema type mapping

Each `schema[].properties[]` entry becomes a `schemaMetadata` field on the logical dataset.
ODCS `logicalType` (and, as a fallback, `physicalType`) is mapped to a DataHub
`SchemaFieldDataType` (string, number, boolean, date/time, record, array, map, bytes, enum).
The original ODCS type string is always preserved as `nativeDataType`. Types that do not map
to a known DataHub type fall back to `NullType` and are recorded in
`report.schema_type_fallbacks` so the gap is visible rather than silent.

### Limitations

- ODCS v3.0 and v3.1 only. Contracts reporting v2.x in `apiVersion` are skipped with a
  warning.
- **Logical Models are in private beta** and render in the UI only when
  `LOGICAL_MODELS_ENABLED` is enabled (off by default). A standalone ODCS file with no
  physical binding produces a logical dataset that, on a default deployment, is ingested but
  not displayed and carries no assertions. See the Prerequisites above for the recommended
  workflow.
- **Assertions require a physical binding** (strict gating). Quality rules on a contract with
  no resolvable physical dataset produce no assertions; only the `odcs.qualityRuleCount`
  provenance counter is recorded.
- **Nested-column assertions and field-path resolution.** Field-scoped assertions
  (`notNull`, `unique`, and property-scoped custom rules) reference the column by the path
  given in the ODCS contract — `rule.column`, or the dotted property path for nested
  `properties[]` (e.g. `address.city`). These resolve cleanly for top-level columns. For a
  nested column to anchor to a field on the **physical** dataset, that dataset must use the
  same dotted naming; SQL connectors that emit v2-encoded field paths for nested structs will
  not match, and the assertion will have no field anchor in the UI. Top-level columns (the
  common case) are unaffected.
- **Property-level `unique: true` is not yet emitted as an assertion.** Uniqueness expressed
  via the idiomatic ODCS property flag (`properties[].unique: true`) is currently not
  materialized; express it as a `quality[]` rule (`rule: unique` with a `column`) to get a
  `FieldMetricAssertion`. Property-flag support is a planned follow-up.
- **Disabling `emit_assertions` after a prior run soft-deletes earlier assertions.** If a run
  with `emit_assertions: true` is followed by one with `emit_assertions: false` (and
  `state_file_path` set), the assertions from the earlier run fall out of state and are
  marked removed — expected cleanup, but worth knowing.
- File loading is capped at 5 MB by default (`max_input_file_bytes`). Larger YAML files
  are skipped with a warning before parsing.
- Out of scope: SLA, pricing, support channels, `customProperties` → DataHub
  `customProperties` (only the `odcs.*` subset is emitted), `classification` →
  `GlossaryTerm` linking, schemaField-level `logicalParent` (only dataset-level links are
  emitted today), and ODCS export. These may land in a follow-up.
- Real-world contracts using deprecated top-level `quality[]` or the legacy library `rule:`
  key may fail strict JSON-Schema validation. The source defaults to
  `strict_validation: false` so these contracts ingest with warnings rather than rejection.
  Set `strict_validation: true` to opt back into strict JSON-Schema enforcement.
- **Contract metadata replication**: By default, contract-level ownership and tags are
  written to every logical dataset on each run. If you edit these aspects in the DataHub UI,
  they will be overwritten on the next ingest. Set `replicate_contract_metadata: false` to
  disable replication (useful for one-time enrichment workflows).
- **Mixed-platform `servers[]`**: A single contract binds to one DataHub platform via
  `servers_to_platform`. Contracts that reference multiple platforms bind only to the first
  matching mapping; use `physical_urn_overrides` for per-table control. Per-table platform
  binding is a follow-up feature.

### Troubleshooting

#### My contract's quality rules produced no assertions

Assertions are emitted only when a `schema[]` entry resolves to a physical dataset (strict
gating). Check `report.physical_bindings_resolved`: if it is `0`, no binding was found. Add a
`servers_to_platform` mapping that matches the contract's `servers[].server` value, or add a
`physical_urn_overrides` entry for the contract's `id`.

#### My logical `odcs` dataset doesn't appear in the UI

Logical Models are in private beta and require the `LOGICAL_MODELS_ENABLED` feature flag
(off by default). Enable it to view logical datasets and their `logicalParent` links. The
metadata is still ingested while the flag is off — it just isn't displayed. The flag is set
on the GMS service (for self-hosted OSS, the `LOGICAL_MODELS_ENABLED` environment variable on
the `datahub-gms` container; on DataHub Cloud, ask your administrator to enable it). The
source also emits a run-summary warning whenever it produced logical datasets but resolved
no physical bindings, so a "nothing showed up" run is visible in the ingestion report.

#### Where do the emitted assertions appear?

When a physical binding resolves, assertions are attached to the **physical** dataset — open
that dataset in the UI and look under its **Quality / Assertions** tab (not the logical
`odcs` dataset). `report.assertions_emitted` counts how many were produced.

#### My `range`/`freshness`/`<vendor>` quality rule shows up as a Custom Assertion

Expected. See [Quality rule mapping](#quality-rule-mapping) above for the full allowlist
of typed assertions. Anything outside that list is preserved verbatim as
`CustomAssertionInfo` rather than misinterpreted into a typed assertion shape.

#### I removed a table from my ODCS file but the physical dataset is still in DataHub

That's expected. ODCS owns the logical `odcs` Dataset and the Assertions it emitted; the
physical Dataset belongs to its platform-of-record source (postgres / snowflake / …), and
the `logicalParent` link is a non-destructive enrichment. Removing the `schema[]` entry
soft-deletes the logical dataset and its assertions on the next ODCS ingest, but never the
physical dataset.

#### I edited owners on a logical dataset and the next ingest reverted them

By default, contract-level ownership replicates on every ingest, so any UI edits are
overwritten. Set `replicate_contract_metadata: false` in the source config to switch to
first-sight-only emission, which preserves UI edits after the initial ingest. The same
setting also governs top-level tags.

---

For the canonical ODCS spec and JSON Schemas, see the
[open-data-contract-standard repository](https://github.com/bitol-io/open-data-contract-standard).
For an audience-friendly entry point and adopter list, see
[bitol.io](https://bitol.io/).
