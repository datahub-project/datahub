### Capabilities

Use the **Important Capabilities** table above as the source of truth for which DataHub
features this source emits. The notes below cover ODCS-specific behavior that the table
does not capture.

#### Quality rule mapping

The source translates each entry in a contract's `quality[]` array into a DataHub
assertion. Only the rules listed below map to typed assertions; rules that carry intent
but no DataHub-modeled type are routed to `CustomAssertionInfo` with their original logic
preserved. Rules that carry no executable intent at all are skipped (never fabricated).

| ODCS `quality[]` rule                | DataHub aspect                                |
| ------------------------------------ | --------------------------------------------- |
| Library `unique` (with `column`)     | `FieldAssertionInfo` + `FieldValuesAssertion` |
| Library `notNull` (with `column`)    | `FieldAssertionInfo` + `FieldValuesAssertion` |
| Library `rowCount`                   | `VolumeAssertionInfo`                         |
| `type: sql` with a non-empty `query` | `SqlAssertionInfo`                            |
| Anything else with operator or body  | `CustomAssertionInfo` (logic preserved)       |
| Anything else with no operator/body  | Skipped with a warning                        |

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
rule's intent was preserved. Any rule that ended up in `rules_skipped_no_threshold` had
no executable content and produced no assertion at all.

:::

### Limitations

- ODCS v3.0 and v3.1 only. Contracts reporting v2.x in `apiVersion` are skipped with a
  warning.
- Dataset binding requires `physicalName` on each `schema[].properties[]` entry. Contracts
  expressed only with logical names cannot bind to a DataHub Dataset and are skipped with
  a warning.
- `DataContractProperties.rawContract` is capped at 1 MB by default
  (`raw_contract_size_limit_bytes`). Larger contracts emit the contract aspects without
  `rawContract` and produce a warning.
- File loading is capped at 5 MB by default (`max_input_file_bytes`). Larger YAML files
  are skipped with a warning before parsing.
- Out of MVP: SLA, pricing, support channels, non-owner team roles,
  `customProperties` → DataHub `customProperties` (only the `odcs.*` subset is emitted),
  `classification` → `GlossaryTerm` linking, and ODCS export. These may land in a follow-up.
- Real-world contracts using deprecated top-level `quality[]` or the legacy library `rule:` key may fail strict JSON-Schema validation. The source defaults to `strict_validation: false` so these contracts ingest with warnings rather than rejection. Set `strict_validation: true` to opt back into strict JSON-Schema enforcement.
- **Contract metadata replication under fan-out**: By default, contract-level ownership, description, and tags replicate to every fanned-out dataset on each run. If you edit these aspects in the DataHub UI, they will be overwritten on the next ingest. Set `replicate_contract_metadata: false` to disable replication after first sight (useful for one-time enrichment workflows).
- **Soft-delete scope**: Removing a `schema[]` entry from a contract file marks the corresponding DataContract and Assertion URNs as removed, but **does not mark the underlying Dataset as removed** — the Dataset belongs to its platform-of-record source. To remove the Dataset, remove the table from your warehouse and re-ingest the platform source.
- **Mixed-platform `servers[]`**: A single contract is bound to one DataHub platform via `servers_to_platform`. Contracts that reference multiple platforms (e.g. a postgres `servers[]` entry alongside a snowflake one) bind only to the first matching mapping. Per-table platform binding is a follow-up feature.

#### Dual-format coexistence

If a dataset already has a contract emitted from a `*.dhub.dc.yaml` file (the deprecated
`datahub datacontract upsert` CLI) and you start ingesting from ODCS, both contracts can
target the same Dataset URN. The most-recently-emitted aspect wins. To avoid drift,
deprecate one path before adopting the other; do not run them in parallel against the same
datasets.

### Troubleshooting

#### Source fails with "no platform mapping found"

The contract's `servers[].server` value does not match any entry in your
`servers_to_platform` config and no `dataset_urn_overrides` entry exists for the
contract's `id`. Add a matching `servers_to_platform` entry, add a `dataset_urn_overrides`
entry for that specific contract, or add a `match_any: true` catch-all mapping if a single
platform is appropriate for everything.

#### My `range`/`freshness`/`<vendor>` quality rule shows up as a Custom Assertion

Expected. See [Quality rule mapping](#quality-rule-mapping) above for the full allowlist
of typed assertions. Anything outside that list is preserved verbatim as
`CustomAssertionInfo` rather than misinterpreted into a typed assertion shape.

#### `datahub datacontract upsert` rejected my ODCS file with a deprecation warning or error

That CLI consumes the deprecated `*.dhub.dc.yaml` format and is unrelated to this source.
Use this source instead via `datahub ingest -c <recipe.yml>` with `source.type: odcs`. See
the [recipe](#starter-recipe) above.

#### I removed a table from my ODCS file but the Dataset is still in DataHub

That's expected. ODCS only owns the `DataContract` and `Assertion` URNs it emitted; the
`Dataset` URN belongs to the platform-of-record source (postgres / snowflake / etc.).
Remove the table from your warehouse and re-ingest that source to mark the Dataset
removed. The corresponding DataContract and Assertion URNs that ODCS emitted for that
table will be marked removed on the next ODCS ingest.

#### I edited owners on a fanned-out dataset and the next ingest reverted them

By default, contract-level ownership replicates on every ingest, so any UI edits to
ownership on a fanned-out dataset are overwritten. Set `replicate_contract_metadata: false`
in the source config to switch to first-sight-only emission, which preserves UI edits
after the initial ingest. The same setting also governs description and top-level tags.

---

For the canonical ODCS spec and JSON Schemas, see the
[open-data-contract-standard repository](https://github.com/bitol-io/open-data-contract-standard).
For an audience-friendly entry point and adopter list, see
[bitol.io](https://bitol.io/).
