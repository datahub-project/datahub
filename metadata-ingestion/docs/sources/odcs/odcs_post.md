### Capabilities

Use the **Important Capabilities** table above as the source of truth for which DataHub
features this source emits. The notes below cover ODCS-specific behavior that the table
does not capture.

#### Quality rule mapping

Each entry in a `schema[]` (table-level) or `properties[]` (column-level) `quality[]` array
becomes a DataHub assertion **attached to the logical `odcs` dataset**, emitted whether or
not a physical binding resolves. The library vocabulary is spec-exact and
version-dependent:

| ODCS rule                                                                         | DataHub aspect                                                                   |
| --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| v3.1 `metric: nullValues` with `mustBe: 0`                                        | `FieldAssertionInfo` + `FieldValuesAssertion` (`NOT_NULL`)                       |
| v3.1 `metric: nullValues` with another threshold                                  | `FieldAssertionInfo` + `FieldMetricAssertion` (`NULL_COUNT` / `NULL_PERCENTAGE`) |
| v3.1 `metric: duplicateValues` / v3.0 `rule: duplicateCount` with `mustBe: 0`     | `FieldAssertionInfo` + `FieldMetricAssertion` (`UNIQUE_PERCENTAGE` == 100)       |
| v3.1 `metric: invalidValues` (`arguments.validValues`) / v3.0 `rule: validValues` | `FieldAssertionInfo` + `FieldValuesAssertion` (`IN`)                             |
| v3.1 `metric: invalidValues` with `arguments.pattern`                             | `FieldAssertionInfo` + `FieldValuesAssertion` (`REGEX_MATCH`)                    |
| `metric: rowCount` (v3.0 `rule: rowCount`)                                        | `VolumeAssertionInfo` (`unit: rows` only)                                        |
| `type: sql` with a `query` and a mappable threshold                               | `SqlAssertionInfo`                                                               |
| `type: custom` (`engine` + `implementation`)                                      | `CustomAssertionInfo` (`type` = the engine, `logic` = the implementation)        |
| `type: text`                                                                      | `CustomAssertionInfo` (`logic` = the description)                                |
| Anything else with content                                                        | `CustomAssertionInfo` (original rule intent preserved as `logic`)                |
| Anything with no operator and no body                                             | Skipped with a warning                                                           |

The v3.1 library key is `metric`; `rule` is accepted as the v3.0 canonical key and as the
deprecated v3.1 alias (v3.1 documents using `rule` get an informational notice). Every
assertion carries ODCS provenance in `customProperties` (`odcs.id`, `odcs.rule.id`,
`odcs.rule.metric`, `odcs.rule.unit`, serialized `odcs.rule.arguments`, dimension /
severity / businessImpact when present), a `dataPlatformInstance` aspect attributing it to
the `odcs` platform, and an `externalUrl` from the rule's `authoritativeDefinitions` when
provided. Assertion URNs are seeded from the spec's `quality.id` when present, so renames
and reordering do not churn identities.

:::warning Routing is exact; nothing is approximated

Tolerances map to typed assertions only when they are exactly representable:

- No threshold at all (the v3.0 `validValues` form) and `mustBe: 0` both mean "no failing
  rows tolerated".
- An integral `mustBeLessOrEqualTo` maps to a fail threshold in rows (`unit: rows`) or
  percent (`unit: percent`).
- Everything else — strict less-than tolerances, non-integral percents,
  `mustNotBeBetween`, duplicate-count tolerances, percent-based `rowCount`, multi-column
  `duplicateValues` (`arguments.properties`), `missingValues` — is preserved as a
  `CustomAssertionInfo` whose `logic` carries a stable rendition of the original rule
  (metric, arguments, thresholds, unit) rather than being approximated into a typed shape.
- Rules routed to custom are listed in `report.rules_routed_to_custom`; rules with no
  modelable content at all are skipped and listed in `report.rules_skipped_no_threshold`.

:::

#### Schema-compliance assertion

For every `schema[]` entry that declares properties, the source emits one `DATA_SCHEMA`
assertion (`SchemaAssertionInfo`) on the logical dataset carrying the contract's declared
schema. This makes schema drift an evaluable contract violation rather than an implied
one. `schema_assertion_compatibility` controls the mode: `SUPERSET` (default — an instance
must contain at least the contract's fields, extras allowed), `EXACT_MATCH`, or `SUBSET`.
Disable with `emit_schema_assertion: false`.

#### Physical dataset binding (the `logicalParent` link)

Binding exists to link physical datasets to their logical model — assertions never depend
on it. Resolution per `schema[]` entry, in priority order:

1. `physical_urn_overrides[<contract id>][<schema entry name>]` — an explicit URN, or an
   empty string to deliberately leave the entry unbound. Entries absent from the map fall
   back to derivation; keys that match no schema entry warn.
2. The contract's first mappable `servers[]` entry. The platform comes from the
   spec-required `servers[].type` (or a matching `server_overrides` entry), and the table
   name (`physicalName`, falling back to `name`) is qualified with the server's own fields
   per the platform's URN convention:

| Server `type` | DataHub platform | Physical name                                                                              |
| ------------- | ---------------- | ------------------------------------------------------------------------------------------ |
| `postgres`    | `postgres`       | `database.schema.table`                                                                    |
| `redshift`    | `redshift`       | `database.schema.table`                                                                    |
| `sqlserver`   | `mssql`          | `database.schema.table`                                                                    |
| `snowflake`   | `snowflake`      | `database.schema.table` (lowercased by default; `lowercase_physical_urns: false` opts out) |
| `bigquery`    | `bigquery`       | `project.dataset.table`                                                                    |
| `databricks`  | `databricks`     | `catalog.schema.table`                                                                     |
| `trino`       | `trino`          | `catalog.schema.table`                                                                     |
| `mysql`       | `mysql`          | `database.table`                                                                           |
| `oracle`      | `oracle`         | not composable — supply a dotted `physicalName` or an explicit override                    |
| anything else | —                | unbound (logical dataset and assertions unaffected)                                        |

A `physicalName` that already contains a dot is used verbatim (assumed pre-qualified) and
counted in `report.physical_names_passthrough`. Missing server fields leave the entry
unbound with an actionable reason — the source never guesses a schema name. When a DataHub
graph is available and `verify_physical_urns_exist` is on (default), derived URNs that do
not exist in DataHub are left unbound with a warning instead of creating stub datasets.
Two schema entries binding the same physical dataset warn: `logicalParent` is
single-valued, so the last writer wins.

#### Data product output ports

In the Bitol model a data product is authored separately (ODPS) and lists its contracts
under `outputPorts` — the interfaces it promises to consumers. ODCS carries only the
back-reference, a free-text `dataProduct` field. With
`emit_data_product_association: true` the source follows that back-reference: the dataset
each contract governs is added to the named Data Product **as an output port**
(`outputPort: true`), which is what the product page's Output Ports module, the lineage
"output ports only" filter, and the search-preview pill read.

The asset added is the **physical dataset** when the contract binds to one — that is the
table consumers actually query, and it renders without the Logical Models beta flag. An
unbound contract contributes its logical `odcs` dataset instead. The DataContract entity
itself is never the asset: DataHub's `DataProductContains` relationship accepts datasets,
jobs, dashboards and the like, not contracts.

The product is expected to exist already, so the `dataProduct` value is resolved against
DataHub in order:

1. a full `urn:li:dataProduct:...` value, used verbatim;
2. `urn:li:dataProduct:<value>`, when a product exists under that id;
3. an exact, case-insensitive match on a product's **display name** — ODCS documents the
   field as the product's name, and a name like `Orders, Retail (EU)` can never be an id
   because commas and parens are structural in DataHub urns.

A value matching nothing is reported (`report.data_products_unresolved`) and skipped rather
than turned into a stub product. A name carried by more than one product is ambiguous and
also skipped — set `dataProduct` to the intended product's id. Values resolved by name are
counted in `report.data_products_resolved_by_name`; with a file sink there is no graph to
resolve against, so the id-derived urn is used unverified.

Set `verify_data_product_exists: false` for a contract-first workflow, where ODCS creates
the product at `urn:li:dataProduct:<value>`, names it after the value, and marks it not
removed. The id is the value itself rather than a generated guid, exactly as
`datahub dataproduct upsert` derives a urn from a product's `id` — so a product ODCS
created can later be taken over from a DataProduct yaml, and a product created that way is
found by ODCS. The name is written only for a product this run creates; on a product that
already exists ODCS never touches its display name.

Every contract naming the same product contributes to one patch per run, so a product
assembled from several contracts gets a single update. Because the patch is additive, ODCS
never clears assets it did not add and the product is never stale-removed — dropping the
`dataProduct` field, or a whole contract, leaves the port in place; remove it in the UI or
via the DataProduct SDK. An asset the product already carries is promoted to an output port
in place.

### Limitations

- ODCS v3.0 and v3.1 only. Contracts reporting v2.x in `apiVersion` are skipped with a
  warning.
- **Logical Models are in private beta** and render in the UI only when
  `LOGICAL_MODELS_ENABLED` is enabled (off by default). The logical datasets and their
  assertions are ingested while the flag is off — they just aren't displayed.
- **Nested-column assertions and field-path resolution.** Field-scoped assertions reference
  the column by its dotted property path (`address.city`), matching the logical dataset's
  own `schemaMetadata`. Propagation onto physical datasets whose connectors encode nested
  struct paths differently is subject to the platform propagation mechanism.
- **Property-level `unique: true` / `required: true` flags are not emitted as
  assertions** — they map into `schemaMetadata` (nullability, keys) and are enforced via
  the schema-compliance assertion; express uniqueness checks as `quality[]` rules
  (`metric: duplicateValues` with `mustBe: 0`) to get a typed field assertion.
- File loading is capped at 5 MB by default (`max_input_file_bytes`). Larger YAML files
  are skipped with a warning before parsing.
- Out of scope: SLA (`slaProperties`), `support`, `price`, v3.1 `relationships` (foreign
  keys), `customProperties` → DataHub `customProperties` (only the `odcs.*` provenance
  subset is emitted), `classification` → `GlossaryTerm` linking, schemaField-level
  `logicalParent` column links, and ODCS export. Spec-valid-but-unmapped fields are
  reported once per file via `report.spec_fields_ignored`. These may land in a follow-up.
- **ODPS documents are not read.** `emit_data_product_association` marks the datasets a
  contract governs as output ports of the product the contract names, but the product side
  of the Bitol model is not ingested: no ODPS file is parsed, so port names, port versions,
  `type`, `sbom`, `inputContracts`, input ports, and management ports are all unavailable,
  and a product's own metadata (description, owners, domain) is never written. Port identity
  is therefore the dataset, not the ODPS `name`/`version` pair, so "one contract across
  several output ports" and "several contracts on one port" remain unrepresentable. A
  contract listed under a product's `inputPorts` rather than `outputPorts` cannot be
  distinguished from ODCS alone; it would be marked as an output port too.
- **Schema validation depends on the bundled JSON Schemas.** The v3.0.2 / v3.1.0 schemas
  are vendored with the plugin, so `strict_validation` normally works out of the box. If a
  contract declares a supported `apiVersion` for which no validator is available (e.g. a
  packaging regression that dropped a schema file), the contract is parsed **without**
  schema checking rather than being rejected — `strict_validation` becomes a no-op for
  those contracts. Confirm the schemas shipped with your install if you rely on strict
  validation as a hard gate.
- **Contract metadata replication**: By default, contract-level ownership and tags are
  written to every logical dataset on each run. If you edit these aspects in the DataHub UI,
  they will be overwritten on the next ingest. Set `replicate_contract_metadata: false` to
  disable replication (useful for one-time enrichment workflows).
- **Mixed-platform `servers[]`**: binding uses the first server whose `type` maps to a
  platform; contracts that reference multiple platforms bind to that one only. Use
  `physical_urn_overrides` for per-table control.

### Troubleshooting

#### Where do the emitted assertions appear?

On the **logical `odcs` dataset** — open it in the UI (requires `LOGICAL_MODELS_ENABLED`)
and look under its **Quality / Assertions** tab. Assertions are not written to physical
datasets by this source; DataHub propagates expectations to physical instances via the
`PhysicalInstanceOf` relationship.

#### My contract produced no logicalParent link

Check `report.unmappable_servers` and the per-entry info messages: the contract may
declare no `servers[]`, use a server `type` with no platform mapping (e.g. `kafka`, `s3`),
or be missing the fields needed to qualify a table name (e.g. a postgres server without
`schema`). With a DataHub graph attached, `report.physical_urns_unverified` counts derived
URNs that were skipped because they do not exist in DataHub yet — ingest the physical
platform first, or set `verify_physical_urns_exist: false` to link optimistically.

#### My logical `odcs` dataset doesn't appear in the UI

Logical Models are in private beta and require the `LOGICAL_MODELS_ENABLED` feature flag
(off by default). Enable it to view logical datasets, their assertions, and their
`logicalParent` links. The metadata is still ingested while the flag is off — it just
isn't displayed. The flag is set on the GMS service (for self-hosted OSS, the
`LOGICAL_MODELS_ENABLED` environment variable on the `datahub-gms` container; on DataHub
Cloud, ask your administrator to enable it).

#### My `missingValues`/`mustNotBeBetween`/vendor rule shows up as a Custom Assertion

Expected. See [Quality rule mapping](#quality-rule-mapping) for the exact typed-assertion
allowlist and the threshold-representability rules. Anything outside them is preserved
verbatim as `CustomAssertionInfo` rather than approximated.

#### I removed a table from my ODCS file but the physical dataset is still in DataHub

That's expected. ODCS owns the logical `odcs` Dataset and the Assertions it emitted; the
physical Dataset belongs to its platform-of-record source (postgres / snowflake / …), and
the `logicalParent` link is a non-destructive enrichment. With `stateful_ingestion`
enabled, removing the `schema[]` entry marks the logical dataset and its assertions
removed on the next ODCS ingest — never the physical dataset.

Note the asymmetry: because the physical dataset is intentionally kept out of the ODCS
stateful checkpoint, the `logicalParent` pointer previously written onto it is **not**
cleared when its logical parent is soft-deleted. The physical dataset therefore retains a
`logicalParent` pointing at a now soft-deleted logical dataset until it is next bound or
manually cleaned up. This is deliberate (ODCS must never mutate a physical dataset it does
not own), but it does leave a stale pointer.

#### My contract owner shows up as an unresolved user

ODCS `team[].username` is a username **or** an email, and DataHub resolves the owner
against whatever identifier your identity source (Okta / Azure AD / SCIM / …) ingests
users under. If the two disagree — the contract says `alice@acme.com` but DataHub knows
the user as `alice`, or vice versa — the ownership reference dangles. Set exactly one of
`strip_owner_email_domain: true` (emails → local part) or `owner_email_domain: acme.com`
(bare usernames → emails) to normalize contract identifiers to your convention; explicit
`urn:li:corpuser:` / `urn:li:corpGroup:` values pass through untouched. When ingesting
through a DataHub graph, owners that don't resolve are flagged in the run report
(`report.owners_unresolved`, one warning per unique owner) — the reference is still
emitted and becomes functional as soon as the user is provisioned.

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
