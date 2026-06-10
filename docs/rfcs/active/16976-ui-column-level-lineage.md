- Start Date: 2026-04-10
- RFC PR: https://github.com/datahub-project/datahub/pull/16976
- Discussion Issue: (TBD)
- Implementation PR(s): (leave empty)

# UI-authored Column-Level Lineage

## Summary

Allow DataHub users to create, edit, and delete column-level lineage (CLL) edges
directly from the DataHub UI, in the same way table-level lineage can already be
manually edited today. User-authored CLL must coexist with CLL produced by
ingestion sources without one silently overwriting the other.

## Basic example

A user navigates to a dataset's _Lineage_ tab, switches to the column view,
picks an upstream dataset, and draws an edge from `orders.customer_id` to
`orders_enriched.customer_id`. The result is a new `fineGrainedLineage` entry
persisted on the downstream dataset's `upstreamLineage` aspect, annotated with a
UI provenance marker so that subsequent ingestion runs do not overwrite it.

GraphQL sketch:

```graphql
mutation updateFineGrainedLineage {
  updateFineGrainedLineage(
    input: {
      edgesToAdd: [
        {
          downstreamField: "urn:li:schemaField:(urn:li:dataset:(...orders_enriched...),customer_id)"
          upstreamFields: [
            "urn:li:schemaField:(urn:li:dataset:(...orders...),customer_id)"
          ]
          upstreamType: FIELD_SET
          transformOperation: "IDENTITY"
          confidenceScore: 1.0
        }
      ]
      edgesToRemove: []
    }
  )
}
```

## Motivation

Column-level lineage is one of DataHub's most valuable features for impact
analysis, data-quality triage, and compliance (e.g. PII propagation). Today CLL
is almost exclusively produced by automated ingestion (dbt manifests, warehouse
query-log parsers, BI connectors). This creates gaps:

- **Coverage gaps.** Sources without a query log or a manifest (hand-written
  Spark jobs, stored procedures, vendor tools, legacy ETL) leave datasets with
  correct table-level lineage but no columns.
- **Correction gaps.** Automated parsers occasionally produce wrong or partial
  CLL; data stewards today have no way to fix it in-product.
- **Knowledge capture.** Domain experts often know column-level relationships
  that no parser can recover; there is nowhere to put that knowledge.
- **Parity with table-level lineage.** Table edges can already be edited from
  the UI. The column view is read-only, which is inconsistent.
- **Hidden structured metadata.** `FineGrainedLineage` already carries
  `transformOperation` and `confidenceScore`, but today neither is surfaced
  in the UI — not even in read-only mode. Ingested edges arrive with useful
  metadata (e.g. `CAST`, `confidenceScore=0.6`) that users cannot see, so
  they cannot judge how much to trust a parser-produced edge.
- **No place for unstructured notes.** Stewards regularly want to annotate an
  edge with context that does not fit the structured fields ("this mapping
  is only valid for partition > 2024-01", "applied only when
  `region='EU'`"). The model has nowhere to put that today.

The expected outcome is that stewards can treat CLL as a first-class editable
asset, closing coverage gaps without waiting for an ingestion-side fix.

## Requirements

- Users with the appropriate privilege can **add**, **remove**, and **update**
  column-level lineage edges from the UI.
- **All users** (not just editors) can **see** the structured metadata that
  already exists on every edge — `transformOperation`, `confidenceScore`,
  `origin`, `actor`, `createdOn`, and the new `description` field — in the
  column lineage view, via hover tooltip and/or a read-only detail panel.
- Users with edit privilege can **set** `transformOperation`,
  `confidenceScore`, and a free-form `description` on any edge they create
  or own.
- User edits **must survive subsequent ingestion runs** of the affected
  datasets. Ingestion must not silently delete edits.
- Conversely, **ingested edges must not silently delete user edits** and vice
  versa; conflicts must be resolvable.
- Every edge must carry provenance (who created it, when, via which actor type
  — UI/ingestion/API) for audit and rollback.
- The feature must be available via the same transport layers as table-level
  lineage edits: **GraphQL mutation**, **OpenAPI v3**, and **Python SDK**.
- Permissions must be governed by an existing or new Metadata Privilege
  (`EDIT_LINEAGE` or a new `EDIT_COLUMN_LINEAGE`).
- The UI must scale to datasets with hundreds of columns without degrading the
  existing lineage visualization.

### Extensibility

- The storage model should accommodate **future edge attributes** such as
  confidence score, transformation DSL, or ML-generated suggestions.
- The provenance model should accommodate **multiple co-existing sources** per
  edge (e.g. both dbt and a human steward assert the same edge).
- The mutation API should be reusable by **bulk import tools** (CSV, YAML,
  notebook) without UI involvement.

## Non-Requirements

- Rich transformation expression language (SQL/DSL) in the UI — out of scope;
  treat the `transformOperation` field as free text for now.
- Lineage approval workflows (propose → review → accept). Discussed in Future
  Work.
- Cross-entity column lineage (e.g. dataset → MLFeature). Reuse the same model
  in later RFCs.
- Retroactive rewriting of historic CLL entries that lack provenance metadata.
- Mobile / small-screen UX.

## Detailed design

### 1. Storage model

CLL lives today on the dataset `upstreamLineage` aspect as
`fineGrainedLineages: array[FineGrainedLineage]`. Each `FineGrainedLineage`
contains `upstreamType`, `upstreams`, `downstreamType`, `downstreams`,
`transformOperation`, and `confidenceScore`.

The aspect today has no per-edge provenance, and there is no field for
free-form human notes. This RFC proposes four additive fields on
`FineGrainedLineage`:

```pdl
record FineGrainedLineage {
  ...existing fields...

  /** The actor who last asserted this edge. */
  @Searchable = { "fieldName": "fineGrainedActor" }
  actor: optional Urn

  /** When this edge was last asserted (ms since epoch). */
  createdOn: optional long

  /** Transport that produced this edge: UI, INGESTION, API, SYSTEM. */
  origin: optional enum LineageOrigin { UI, INGESTION, API, SYSTEM }

  /**
   * Free-form human description of this edge. Used for context that does not
   * fit the structured `transformOperation` / `confidenceScore` fields
   * (e.g. "only valid for partition > 2024-01", "applies only when
   * region='EU'"). Rendered in the UI detail panel alongside the structured
   * fields.
   */
  description: optional string
}
```

All four fields are optional so existing data remains valid. Writers that
omit them behave as today.

Note on the **existing** fields `transformOperation` and `confidenceScore`:
no schema change is needed for these — they are already on the record. What
this RFC changes is that they (and the new `description`) are now
**first-class in the UI**, not just in the write path (see §5).

### 2. Merge semantics between UI and ingestion

This is the hard problem. Three merge rules must hold simultaneously:

1. Ingestion may **add** edges it discovers.
2. Ingestion may **remove** edges it previously asserted (it is the source of
   truth for what it can parse) — but only edges whose `origin = INGESTION`
   and whose `actor` matches the ingestion source URN.
3. UI edits (`origin = UI`) are opaque to ingestion: never removed by an
   ingestion MCP, only by another UI action or an explicit API call.

Because `upstreamLineage` is stored as a single aspect, a naive overwrite from
an ingestion MCE would clobber UI edits. We therefore require ingestion to
write CLL **as a patch aspect** (DataHub already supports JSON-patch MCPs
via `changeType: PATCH`). Ingestion sources must migrate to emit
`fineGrainedLineages` as an add/remove patch keyed on
`(downstreams, upstreams, transformOperation)` rather than a full-aspect
overwrite.

Sources that still emit `UPSERT` full-aspect writes will be handled by a
server-side merge hook in the Metadata Service: on `UPSERT`, edges whose
`origin != INGESTION` (or whose `actor` differs from the asserting source) are
re-merged into the resulting aspect before commit. This preserves UI edits even
for legacy sources, at the cost of one extra read per write.

### 3. API surface

Three transports, one underlying merge function.

**GraphQL.** New mutation `updateFineGrainedLineage(input:
UpdateFineGrainedLineageInput!)` on the existing `Mutation` type. Input:

```graphql
input UpdateFineGrainedLineageInput {
  edgesToAdd: [FineGrainedLineageEdgeInput!]!
  edgesToRemove: [FineGrainedLineageEdgeInput!]!
}

input FineGrainedLineageEdgeInput {
  downstreamField: String! # schemaField URN
  upstreamFields: [String!]! # schemaField URNs
  upstreamType: FineGrainedLineageUpstreamType
  downstreamType: FineGrainedLineageDownstreamType
  transformOperation: String
  confidenceScore: Float
  description: String
}
```

The resolver loads the current `upstreamLineage` aspect on the downstream
dataset, applies the add/remove set with `origin = UI` and
`actor = <current user>`, and writes back via a patch MCP.

**OpenAPI v3.** New endpoint
`POST /openapi/v3/lineage/fine-grained/{urn}:patch` accepting the same
add/remove shape. This is a thin wrapper over the same merge function.

**Python SDK.** Add `DataHubGraph.update_fine_grained_lineage(downstream,
edges_to_add, edges_to_remove)` that calls the OpenAPI endpoint. Also expose
the same function on the emitter so bulk tools can use it.

### 4. Authorization

Introduce `EDIT_LINEAGE_FIELDS` privilege. Grant it by default to roles that
already hold `EDIT_LINEAGE`. Resource-level: the downstream dataset URN. Deny
by default for anonymous or read-only users.

### 5. UI

**Read-only rendering (available to everyone who can view the dataset).**

- Clicking an edge in the column lineage view opens a **detail panel** (or
  hover card on small edges) that shows every populated field on the
  underlying `FineGrainedLineage`:
  - `transformOperation` (rendered as-is; monospace when it looks like
    SQL / an expression)
  - `confidenceScore` (rendered as a `0%–100%` badge with color ramp;
    hidden when unset, which is treated as the default `1.0`)
  - `origin` (UI / INGESTION / API / SYSTEM — as a small chip)
  - `actor` (link to the user or ingestion source URN)
  - `createdOn` (relative time, with absolute on hover)
  - `description` (multi-line, markdown-rendered, wrapped)
- These fields are visible **without** `EDIT_LINEAGE_FIELDS`. They exist
  on the aspect today; hiding them is the current bug, not a feature.
- Edges are **color-coded by `origin`** (ingestion vs. UI vs. API), so
  stewards can tell at a glance what they are looking at.

**Edit mode (gated on `EDIT_LINEAGE_FIELDS`).**

- Extend the existing column-level lineage view with an **Edit mode**
  toggle, visible only to users with the privilege.
- In Edit mode, the user can:
  - Click a downstream column to start an edge; click an upstream column
    to finish it.
  - Select an existing edge and delete it.
  - Open the same detail panel used by read-only mode, now with the
    `transformOperation`, `confidenceScore`, and `description` fields
    editable in place.
- Edit operations are batched; the user clicks _Save_ to issue one
  mutation.
- Undo is a client-side inverse mutation; no server-side history beyond
  the normal aspect version log.

### 6. Terminology

- _Edge_: one `FineGrainedLineage` record — a set of upstream fields mapping
  to one or more downstream fields.
- _Origin_: where an edge came from (UI, INGESTION, API, SYSTEM).
- _Assertion_: the act of a particular actor claiming an edge exists. Multiple
  actors may assert the same edge; only the most recent is stored in
  `actor`/`createdOn`, but history is available via aspect versions.

## How we teach this

- Extend the existing _Lineage_ section of the DataHub user guide with an
  **"Editing column lineage"** subsection, parallel to the existing
  "Editing table lineage".
- Add a short explainer on the **merge semantics**: "edits you make in the UI
  won't be overwritten by the next ingestion run, and vice versa".
- Frontend developers: the lineage visualization is extended with an edit
  mode; no new library is introduced.
- Backend / ingestion developers: emit `fineGrainedLineages` as PATCH, not
  UPSERT. Provide a migration guide and a deprecation window for UPSERT.
- SDK users: new `update_fine_grained_lineage` helper mirrors the existing
  table-level helper.

Nothing in the guides is reorganized; this is an additive feature.

## Drawbacks

- **Merge complexity.** Adding provenance-aware merge semantics in the
  Metadata Service is a new invariant that every ingestion source must respect.
  Getting this wrong silently loses either user edits or ingested edges.
- **Aspect bloat.** Datasets with many columns already produce large
  `upstreamLineage` aspects. Adding per-edge metadata (`actor`, `createdOn`,
  `origin`) grows the payload ~2–3×.
- **UI scalability.** A fully interactive column-lineage canvas on a 500-column
  table is hard to make ergonomic. Edit mode will likely feel sluggish on very
  wide tables without additional virtualisation work.
- **Permissions sprawl.** Another privilege to manage.
- **Migration cost for ingestion sources.** Every source that emits CLL must
  move to PATCH semantics or accept the server-side merge hook overhead.
- **User error surface.** Manual CLL edits can be subtly wrong in ways that are
  hard to validate (e.g. wrong transform type), and they have no automated
  regression test. Bad edits can mislead downstream impact analysis.

## Alternatives

### Alternative A — API-only, no UI

Ship `updateFineGrainedLineage` in GraphQL/OpenAPI/SDK and leave the UI
read-only.

- **Pros.** Simple. Avoids the hardest part (UI UX). Power users and platform
  teams can script edits.
- **Cons.** Does not solve the steward's problem: the person who _knows_ the
  lineage is rarely the person comfortable with a GraphQL client. Coverage
  gaps remain in practice.

### Alternative B — Separate aspect for user edits

Introduce a new aspect `userFineGrainedLineage` that lives next to
`upstreamLineage`. Ingestion keeps writing `upstreamLineage` unchanged;
the GMS merges both aspects at read time before returning lineage.

- **Pros.** Clean isolation: ingestion and UI never touch the same bytes. No
  migration of existing sources. Easy to roll back (delete the aspect).
- **Cons.** Two sources of truth to merge at every read. All lineage consumers
  (graph service, search, frontend, SDK, impact analysis) must be updated to
  read from both. Higher read cost forever. Harder to answer "what edges
  exist on this dataset" in a single query. Diverges from the table-level
  lineage pattern, which uses the same aspect for both.

### Alternative C — Lineage "proposals" with an approval workflow

Treat UI edits as proposals that require review (analogous to term/tag
proposals). Proposed edges are stored in a separate aspect and promoted into
`upstreamLineage` upon approval.

- **Pros.** Safer; high-value datasets won't get junk edits. Creates an audit
  trail beyond aspect versions. Aligns with the existing proposal
  infrastructure for glossary terms and tags.
- **Cons.** Significantly more work. Most users want a lightweight "fix the
  typo" affordance, not a ticket queue. Can be layered on top of this RFC
  later (see Future Work).

### Alternative D — Ingestion-only, add a "manual source"

Instead of editing via UI, expose a YAML/CSV ingestion recipe that users fill
in and run through the normal ingestion pipeline.

- **Pros.** Reuses all existing ingestion plumbing. No new merge semantics —
  it's just another source. No frontend work.
- **Cons.** Terrible UX for the steward use case. Zero discoverability.
  Requires users to run ingestion jobs, which most domain experts cannot.
  Still hits the "next ingestion run overwrites me" problem unless the manual
  recipe is run last.

### Prior art

- **OpenLineage** treats column lineage as facets on run events; edits are not
  really a concept because facets are append-only.
- **dbt exposures + `yml`** allow declarative CLL, but are file-based and
  version-controlled, not UI-editable.
- **Atlan / Collibra / Alation** all expose UI-based CLL editing; most use a
  provenance/origin flag and a merge-at-write strategy, which supports the
  design proposed here.

## Rollout / Adoption Strategy

1. **Phase 1 — model & merge (no UI).** Ship the additive fields on
   `FineGrainedLineage`, the server-side merge hook, and the GraphQL/OpenAPI
   mutations. Feature-flag the mutations behind a config.
2. **Phase 2 — SDK + bulk tools.** Ship `update_fine_grained_lineage` in the
   Python SDK. Document the pattern for bulk CSV/YAML imports.
3. **Phase 3 — ingestion PATCH migration.** Update the most-used sources
   (dbt, Snowflake, BigQuery, Redshift, Looker) to emit CLL as PATCH. Leave the
   server-side merge hook in place as a safety net for third-party sources.
4. **Phase 4 — UI.** Ship the Edit mode in the column lineage view behind a
   feature flag; enable it by default after one release.

This is **not a breaking change** for:

- Existing ingestion sources (merge hook handles legacy UPSERTs).
- Existing consumers of `upstreamLineage` (new fields are optional).
- Existing `updateLineage` GraphQL mutation (unchanged; new mutation is
  additive).

No automatic refactor tooling is required. A one-page migration guide for
third-party ingestion plugin authors is sufficient.

## Future Work

- **Proposal workflow** (Alternative C) layered on top: edits from users
  without `EDIT_LINEAGE_FIELDS` become proposals reviewed by data owners.
- **Transformation DSL** — structured transform expressions (e.g. `SUM(x)`,
  `LOWER(y)`) in `transformOperation` with validation.
- **Bulk import UI** — drop a CSV/YAML, preview diff, commit.
- **Cross-entity CLL** — dataset field → ML feature, dataset field → dashboard
  chart dimension.
- **AI-suggested edits** — an LLM assistant proposes CLL edges from SQL/code
  and the steward accepts/rejects in the same Edit mode UI.
- **Lineage diffing** — show what changed between two versions of the aspect
  in the UI.

## Unresolved questions

- Should `origin` be per-edge, per-actor, or both? An edge asserted by both
  dbt and a steward needs to survive removal by either, which argues for a
  list of assertions per edge, not a single `origin` field.
- How does the server-side merge hook scale for very large
  `upstreamLineage` aspects? Is a streaming merge needed, or is
  load-modify-write acceptable?
- Do we reuse `EDIT_LINEAGE` or introduce `EDIT_LINEAGE_FIELDS`? The former
  is simpler; the latter lets admins gate CLL editing separately from the
  more obvious table-level editing.
- Should UI edits be auto-propagated to sibling siblings (sibling datasets
  in the sibling-group sense) the way other aspects are? Probably yes, but
  it interacts with the merge semantics in non-obvious ways.
- What is the story for _deleted_ downstream columns? Orphan CLL edges after
  a schema change — tombstone or auto-prune?
