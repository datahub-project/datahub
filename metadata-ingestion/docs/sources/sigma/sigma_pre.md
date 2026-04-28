### Overview

The `sigma` module ingests metadata from Sigma into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- Workspaces and workbooks within that workspaces as Container.
- Sigma Datasets as Datahub Datasets.
- Pages as Datahub dashboards and elements present inside pages as charts.
- Sigma Data Models as Containers, with each element as a Dataset
  inside the Container (opt-in via `ingest_data_models: true`; default
  `false`). `UpstreamLineage` is emitted on each DM element Dataset for
  intra-DM, cross-DM, and external (warehouse / Sigma Dataset) upstream
  references. Workbook-to-DM connections are emitted as `ChartInfo.inputs`
  on the workbook chart entity (not as `UpstreamLineage` on the DM element
  Dataset). Personal-space DMs referenced from ingested DMs are
  discovered on demand and gated by `ingest_shared_entities` and
  `data_model_pattern`. Report counters under
  `data_model_*` / `element_dm_*` surface per-shape resolution outcomes.

  Notes:

  - Element Dataset URNs are keyed by the immutable Data Model UUID
    (`urn:li:dataset:(sigma,<dataModelId>.<elementId>,env)`) so
    attachments survive Sigma slug rotation; the slug is captured on
    `customProperties.dataModelUrlId`.
  - Column types are emitted as `NullType` with `nativeDataType: "unknown"` —
    Sigma's `/columns` API does not return per-column types today. Earlier
    pre-releases hardcoded `String`; that was a lie (no Sigma side confirms
    it) and has been softened so downstream type-aware features can tell
    "unknown" from "actually a string."
  - Column-level lineage is not emitted yet; `SchemaMetadata` is
    present, so CLL can be added later without re-ingestion.
  - Unresolved upstream references (Sigma Dataset not ingested, DM not
    reachable, element name not matched) are suppressed rather than
    fabricated as dangling URNs. External upstreams to Sigma Datasets
    only resolve when the referenced dataset is ingested in the same
    recipe run. Splitting Sigma Datasets and Data Models into separate
    recipes will leave those upstreams unresolved. The report splits
    these between `data_model_element_upstreams_unresolved_external`
    (target Sigma Dataset exists but wasn't ingested this run) and
    `data_model_element_upstreams_unknown_shape` (source*id shapes this
    release does not parse — cross-DM refs have their own dedicated
    `data_model_element_cross_dm_upstreams*\*`counters); the aggregate`data_model_element_upstreams_unresolved`
    is retained for dashboards that already read it. Re-run with
    broader patterns to materialize them.
  - Setting `ingest_data_models: true` issues `/dataModels/{id}/elements`
    and `/columns` calls per DM unconditionally, but the per-DM
    `/lineage` call is gated on `extract_lineage: true`. If you opt out
    of lineage at the workbook surface, the DM connector also stops
    hitting any `/lineage` endpoint — DM Containers, element Datasets,
    and `SchemaMetadata` are still emitted, but without `UpstreamLineage`.
  - The DM Container URN is keyed on `platform` and
    `platform_instance` but not `env`. Multi-environment deployments
    against the same tenant should set a distinct `platform_instance`
    per env so DM Containers do not collide on a single URN. Element
    Datasets are already env-scoped.
  - **Cross-DM lineage is name-based.** Sigma's `/lineage` endpoint
    identifies cross-DM producer elements by free-text `name` only
    (there is no stable element ID on the upstream node), so the
    resolver matches producer and consumer elements by
    case-insensitive `name`. Renaming an element in the producing DM
    between runs silently moves the edge: the old target loses the
    upstream and the new target may never match. The counter that
    ticks up is `data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known`
    — a non-zero value usually means an upstream rename, not a
    connector bug. For single-element producer DMs, the fallback
    above recovers the edge automatically; for multi-element DMs
    operators should treat a jump in that counter as a re-ingest
    prompt.
  - **Single-element cross-DM fallback.** When a DM element references
    another DM's element (`sourceIds = ["<otherDmUrlId>/<suffix>"]`) and
    the name bridge cannot match the consuming element's name against
    any producer-DM element, the resolver falls back to binding the
    edge to the producer's sole element _iff_ the producer DM contains
    exactly one element. This covers the CSV-upload / single-asset
    personal-DM case where either side has been renamed (by
    construction, there is only one possible target). The fallback bumps both
    `data_model_element_cross_dm_upstreams_resolved` (aggregate success
    count) and `data_model_element_cross_dm_upstreams_single_element_fallback`
    (sub-shape count), so it is distinguishable in the report.
    Semantics: if the producer DM is later edited to add a second
    element and the user renames the consumer to point at the new one,
    the fallback no longer fires and the previously-emitted edge will
    reflect whatever name matches at the next ingest — i.e. the edge
    tracks "current state of Sigma," not "state at edge-creation time."
    This matches how Sigma's own UI resolves the reference.
  - **Ambiguous cross-DM name matches.** When two or more elements in a
    producer DM share the same case-insensitive name, the resolver
    cannot determine which element is the intended upstream. It picks
    the lexicographically smallest Dataset URN among the candidates
    (stable and deterministic across runs) and increments
    `data_model_element_cross_dm_upstreams_ambiguous`. The emitted
    lineage edge is technically valid but may point at the wrong
    element — a `logger.warning` is also emitted at ingestion time so
    the ambiguous pick is visible in the connector log. The root cause
    is a naming collision on the Sigma side: renaming one of the
    duplicates in Sigma will let the resolver match precisely and
    clear the counter. This is distinct from the rename case above:
    here the name _is_ found but is not unique, whereas the rename case
    produces a name-miss (`data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known`).

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer [doc](https://help.sigmacomputing.com/docs/generate-api-client-credentials) to generate an API client credentials.
2. Provide the generated Client ID and Secret in Recipe.

We have observed issues with the Sigma API, where certain API endpoints do not return the expected results, even when the user is an admin. In those cases, a workaround is to manually add the user associated with the Client ID/Secret to each workspace with missing metadata.
Empty workspaces are listed in the ingestion report in the logs with the key `empty_workspaces`.
