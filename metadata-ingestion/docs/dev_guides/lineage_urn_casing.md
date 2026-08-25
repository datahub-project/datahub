# Lineage URN Casing Normalization

DataHub identifies every entity by its URN, and URNs are compared as **exact, case-sensitive strings**.
When two sources refer to the same physical table with different casing, DataHub treats them as two
different entities, so the lineage edge between them is never drawn.

A common example: a warehouse like Snowflake reports table names in uppercase (its convention), while a
BI tool like Looker or Tableau references the same table in lowercase. The result is two disconnected
nodes instead of one connected lineage edge:

```text
Warehouse (Snowflake) entity:
  urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.TABLE,PROD)

BI tool reports lineage pointing at:
  urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)

Result: ❌ no lineage edge — two orphaned nodes
```

This silently breaks multi-hop lineage too: a single broken edge in the middle of a chain hides the
entire downstream path.

## Two ways to keep lineage connected

**`convert_urns_to_lowercase` (legacy).** The older mitigation is this per-source flag, which keeps
lineage connected by **lowercasing every URN**. It works only when enabled consistently across _all_
sources that reference the same entities, isn't available on some BI connectors (e.g. Looker, Tableau),
and — because it flattens every identity to lowercase — loses the warehouse's real display casing and can
even merge two genuinely different tables (`MyTable` and `mytable`) into one entity.

**Lineage URN casing normalization (recommended).** Instead of flattening identities, this feature
resolves each upstream reference to the casing of the entity that **already exists** in DataHub. It is
preferred because it **does not alter the identity of your assets**: the warehouse keeps its original
casing, reconciliation happens per ingestion with no cross-source coordination, and references are only
rewritten when the match is unambiguous. It is opt-in and **not enabled by default**.

## How it works

When enabled, the feature inspects each source's lineage before it is sent to DataHub and reconciles the
casing of **upstream warehouse references** against the casing DataHub already stores:

- If an entity with the **exact** URN already exists, the reference is left unchanged (`EXACT`).
- Otherwise, if the reference matches an existing entity when casing is ignored, it is rewritten to
  that entity's stored URN (`NORMALIZED`). Any stored casing is reachable — lowercase, UPPER, Pascal
  or Mixed — since matching is on the case-insensitive form of the whole URN.
- If no existing entity matches, the reference is left unchanged and flagged `UNRESOLVED`.
- If the reference matches **two** existing entities differing only by case, it is resolved to the
  **lowercase-named** one (`NORMALIZED`) — the common warehouse default, and better than leaving the
  edge broken. This applies only when the reference matches neither casing exactly; if it matches one
  casing exactly, that casing wins (`EXACT`). If none of the colliding entities is lowercase-named
  (and there is no exact match), there is no basis to choose and
  the reference is left unchanged and flagged `UNRESOLVED`.

Matching is on the whole URN, so `platform_instance` and `env` are part of the comparison: a `DEV`
reference is never healed to a same-named `PROD` entity.

Only references **to** warehouse assets are modified. The entity the aspect is attached to and its
downstream fields are never touched — the feature respects the casing the warehouse itself reported.
Column-level casing is corrected the same way, using the schema DataHub stores for the resolved table
(so a BI tool reporting `AMOUNT` on a lowercase-stored table is reconciled to the warehouse's `amount`).

> **Entities without a schema.** A table-level reference is healed even when DataHub holds no
> `schemaMetadata` for the entity. Column-level casing cannot be corrected in that case — there are no
> columns to match against — so field paths are left as the source reported them.

### What gets fixed

| Reference                                               | Fixed                                     |
| ------------------------------------------------------- | ----------------------------------------- |
| `upstreamLineage` upstream datasets                     | ✅ table-level                            |
| `fineGrainedLineage` upstream fields                    | ✅ table-level **and** column-name casing |
| `dashboardInfo` dataset references                      | ✅ table-level                            |
| `dataJobInputOutput` **inputs** (dbt / Airflow / Spark) | ✅ table-level **and** column-name casing |
| `chartInfo` upstream datasets                           | ✅ table-level                            |
| `dataJobInputOutput` **outputs**                        | ❌ left unchanged (the job's own outputs) |
| `dataJobInputOutput` `inputDatasetFields`               | ❌ not yet covered                        |
| `dataProcessInstance` run lineage                       | ❌ not yet covered                        |

A DataJob's **outputs** are its own declared products, so they are deliberately left untouched — the
feature never rewrites an entity's own or downstream side. The not-yet-covered rows are incremental
follow-ups; most connectors emit the covered aspects.

## Enabling the feature

Add the `auto_resolve_lineage_urns` flag under the pipeline-level `flags` block, and list the upstream
warehouse platform(s) this source references heavily:

```yaml
source:
  type: looker
  config:
    # ... your Looker config ...

flags:
  auto_resolve_lineage_urns:
    enabled: true
    upstream_platforms:
      - platform: snowflake
        platform_instance: my_instance # optional
        env: PROD # optional, defaults to PROD
      # add more entries for additional upstream platforms
      # - platform: redshift
      #   env: PROD
    # also reconcile references to platforms not listed above, asking DataHub about
    # each one rather than reading its catalog:
    # resolve_all_platforms: true

sink:
  # ... your sink config ...
```

### Configuration reference

| Field                                    | Required  | Default | Description                                                                   |
| ---------------------------------------- | --------- | ------- | ----------------------------------------------------------------------------- |
| `enabled`                                | yes       | `false` | Whether to reconcile upstream lineage URN casing.                             |
| `upstream_platforms`                     | see below | `[]`    | Upstream warehouse platform(s) whose catalogs to read once, up front.         |
| `upstream_platforms[].platform`          | yes       | —       | The upstream data platform, e.g. `snowflake`.                                 |
| `upstream_platforms[].platform_instance` | no        | `null`  | Platform instance of the upstream platform, if any.                           |
| `upstream_platforms[].env`               | no        | `PROD`  | Environment (FabricType) of the upstream platform's assets.                   |
| `resolve_all_platforms`                  | no        | `false` | Reconcile references to every platform, not just the listed ones — see below. |

When enabled, you must set `upstream_platforms`, `resolve_all_platforms: true`, or both. Enabling the
feature with neither is rejected at config parse, since there would be nothing to reconcile.

### What gets reconciled, and how

The two settings answer different questions. `upstream_platforms` is a **preload hint** — the
warehouses this source names so often that reading their catalogs once beats asking table by table.
`resolve_all_platforms` is the **scope** — whether references to anything else are reconciled at all.

| `upstream_platforms` | `resolve_all_platforms` | Read at startup | Reconciled                    | How                                                   |
| -------------------- | ----------------------- | --------------- | ----------------------------- | ----------------------------------------------------- |
| `[snowflake]`        | `false` (default)       | snowflake       | snowflake only                | from the preloaded catalog; a miss is asked           |
| `[snowflake]`        | `true`                  | snowflake       | snowflake and everything else | snowflake from its catalog; every other miss is asked |
| `[]`                 | `true`                  | nothing         | everything                    | one query per distinct table                          |
| `[]`                 | `false`                 | —               | —                             | rejected at config parse                              |

Reach for `resolve_all_platforms` when a source references several warehouses of which only one is
hot: preload that one, and let the occasional reference to the others cost a query rather than a
whole catalog read. With no `upstream_platforms` at all, nothing is read up front and every reference
is answered individually — right for a source that touches only a handful of warehouse tables, or
whose warehouse is too large to read.

Preloading and querying divide the work rather than stacking on it. A preloaded catalog is a **cache,
not an authority**: a reference it holds is answered locally, and a reference it does not hold is asked
of DataHub. Nothing is ever concluded from a preload's silence, because a preload covers one
`platform` / `platform_instance` / `env` and a reference may legitimately live outside it — in a
sibling instance, say. So every `UNRESOLVED` is backed by an actual search, and a preload only ever
saves a query.

That makes a failed or empty read cost speed rather than correctness. The one exception is a read that
dies **part way**: its rows are real, but its list of casings for a given name may be incomplete, and
an incomplete list is a _hit_ that heals a reference to the wrong table. Such a load is therefore
discarded whole and its references are asked instead.

Column casing works the same way, and is loaded separately: a preloaded catalog answers from its
schemas, and anything it does not hold — including everything in a slice whose schema read failed —
has its columns fetched. Such a reference still heals at table level; only its columns cost a round
trip.

### Where to enable it

Enable it on **BI-tool and other cross-platform ingestions** that reference warehouse assets (Looker,
Tableau, Sigma, Redash, Superset, Qlik, etc.), pointing it at the upstream warehouse platform(s).

Do **not** enable it on the warehouse ingestion itself (e.g. Snowflake) — the warehouse is the source of
truth for its own casing and identity.

> **Expect mostly `EXACT` if there is no actual mismatch.** The feature only rewrites genuine casing
> mismatches. If both sides already agree on casing, references are recorded `EXACT` with zero rewrites —
> that is the feature working correctly. You'll see `NORMALIZED` only where the two sides disagree.

## Match types

For every upstream reference **in scope**, the feature records a `matchType` on the lineage aspect:

- **`EXACT`** — already matched an existing entity exactly, including casing. Left unchanged.
- **`NORMALIZED`** — rewritten to heal a casing mismatch against an existing entity.
- **`UNRESOLVED`** — could not be resolved to a single existing entity (no match, or an ambiguous
  collision). Left unchanged, but flagged so potentially **broken lineage** is visible rather than
  indistinguishable from a clean edge.

**No `matchType` means the reference is out of scope** — its platform is neither listed in
`upstream_platforms` nor covered by `resolve_all_platforms`, the feature is disabled for that source, or
the data predates the feature. A reference whose lookup **failed** is also left unstamped, and counted
separately rather than as `UNRESOLVED`. Absence is not a verdict. Stamping is
ingest-time only: existing metadata is updated only when its source is re-ingested with the feature on.

## Requirements and limitations

- **Requires a DataHub backend connection.** Resolution looks up existing entities, so it is a no-op for
  offline / file-only ingestion.
- **Requires the dataset `aliases` backfill to have succeeded.** GMS derives the aspect from the dataset
  key aspect, written once at creation, so a dataset created before aliases shipped has none until the
  `BackfillDatasetAliases` system update (default-on, non-blocking) reaches it. An entity with no alias
  is invisible to the lookup, so a reference naming it exactly can be healed onto a lowercase-named
  sibling instead — a wrong edge, not an `UNRESOLVED` one. The feature therefore stays off, with a
  warning, until the completion marker on `urn:li:dataHubUpgrade:dataset-aliases-v1` reports
  `SUCCEEDED`. That marker is written when the backfill's scroll is exhausted, and the alias writes
  themselves land through the MCE consumer, so references resolved while that backlog drains can still
  miss; they heal on the source's next run.
- **Aliases are written asynchronously.** For a short window after a backfill or an ingestion, a
  reference can still miss and be reported `UNRESOLVED`. It heals on the source's next run.
- **Resolves only against entities that already exist at ingestion time.** This relies on the warehouse
  being ingested before the BI tool that references it (the normal order for scheduled pipelines). A
  reference whose target doesn't yet exist is left unchanged and self-heals once the warehouse is ingested
  and the BI source re-runs.
- **Does not retroactively heal existing broken edges.** Re-ingest the affected source after enabling the
  flag to fix them.
- **Collisions resolve to the lowercase-named entity.** On case-sensitive platforms where two genuinely
  different tables differ only by case, a reference that matches neither exactly is healed to the
  lowercase-named one; only a collision with no lowercase-named side is left unchanged. See
  [Match types](#match-types).
- **Column casing needs the table's schema.** A referenced table that exists without a `schemaMetadata`
  aspect is still healed at table level; only its column casing is left as the source reported it.
- **Scope is by platform.** By default a reference is reconciled only if its platform is named in
  `upstream_platforms`; a reference to any other platform is never looked up, and is left unchanged and
  unstamped — no `matchType` verdict, counted under `num_refs_out_of_scope` and never `UNRESOLVED`,
  since nothing was checked. The `platform_instance` and `env` on an entry say what to _preload_, not
  what is in scope, so a reference into an instance or env you did not list is still reconciled — by
  asking — as long as its platform is listed. Set `resolve_all_platforms: true` to reconcile every
  platform a reference points at.
- **`platform_instance` narrows the catalog read via DataHub's search, which matches the entity's
  `dataPlatformInstance` aspect — not the URN.** The instance is part of the dataset URN's name
  regardless, but that is not what the filter reads, so a connector that emits the URN without the
  aspect matches nothing and the read returns 0 datasets. That is harmless — the preload simply holds
  nothing and every reference is asked instead — but it costs a query per reference a working filter
  would have answered locally. Unity Catalog does not emit the aspect by default
  (`ingest_data_platform_instance_aspect`), and the instance name must also match the casing that was
  emitted. If the log shows `Loaded 0 URNs`, drop `platform_instance` and read the whole platform / env
  instead.
- **Requires the SQL-parser dependency (`sqlglot`).** Every intended BI/dashboard connector already
  bundles it, so the target use case needs no extra install. If you enable the flag on a source that
  doesn't, the feature reports a clear failure (`install acryl-datahub[sql-parser]`) and emits lineage
  unchanged.
- **Only reconciles full-aspect (UPSERT) lineage, not PATCH.** A lineage aspect emitted as a patch
  (e.g. `dataJobInputOutput` via `DatasetPatchBuilder.add_upstream_lineage` / `DataJobPatchBuilder`,
  used by some dbt / Airflow / Spark paths) is emitted unchanged and counted under
  `num_patch_lineage_skipped` with an end-of-run warning. The BI/dashboard targets emit full aspects
  and are unaffected; broadening this to patches is a tracked follow-up.
