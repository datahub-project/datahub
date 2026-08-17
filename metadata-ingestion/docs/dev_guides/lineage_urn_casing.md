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
  edge broken. If none of the colliding entities is lowercase-named, there is no basis to choose and
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
| `chartInfo` / `dataJob` column-level field lists        | ❌ not yet covered                        |
| `dataProcessInstance` run lineage                       | ❌ not yet covered                        |

A DataJob's **outputs** are its own declared products, so they are deliberately left untouched — the
feature never rewrites an entity's own or downstream side. The not-yet-covered rows are incremental
follow-ups; most connectors emit the covered aspects.

## Enabling the feature

Add the `auto_resolve_lineage_urns` flag under the pipeline-level `flags` block, and list the upstream
warehouse platform(s) whose references should be reconciled:

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

sink:
  # ... your sink config ...
```

### Configuration reference

| Field                                    | Required           | Default | Description                                                                     |
| ---------------------------------------- | ------------------ | ------- | ------------------------------------------------------------------------------- |
| `enabled`                                | yes                | `false` | Whether to reconcile upstream lineage URN casing.                               |
| `upstream_platforms`                     | yes (when enabled) | `[]`    | Upstream warehouse platform(s) to reconcile against. Others are left unchanged. |
| `upstream_platforms[].platform`          | yes                | —       | The upstream data platform, e.g. `snowflake`.                                   |
| `upstream_platforms[].platform_instance` | no                 | `null`  | Platform instance of the upstream platform, if any.                             |
| `upstream_platforms[].env`               | no                 | `PROD`  | Environment (FabricType) of the upstream platform's assets.                     |
| `on_demand_lookup`                       | no                 | `false` | Ask DataHub per reference instead of reading catalogs up front — see below.     |

### How references are looked up

By default the feature reads each configured platform's catalog once at startup and answers every
reference locally. Setting `on_demand_lookup: true` skips that read and asks DataHub about each
reference instead:

|                          | Up front                              | Per reference                                       |
| ------------------------ | ------------------------------------- | --------------------------------------------------- |
| default                  | Reads each configured platform's URNs | Nothing — every reference is answered locally       |
| `on_demand_lookup: true` | Nothing                               | One batched query per distinct unresolved reference |

The default suits a source that references a lot of one warehouse. Prefer `on_demand_lookup` when a
source touches only a handful of warehouse tables, or when the warehouse is large enough that reading
its catalog costs more than the references are worth.

The two are alternatives, not layers. A catalog read records which slices it covered, so a reference
it does not find is already known to be absent — there is nothing left for a query to add.

### Where to enable it

Enable it on **BI-tool and other cross-platform ingestions** that reference warehouse assets (Looker,
Tableau, Sigma, Redash, Superset, Qlik, etc.), pointing it at the upstream warehouse platform(s).

Do **not** enable it on the warehouse ingestion itself (e.g. Snowflake) — the warehouse is the source of
truth for its own casing and identity.

> **Expect mostly `EXACT` if there is no actual mismatch.** The feature only rewrites genuine casing
> mismatches. If both sides already agree on casing, references are recorded `EXACT` with zero rewrites —
> that is the feature working correctly. You'll see `NORMALIZED` only where the two sides disagree.

## Match types

For every upstream reference **on a configured platform**, the feature records a `matchType` on the
lineage aspect:

- **`EXACT`** — already matched an existing entity exactly, including casing. Left unchanged.
- **`NORMALIZED`** — rewritten to heal a casing mismatch against an existing entity.
- **`UNRESOLVED`** — could not be resolved to a single existing entity (no match, or an ambiguous
  collision). Left unchanged, but flagged so potentially **broken lineage** is visible rather than
  indistinguishable from a clean edge.

**No `matchType` means the reference is out of scope** — its platform isn't configured, the feature is
disabled for that source, or the data predates the feature. Absence is not a verdict. Stamping is
ingest-time only: existing metadata is updated only when its source is re-ingested with the feature on.

## Requirements and limitations

- **Requires a DataHub backend connection.** Resolution looks up existing entities, so it is a no-op for
  offline / file-only ingestion.
- **Resolves only against entities that already exist at ingestion time.** This relies on the warehouse
  being ingested before the BI tool that references it (the normal order for scheduled pipelines). A
  reference whose target doesn't yet exist is left unchanged and self-heals once the warehouse is ingested
  and the BI source re-runs.
- **Does not retroactively heal existing broken edges.** Re-ingest the affected source after enabling the
  flag to fix them.
- **Conservative on collisions.** On case-sensitive platforms where two genuinely different tables differ
  only by case, ambiguous references are left unchanged rather than risk merging distinct entities.
- **Column casing needs the table's schema.** A referenced table that exists without a `schemaMetadata`
  aspect is still healed at table level; only its column casing is left as the source reported it.
- **Only reconciles against the platforms you configure.** A reference is resolved only to an entity
  inside the `platform` / `platform_instance` / `env` combinations listed in `upstream_platforms`, so a
  reference to a `platform_instance` you did not list is reported `UNRESOLVED` even if that entity
  exists in DataHub. Add it to `upstream_platforms` to have it reconciled.
- **Requires the SQL-parser dependency (`sqlglot`).** Every intended BI/dashboard connector already
  bundles it, so the target use case needs no extra install. If you enable the flag on a source that
  doesn't, the feature reports a clear failure (`install acryl-datahub[sql-parser]`) and emits lineage
  unchanged.
- **Only reconciles full-aspect (UPSERT) lineage, not PATCH.** A lineage aspect emitted as a patch
  (e.g. `dataJobInputOutput` via `DatasetPatchBuilder.add_upstream_lineage` / `DataJobPatchBuilder`,
  used by some dbt / Airflow / Spark paths) is emitted unchanged and counted under
  `num_patch_lineage_skipped` with an end-of-run warning. The BI/dashboard targets emit full aspects
  and are unaffected; broadening this to patches is a tracked follow-up.
