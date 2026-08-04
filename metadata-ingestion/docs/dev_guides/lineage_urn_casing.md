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

**`convert_urns_to_lowercase` (legacy)** — a per-source flag that **lowercases every URN**. Drawbacks:

- must be enabled consistently across _all_ sources referencing the same entities
- unavailable on some BI connectors (e.g. Looker, Tableau)
- loses the warehouse's real display casing, and can merge `MyTable` with `mytable`

**Lineage URN casing normalization (recommended)** — rewrites each upstream reference to the casing of the
entity that **already exists** in DataHub, leaving asset identity untouched:

- the warehouse keeps its original casing
- reconciliation is per ingestion, with no cross-source coordination
- references are rewritten only when an existing entity matches
- opt-in, **not enabled by default**

## How it works

When enabled, the feature inspects each source's lineage before it is sent to DataHub and reconciles the
casing of **upstream warehouse references** against the casing DataHub already stores:

- If an entity with the **exact** URN already exists, the reference is left unchanged (`EXACT`).
- Otherwise, if the reference matches an existing entity when casing is normalized, it is rewritten to
  that entity's stored URN (`NORMALIZED`).
- If no existing entity matches, the reference is left unchanged and flagged `UNRESOLVED`.

Only references **to** warehouse assets are modified. The entity the aspect is attached to and its
downstream fields are never touched — the feature respects the casing the warehouse itself reported.
Column-level casing is corrected the same way, using the schema DataHub stores for the resolved table
(so a BI tool reporting `AMOUNT` on a lowercase-stored table is reconciled to the warehouse's `amount`).

> **Coverage depends on the resolution mode.** The default `bulk_catalog` mode heals a reference when the
> warehouse stores the entity in its **lowercased** form (the common Snowflake/BigQuery default),
> regardless of how the BI tool cased it; a warehouse that keeps a **non-lowercase** identity
> (UPPER / Pascal / Mixed) is not reconciled, and case-collisions are not detected. The `alias_lookup`
> mode covers **every** casing and handles collisions. See [Resolution modes](#resolution-modes).

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
feature never rewrites an entity's own or downstream side.

## Resolution modes

Both modes reconcile the same references and record the same match types; they differ only in how the
stored URN is found.

**`bulk_catalog` (default).** Downloads each configured upstream platform's catalog once at the start of
the run and matches against it locally. Because it rebuilds the URN from the table's name parts, it can
also heal a reference whose platform-instance prefix is missing or miscased — the one thing `alias_lookup`
cannot do.

**`alias_lookup`.** Looks each reference up directly in DataHub, using an index the server maintains of
every dataset's lowercased URN:

- downloads no catalog
- covers **every** casing, not just the lowercased form
- handles case-collisions instead of missing them (below)
- resolves any platform, so `upstream_platforms` is not consulted
- matches **by casing alone**, so a reference with a wrong platform-instance prefix stays unresolved

When two live entities differ only by casing — usually the residue of turning `convert_urns_to_lowercase`
on or off — the reference resolves to whichever it matches exactly, else to the **lowercased** one, since
that is the variant the flag produced. Retired duplicates don't interfere: soft-deleted entities are never
candidates. Only when neither the reference nor the lowercased form exists is it left unchanged and
reported.

`alias_lookup` requires a DataHub server that registers the `aliases` aspect on datasets. On an older
server the run **fails at startup** with an actionable message rather than silently resolving nothing.

Start on the default. Switch to `alias_lookup` when your warehouse keeps non-lowercase identities, when
the catalog is large enough that holding it in memory is a problem, or when you'd rather not maintain
`upstream_platforms`.

> **Not necessarily faster.** `alias_lookup` trades one large upfront download for one small query per
> reference, and references are not memoized — a table referenced by fifty dashboards costs fifty
> lookups. Pick it for coverage and memory, not for run time.

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

| Field                                    | Required               | Default        | Description                                                                                                     |
| ---------------------------------------- | ---------------------- | -------------- | --------------------------------------------------------------------------------------------------------------- |
| `enabled`                                | yes                    | `false`        | Whether to reconcile upstream lineage URN casing.                                                               |
| `mode`                                   | no                     | `bulk_catalog` | `bulk_catalog` or `alias_lookup` — see [Resolution modes](#resolution-modes).                                   |
| `upstream_platforms`                     | in `bulk_catalog` mode | `[]`           | Upstream warehouse platform(s) to reconcile against. Others are left unchanged. Ignored in `alias_lookup` mode. |
| `upstream_platforms[].platform`          | yes                    | —              | The upstream data platform, e.g. `snowflake`.                                                                   |
| `upstream_platforms[].platform_instance` | no                     | `null`         | Platform instance of the upstream platform, if any.                                                             |
| `upstream_platforms[].env`               | no                     | `PROD`         | Environment (FabricType) of the upstream platform's assets.                                                     |

To use `alias_lookup`, set the mode and drop `upstream_platforms` — every platform is resolved without
being listed:

```yaml
flags:
  auto_resolve_lineage_urns:
    enabled: true
    mode: alias_lookup
```

### Where to enable it

Enable it on **BI-tool and other cross-platform ingestions** that reference warehouse assets (Looker,
Tableau, Sigma, Redash, Superset, Qlik, etc.), pointing it at the upstream warehouse platform(s).

Do **not** enable it on the warehouse ingestion itself (e.g. Snowflake) — the warehouse is the source of
truth for its own casing and identity.

> **Expect mostly `EXACT`.** Where both sides already agree on casing, references are recorded `EXACT`
> with zero rewrites — that is the feature working correctly. `NORMALIZED` appears only where they
> disagree.

## Match types

For every upstream reference **in scope** — the configured platforms in `bulk_catalog` mode, any dataset
reference in `alias_lookup` mode — the feature records a `matchType` on the lineage aspect:

- **`EXACT`** — already matched an existing entity exactly, including casing. Left unchanged.
- **`NORMALIZED`** — rewritten to heal a casing mismatch against an existing entity.
- **`UNRESOLVED`** — no existing entity matched, or several did with no way to choose between them. Left
  unchanged, but flagged so potentially **broken lineage** is visible rather than indistinguishable from a
  clean edge.

**No `matchType` means the reference is out of scope** — it isn't a dataset reference, its platform isn't
configured (`bulk_catalog` only), the feature is disabled for that source, or the data predates the
feature. Absence is not a verdict. Stamping is ingest-time only: existing metadata is updated only when
its source is re-ingested with the feature on.

## Requirements and limitations

- **Requires a DataHub backend connection.** Resolution looks up existing entities, so it is a no-op for
  offline / file-only ingestion.
- **Resolves only against entities that already exist at ingestion time.** This relies on the warehouse
  being ingested before the BI tool that references it (the normal order for scheduled pipelines). A
  reference whose target doesn't yet exist is left unchanged and self-heals once the warehouse is ingested
  and the BI source re-runs.
- **Does not retroactively heal existing broken edges.** Re-ingest the affected source after enabling the
  flag to fix them.
- **Conservative on collisions.** When two entities differ only by casing and neither the reference nor
  the lowercased form is one of them, the reference is left unchanged rather than risk merging distinct
  tables. These get their own warning, since the fix (remove the duplicate entity) differs from the fix
  for an unresolved reference (ingest the upstream). Only `alias_lookup` mode sees collisions at all —
  see [Resolution modes](#resolution-modes).
- **In `bulk_catalog` mode, reconciles only against tables that have a schema in DataHub.** A referenced
  table that exists without a schema (more common on schemaless platforms like Kafka/DynamoDB) is left
  unchanged and reported `UNRESOLVED`. `alias_lookup` mode resolves on existence and is unaffected.
- **Requires the SQL-parser dependency (`sqlglot`).** Every intended BI/dashboard connector already
  bundles it, so the target use case needs no extra install. If you enable the flag on a source that
  doesn't, the feature reports a clear failure (`install acryl-datahub[sql-parser]`) and emits lineage
  unchanged.
- **Only reconciles full-aspect (UPSERT) lineage, not PATCH.** A lineage aspect emitted as a patch (some
  dbt / Airflow / Spark paths) is emitted unchanged and counted under `num_patch_lineage_skipped` with an
  end-of-run warning. The BI/dashboard targets emit full aspects and are unaffected.
