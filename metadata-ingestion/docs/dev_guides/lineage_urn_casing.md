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

### The existing mitigation, and why it isn't enough

Today the only built-in mitigation is the per-source `convert_urns_to_lowercase` flag, which keeps
lineage connected by **blindly lowercasing every URN**. It works only when enabled consistently across
_all_ sources that reference the same entities (an N-source coordination problem), and it carries real
costs:

- It only ever forces names to lowercase, and it is not available on some BI connectors (e.g. Looker,
  Tableau) at all. Because you cannot control the casing those connectors emit, the only way to reconcile
  a warehouse↔BI mismatch is to lowercase the warehouse side too — there is no setting that connects them
  while preserving the warehouse's original (upper- or mixed-case) casing.
- That path commits you to lowercasing every identity, which loses the warehouse's real display casing
  and, on case-sensitive platforms, can merge two genuinely different tables (`MyTable` and `mytable`)
  into a single entity.

The **lineage URN casing normalization** feature takes a different approach. Instead of flattening all
identities, it resolves each upstream reference to the casing of the entity that **already exists** in
DataHub — per ingestion, with no global coordination, preserving the warehouse's original casing, and
only when the match is unambiguous. It is an explicit opt-in feature and is **not enabled by default**.

## How it works

When enabled, a work unit processor inspects each source's lineage aspects before they are sent to
DataHub and reconciles the casing of **upstream warehouse references** against the casing DataHub
already stores:

- For each configured upstream platform, it bulk-loads that platform's existing schema-bearing entities
  from DataHub once per run, so resolution happens locally without a round trip per reference.
- For every upstream reference, it looks for the existing entity that matches **ignoring case**:
  - If an entity with the **exact** URN already exists, the reference is left unchanged (recorded as an
    exact match). This ensures genuinely distinct entities on case-sensitive platforms are never merged.
  - Otherwise, if exactly **one** existing entity matches case-insensitively, the reference is rewritten
    to that entity's URN (recorded as a normalized match). This heals the mismatch in **both**
    directions — uppercase-reported→lowercase-stored and lowercase-reported→uppercase-stored.
  - If **no** entity matches, or **more than one** matches (an ambiguous collision on a case-sensitive
    platform, e.g. both `orders` and `Orders` exist), the reference is left unchanged.

Only references **to** warehouse assets are modified. The entity the aspect is attached to, and
downstream field references, are never touched — the feature respects the identity and casing the
warehouse itself reported.

### What gets fixed

| Reference                                                                                 | Fixed                                          |
| ----------------------------------------------------------------------------------------- | ---------------------------------------------- |
| `upstreamLineage` upstream dataset URNs                                                   | ✅ table-level                                 |
| `fineGrainedLineage` upstream field URNs                                                  | ✅ table-level **and** column-name casing      |
| `dashboardInfo` dataset references (`datasets`, `datasetEdges`)                           | ✅ table-level                                 |
| `dataJobInputOutput` inputs (`inputDatasets`, `inputDatasetEdges`, `fineGrainedLineages`) | ✅ table-level **and** column-name casing      |
| `dataJobInputOutput` outputs (`outputDatasets`, `outputDatasetEdges`)                     | ❌ left unchanged (the job's own outputs)      |
| `dataJobInputOutput` `inputDatasetFields` / `outputDatasetFields`                         | ❌ not yet covered (use `fineGrainedLineages`) |
| `chartInfo` upstream datasets (`inputs`, `inputEdges`)                                    | ✅ table-level                                 |
| `chartInfo` `inputFields` aspect (column-level)                                           | ❌ not yet covered                             |
| `dataProcessInstance` lineage (run inputs/outputs)                                        | ❌ not yet covered                             |

`dataJobInputOutput` covers the dbt / Airflow / Spark warehouse-upstream path: a job's **inputs** are
upstream warehouse references and are healed like any other upstream, while its **outputs** are the job's
declared products and are left untouched — consistent with the feature never rewriting an entity's own /
downstream side.

Column-level casing is corrected using the schema DataHub stores for the resolved table, so a BI tool
that reports a column as `AMOUNT` is reconciled to the warehouse's actual `amount` (or vice versa).

#### Boundary / not yet covered

The reconciled aspects above carry the bulk of warehouse-upstream references. A few lineage-bearing
paths are intentionally **out of scope** for this iteration and treated as incremental follow-ups:

- `dataJobInputOutput.inputDatasetFields` / `outputDatasetFields` — column-level lists separate from
  `fineGrainedLineages`; most connectors emit fine-grained lineage, which **is** covered.
- `chartInfo` `inputFields` aspect (column-level for charts/dashboards) — the chart's dataset-level
  inputs **are** covered; only the column-level field references are not.
- `dataProcessInstance` run-level lineage.

Adding any of these is incremental: every reference funnels through one resolver, so a new aspect is just
"extract its URNs → resolve → write back" — the matching logic does not change.

## Enabling the feature

Add the `auto_resolve_lineage_urns` flag under the pipeline-level `flags` block, and list the
upstream warehouse platform(s) whose references should be reconciled:

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

| Field                                    | Required           | Default | Description                                                                                                                     |
| ---------------------------------------- | ------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`                                | yes                | `false` | Whether to reconcile upstream lineage URN casing.                                                                               |
| `upstream_platforms`                     | yes (when enabled) | `[]`    | The upstream warehouse platform(s) to reconcile references against. References to platforms not listed here are left unchanged. |
| `upstream_platforms[].platform`          | yes                | —       | The upstream data platform, e.g. `snowflake`.                                                                                   |
| `upstream_platforms[].platform_instance` | no                 | `null`  | Platform instance of the upstream platform, if any.                                                                             |
| `upstream_platforms[].env`               | no                 | `PROD`  | Environment (FabricType) of the upstream platform's assets.                                                                     |

### Where to enable it

Enable this feature on **BI-tool and other cross-platform ingestions** that reference warehouse assets
(Looker, Tableau, Sigma, Redash, Superset, Qlik, etc.) and point it at the upstream warehouse
platform(s).

Do **not** enable it on the warehouse ingestion itself (e.g. the Snowflake ingestion) — the warehouse is
the source of truth for its own casing and identity, which must be respected.

> **Expect mostly `EXACT` if there's no actual mismatch.** The feature only rewrites genuine casing
> mismatches. If your warehouse is ingested with default settings (e.g. Snowflake URNs already lower-cased
> by `convert_urns_to_lowercase`) and the BI tool also emits lower-case, references will already match and
> be recorded as `EXACT` with zero rewrites — that is the feature working correctly, not a no-op. You'll
> see `NORMALIZED` only where the two sides genuinely disagree on casing.

## Match type explainability

For every upstream reference **on a configured platform**, the feature records a `matchType` verdict on
the lineage aspect:

- **`EXACT`** — the reference already matched an existing entity exactly, including casing. Left unchanged.
- **`NORMALIZED`** — the reference was rewritten to heal a casing mismatch against an existing entity.
- **`UNRESOLVED`** — the reference is on a configured platform but could **not** be resolved to a single
  existing entity (no match under any casing, or an ambiguous casing collision). Left unchanged, but
  flagged so potentially **broken lineage** is visible rather than indistinguishable from a clean edge.

For a `fineGrainedLineage`, the aspect-level `matchType` is the aggregate of its upstream fields, surfacing
the most actionable verdict first: `NORMALIZED` > `UNRESOLVED` > `EXACT`.

**Absence of `matchType` means the reference is out of scope** — its platform is not in
`upstream_platforms`, the feature is disabled for that source, or the data was ingested before the feature
was enabled. Absence is **not** a verdict; only a present value (including `EXACT`) is an assertion by this
feature.

A consequence worth calling out: stamping is **ingest-time only**. Metadata already stored in DataHub is
never modified — a reference gains (or updates) its `matchType` only when its source is re-ingested with
the feature enabled. So historical edges stay unstamped until re-ingested, regardless of whether they are
actually exact or broken.

## Requirements and limitations

- **Requires a DataHub backend connection.** Resolution queries DataHub for the existing entities, so the
  feature is a no-op for offline / file-only ingestion.
- **Requires the SQL-parser dependency (`sqlglot`).** Resolution reuses the ingestion `SchemaResolver`,
  which depends on `sqlglot`. Every intended BI / dashboard connector extra (Looker, Power BI, Tableau,
  Mode, Superset/Preset, Redash, Metabase, Sigma, Qlik, Hex, Grafana, Fivetran, …) already bundles it, so
  the target use case needs no extra install. But `sqlglot` is **not** part of the ingestion core, so if
  you enable this flag on a pure-API source whose extra doesn't pull it in, the processor reports a clear
  failure (`install acryl-datahub[sql-parser]`) and emits lineage unchanged rather than reconciling it.
- **Resolves only against entities that already exist at ingestion time.** This relies on the warehouse
  being ingested before the BI tool that references it — the normal order for scheduled pipelines. If a
  reference's target does not yet exist in DataHub, it is left unchanged and self-heals once the
  warehouse is ingested and the BI source re-runs.
- **Does not retroactively heal existing broken edges** already stored in the graph. Re-ingest the
  affected source after enabling the flag to fix them.
- **Collision-safe but conservative.** On case-sensitive platforms where two genuinely different tables
  differ only by case, ambiguous references are left unchanged rather than risk merging distinct
  entities.
- **Reconciles against schema-bearing entities only.** Membership/casing comes from what the
  `SchemaResolver` already loads — the platform's entities that have a schema in DataHub. A reference to a
  warehouse table that **exists but has no schema** (more common on schemaless platforms like Kafka or
  DynamoDB than on Snowflake/BigQuery) is therefore left unchanged and reported `UNRESOLVED`. Covering
  schemaless entities needs a richer `SchemaResolver` and is a tracked follow-up.
- **Heals only references that were emitted; it cannot recover lineage dropped upstream.** This processor
  reconciles the casing of references a source _emits_. For **sqlglot-derived** sources (those that parse
  SQL via `SqlParsingAggregator` — e.g. Looker derived tables, Mode/Superset query SQL), column-level
  (fine-grained) lineage is only produced when the table's schema resolves at parse time, and
  `SchemaResolver.resolve_table` tries just three casing variants (as-parsed, fully-lowercased, and
  lower-table + non-lowercased-instance). If the warehouse's stored casing falls outside those (e.g.
  arbitrary PascalCase), the schema lookup misses and **no column-level edge is emitted** — so there is
  nothing here to heal. Table-level lineage is unaffected: `resolve_table` always returns a best-effort
  URN, so the table edge is emitted in the parsed casing and then reconciled by this processor. Closing
  the parse-time column-level gap needs the same casing-aware `SchemaResolver` follow-up referenced above.
- **Loads the upstream platform's catalog on first use.** For each configured upstream platform the
  resolver bulk-fetches its schema-bearing entities, and the processor builds an in-memory
  case-insensitive index over them — so resolution is then fully local (no per-reference round trips). On
  very large warehouses (hundreds of thousands to millions of tables) this index is the processor's main
  memory cost; the number of URNs loaded per platform is logged at `INFO`
  (`Loaded N '<platform>' dataset URNs ...`) so you can gauge it. Scope `upstream_platforms` to the
  platforms (and, where possible, `platform_instance` / `env`) the BI source actually references. (A
  disk-backed index is a planned follow-up.)
- **Platform-instance casing is normalized on the heal path.** The case-insensitive match lowercases the
  entire dataset-name segment of the URN, which **includes** any `platform_instance` prefix. So a
  reference like `MyInstance.db.schema.table` can heal to a stored `myinstance.db.schema.table` entity —
  the instance casing is reconciled along with the table name. Only the exact-match check (which leaves a
  reference untouched) compares the instance casing case-sensitively. As always, if two instance-case
  variants genuinely co-exist as separate entities, that is an ambiguous collision and the reference is
  left unchanged.

### Interaction with `convert_urns_to_lowercase`

The two settings are complementary, not mutually exclusive — they can both be enabled on the same source.
`convert_urns_to_lowercase` runs first and blindly lowercases URNs; this normalizer then runs afterward
and resolves each (now-lowercased) upstream reference to the actual casing stored in DataHub. So if both
are on, the normalizer effectively wins, restoring the warehouse's real casing where an unambiguous match
exists. In practice you should prefer this normalizer **instead of** `convert_urns_to_lowercase` for the
sources you enable it on, since it achieves connected lineage without flattening casing; the coexistence
behavior is documented only so a source that already has `convert_urns_to_lowercase` set behaves
predictably.
