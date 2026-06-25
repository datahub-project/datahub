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

- For each configured upstream platform, it bulk-loads that platform's existing URNs (and schemas) from
  DataHub once per run, so resolution happens locally without a round trip per reference.
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

| Reference                                                                                 | Fixed                                     |
| ----------------------------------------------------------------------------------------- | ----------------------------------------- |
| `upstreamLineage` upstream dataset URNs                                                   | ✅ table-level                            |
| `fineGrainedLineage` upstream field URNs                                                  | ✅ table-level **and** column-name casing |
| `dashboardInfo` dataset references (`datasets`, `datasetEdges`)                           | ✅ table-level                            |
| `dataJobInputOutput` inputs (`inputDatasets`, `inputDatasetEdges`, `fineGrainedLineages`) | ✅ table-level **and** column-name casing |
| `dataJobInputOutput` outputs (`outputDatasets`, `outputDatasetEdges`)                     | ❌ left unchanged (the job's own outputs) |

`dataJobInputOutput` covers the dbt / Airflow / Spark warehouse-upstream path: a job's **inputs** are
upstream warehouse references and are healed like any other upstream, while its **outputs** are the job's
declared products and are left untouched — consistent with the feature never rewriting an entity's own /
downstream side.

Column-level casing is corrected using the schema DataHub stores for the resolved table, so a BI tool
that reports a column as `AMOUNT` is reconciled to the warehouse's actual `amount` (or vice versa).

## Enabling the feature

Add the `normalize_lineage_urn_casing` flag under the pipeline-level `flags` block, and list the
upstream warehouse platform(s) whose references should be reconciled:

```yaml
source:
  type: looker
  config:
    # ... your Looker config ...

flags:
  normalize_lineage_urn_casing:
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

## Match type explainability

When the feature **rewrites** a reference to heal a casing mismatch, it stamps `matchType = NORMALIZED`
on the lineage aspect. Treat `matchType` as a **positive signal**: `NORMALIZED` means "this feature
healed a casing mismatch on this reference."

Every other reference is **left untouched**, with no `matchType` written — this includes references that
already matched exactly, references the feature could **not** reconcile (no match, or an ambiguous
collision — i.e. genuinely broken lineage), references ingested **before** this feature was enabled, and
references from sources where the flag is off. So **absence of `matchType` does not mean "exact"** — it
only means this feature made no assertion. Do not read it as a guarantee of correctness.

Two consequences worth calling out:

- **Stamping is opt-in and ingest-time only.** Metadata already stored in DataHub is never modified; a
  reference only gains (or loses) a `matchType` when its source is re-ingested with the feature enabled.
- **`EXACT` is intentionally never emitted.** The `LineageMatchType` model defines an `EXACT` value for
  completeness, but the processor does not stamp it. Doing so would rewrite every clean edge's content on
  first enable (defeating GMS no-op de-duplication and causing a metadata-change-log storm) for a
  distinction no consumer currently relies on — and it still would not help historical data, which stays
  unstamped until re-ingested.

## Requirements and limitations

- **Requires a DataHub backend connection.** Resolution queries DataHub for the existing entities, so the
  feature is a no-op for offline / file-only ingestion.
- **Resolves only against entities that already exist at ingestion time.** This relies on the warehouse
  being ingested before the BI tool that references it — the normal order for scheduled pipelines. If a
  reference's target does not yet exist in DataHub, it is left unchanged and self-heals once the
  warehouse is ingested and the BI source re-runs.
- **Does not retroactively heal existing broken edges** already stored in the graph. Re-ingest the
  affected source after enabling the flag to fix them.
- **Collision-safe but conservative.** On case-sensitive platforms where two genuinely different tables
  differ only by case, ambiguous references are left unchanged rather than risk merging distinct
  entities.
- **Loads the upstream platform's full catalog into memory.** On first use the feature bulk-fetches every
  dataset URN (and the schema of schema-bearing entities) for each configured upstream platform, and
  builds an in-memory case-insensitive index over them. Resolution is then fully local (no per-reference
  round trips), but on very large warehouses (hundreds of thousands to millions of tables) this scroll is
  heavy and the index is the processor's main memory cost. The number of URNs loaded per platform is
  logged at `INFO` (`Loaded N '<platform>' dataset URNs ...`) so you can gauge it. Scope
  `upstream_platforms` to the platforms (and, where possible, `platform_instance` / `env`) the BI source
  actually references.
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
