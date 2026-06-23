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

- It is not available on some BI connectors (e.g. Looker, Tableau), so a mixed Snowflake-uppercase +
  BI-lowercase setup stays broken regardless.
- Flattening every identity to lowercase loses the warehouse's real display casing, and on
  case-sensitive platforms it can merge two genuinely different tables (`MyTable` and `mytable`) into a
  single entity.

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

| Reference                                                       | Fixed                                     |
| --------------------------------------------------------------- | ----------------------------------------- |
| `upstreamLineage` upstream dataset URNs                         | ✅ table-level                            |
| `fineGrainedLineage` upstream field URNs                        | ✅ table-level **and** column-name casing |
| `dashboardInfo` dataset references (`datasets`, `datasetEdges`) | ✅ table-level                            |

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

When the feature reconciles a reference, it records a `matchType` on the lineage aspect:

- `EXACT` — the reference already matched an existing entity, including casing.
- `NORMALIZED` — the reference was rewritten to heal a casing mismatch.

When no reconciliation was performed, `matchType` is left unset. This makes it possible to distinguish
exact lineage from casing-resolved lineage downstream.

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
