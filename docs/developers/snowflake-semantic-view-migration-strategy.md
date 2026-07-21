# Strategy: Snowflake semantic-view governance migration

Teammates often ask: *why a separate migrate CLI, and why not reuse `dataplatform2instance` or fold this into ingest?* This doc answers that. Behavior details: [migration design](./snowflake-semantic-view-migration.md). How to run it: [deployment](./snowflake-semantic-view-migration-deployment.md).

## Problem in one line

Turning on `semantic_views.emit_semantic_model_entities` changes **entity type and URN**. Ingest creates the new entities and (with stateful ingestion) soft-deletes the old datasets. It does **not** copy human governance. Something else has to move ownership/tags/terms/etc. onto the new URNs.

## What we chose

**`datahub migrate snowflake-semantic-views`** — a one-shot CLI under the existing `migrate` group.

| Property | Choice |
| --- | --- |
| Vehicle | CLI subcommand (ships with `acryl-datahub` / `datahub`) |
| When | Operator-run around the flag flip — not on a cron |
| What it copies | Entity governance + DataHub-only column tags/terms |
| What it does not | Soft-delete, lineage rewrite, policies, data products |
| Directions | `dataset-to-sm` (flag ON) and `sm-to-dataset` (flag OFF) |

Precedent: `datahub migrate` already exists for one-shot cutovers ([#3996](https://github.com/datahub-project/datahub/pull/3996) introduced the group; later `instance2instance`). We add a **new subcommand**, we do not invent a parallel entrypoint.

**Important:** this is *compatible with the migrate convention*, not with reusing `dataplatform2instance`. That command same-types datasets and rewrites some relationships. We change `dataset` → `semanticModel`/`metric`, drop env from the key, and cannot rewrite `Upstream.dataset` (still `DatasetUrn`-typed).

## Alternatives (and why not)

| Alternative | Verdict | Why |
| --- | --- | --- |
| **A. Do nothing (docs only)** | Insufficient if anyone applied governance | Soft-delete hides old datasets; ownership/tags stay on dead URNs |
| **B. Connector copies via `ctx.graph` during ingest** | Rejected | Couples migration to every run; hard to dry-run/review; partial failures leave half-migrated state |
| **C. Reuse `dataplatform2instance` / `instance2instance`** | Rejected | Same entity type + instance prefix rewrite only; no type change; relationship rewrite assumptions don’t apply |
| **D. `datahub-upgrade` system job** | Rejected | Upgrade jobs are platform-wide and automatic; this is tenant opt-in and recipe-flag gated |
| **E. Forever-scheduled Airflow/cron Job** | Rejected | Cutover tool, not ongoing sync; after flip, connector + UI own the new URNs |
| **F. Example script only (`examples/library`)** | Too weak alone | Easy to miss; no dry-run UX / discovery next to other migrates. Fine as a pattern reference, not the product surface |
| **G. Dedicated CLI under `migrate` (chosen)** | **Yes** | Matches existing one-shot ops model; versioned with CLI/GMS; dry-run; stays out of the Snowflake recipe |

## Responsibility split

```
┌─────────────────────┐     ┌──────────────────────────────┐
│ Snowflake ingest    │     │ datahub migrate              │
│ (recipe / DAG)      │     │ snowflake-semantic-views     │
├─────────────────────┤     ├──────────────────────────────┤
│ Emit SM + metrics   │     │ Copy governance old → new    │
│ Soft-delete old SVs │     │ Report unrepointable refs    │
│   (stateful)        │     │ No delete / no lineage fix   │
└─────────────────────┘     └──────────────────────────────┘
```

Order for flag ON: dry-run migrate → real migrate → flip flag → normal ingest (soft-delete).

## What still needs humans / model work

Cannot be fixed by this script (needs product/model changes):

- Downstream dataset lineage pointing at the old SV (`Upstream.dataset`)
- Charts/dashboards, data-product assets, incidents, assertions
- Policy privilege UI entries for `semanticModel` / `metric`

The CLI can `--report-inbound-refs` so operators see those; it does not rewrite them.

## Decision summary

Use a **migrate CLI subcommand** because the work is a **tenant cutover** (opt-in flag, governance copy, dry-run), not recurring ingest and not a global upgrade. Keep ingest pure; keep soft-delete with stateful ingestion; document unrepointable graph edges honestly.
