# Ingestion Migrations

Ingestion migrations let a connector ship a **one-time fix that reshapes metadata already in DataHub** when the connector introduces a breaking change — for example a URN-scheme change (see [Lineage URN casing](./lineage_urn_casing.md)), an aspect-shape change, or a renamed custom property. Without this, existing entities keep the old shape until they happen to be re-ingested, and references to them dangle.

A migration is declared **in the connector code**, DataHub records which migrations have already been applied, and any not-yet-applied migration runs **before ingestion** (when enabled).

## When to use a migration

Use one when a connector change means metadata **already in DataHub** must be transformed — not merely that future ingestion emits a new shape. If the next normal ingestion run naturally overwrites the affected aspects, you do not need a migration.

## How it works

- Migrations are declared by overriding `get_migrations()` on a source that extends `StatefulIngestionSourceBase`.
- The set of applied migration ids is stored in a **ledger** that rides the stateful-ingestion checkpoint, keyed per pipeline. So migrations **require stateful ingestion** to be enabled.
- Before ingestion, a pre-ingestion hook computes the **pending** migrations (declared but not in the ledger) and, when enabled, runs them in id order, recording each id as it succeeds.

Because the ledger is per pipeline, a migration can re-trigger when a *different* pipeline ingests the same data, and the ledger read is eventually consistent (so two runs of the *same* pipeline in quick succession could both apply). **Migrations must therefore be idempotent** — re-running one must be a no-op. A migration that selects its targets with a filter that matches nothing after the first apply (e.g. "datasets whose URN is not already lowercase") satisfies this naturally.

## Declaring a migration

A `Migration` is a stable `id`, a `description`, and a callable `run(graph, report, dry_run)` containing the migration code:

```python
from datahub.ingestion.source.state.ingestion_migration import Migration
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)


class MySource(StatefulIngestionSourceBase):
    def get_migrations(self):
        return [
            Migration(
                id="20260722-mysource-lowercase-urns",  # timestamp prefix = run order; never reuse
                description="Lowercase dataset URNs after the casing fix.",
                run=self._lowercase_urns,
                fixed_in="1.5.0",  # optional; data from 1.5.0+ is already correct
            ),
        ]

    def _lowercase_urns(self, graph, report, dry_run):
        # Migration code: select the affected URNs and mutate them. Idempotent.
        ...
```

`run` is a plain callable (method, lambda, or module-level function). It receives the `DataHubGraph`, the source's `SourceReport` (use it to log counts/warnings), and a `dry_run` flag.

### Rename-type migrations

For breaking changes that rename URNs, reuse the `migrate transform` core rather than hand-rolling the rewrite — it moves all aspects and rewrites table- and column-level lineage plus incoming references:

```python
from datahub.cli.migrate import run_transform
from datahub.cli.migration_utils import LowercaseConverter

def _lowercase_urns(self, graph, report, dry_run):
    run_transform(
        graph,
        LowercaseConverter(),
        platform="mysource",
        platform_instance=self.config.platform_instance,  # optional; scope to this instance
        dry_run=dry_run,
    )
```

`run_transform` discovers **all** matching datasets from the graph for the given `platform` (optionally narrowed to a `platform_instance`) — not just the URNs in the current pipeline's checkpoint state — so a migration reshapes every affected entity regardless of which pipeline produced it. Omit `platform_instance` to cover all instances of the platform.

For aspect-shape changes, fetch the affected aspects and re-emit them in the new shape from within `run`.

## Ordering and version gating

**Ordering** is by **id**. Pending migrations run sorted by their `id`, each recorded on success — so the id is the ordering key, not the position in the list. Use a **timestamp prefix** (`20260722-...`, optionally `20260722T1530-...`) so migrations run chronologically and a global fix (see below) interleaves correctly with a connector's own. If one migration depends on another, give it a later timestamp. Ids are permanent — never reorder or reuse them.

**Version gating** is optional and answers "was the data in DataHub produced by an affected version?". Describe the bug's lifecycle with a window `[introduced_in, fixed_in)`:

- `introduced_in` — the first release that produced the broken shape (inclusive). Data from **before** it was never affected, so the migration is skipped.
- `fixed_in` — the first release whose output is already correct (exclusive). Data from it onward is already right, so the migration is skipped. (This is the release that usually ships the migration.)

So a migration runs only when the last-run version is in `introduced_in <= v < fixed_in`. Either bound may be omitted for a one-sided window; omit both to gate on the id ledger alone. Unknown last-run version → the migration runs (`converter.should_convert` is the fine-grained guard that never rewrites data that doesn't match). The last-run version is refreshed in the ledger on every enabled run.

## Global (cross-connector) migrations

A fix that applies to more than one connector can be registered once instead of copied into each `get_migrations()`. Register a **factory** that receives the running source and returns a `Migration` scoped to it:

```python
from datahub.ingestion.source.state.ingestion_migration import register_migration, Migration

def _my_global_fix(source):
    return Migration(
        id="20260722-lowercase-all-urns",
        description="Lowercase URNs across connectors after the casing fix.",
        run=lambda graph, report, dry_run: run_transform(
            graph, LowercaseConverter(), platform=source.get_platform(),
            platform_instance=source.config.platform_instance, dry_run=dry_run,
        ),
        fixed_in="1.6.0",
    )

register_migration(_my_global_fix)
```

The framework merges registered migrations into every source's list (a source-declared id wins on a clash) and gates them with the **same per-pipeline ledger** — so a global migration simply runs once per pipeline, scoped to that pipeline's source. No global ledger or cross-pipeline lock is involved.

## Enabling migrations

Migrations are **disabled by default** — they never mutate metadata silently. Opt in per recipe:

```yaml
source:
  type: mysource
  config:
    stateful_ingestion:
      enabled: true
      migrations:
        enabled: true        # apply pending migrations before ingestion
        dry_run: false       # log what would run without mutating
        fail_on_pending: false  # if true, abort when a migration is pending but disabled
        force: false         # re-run every migration even if the ledger/version gate would skip it
```

When migrations are pending but `enabled` is false, the run logs a warning listing the pending ids (and fails if `fail_on_pending` is set), so operators know a migration is waiting.

Set `force: true` to re-run every migration regardless of the ledger or the version window — useful to re-apply after a fix or to force a one-off re-run. It still requires `enabled: true` to mutate, and (like all migrations) relies on migrations being idempotent.

## Guidelines

- **Ids are permanent.** Once a migration has run anywhere, its id is recorded; never reuse or renumber ids. Prefix with the connector and an increasing number: `mysource-0001-...`.
- **Idempotent, always.** Assume a migration may run more than once against the same data.
- **A migration failure aborts ingestion** — ingestion should not land on half-migrated metadata. The ledger records only successes, so a re-run resumes at the first unapplied migration.
- **Prefer a narrowing filter** so an already-migrated instance is a cheap no-op.
