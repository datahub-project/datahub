# Ingestion Migrations

Ingestion migrations let a connector ship a **one-time fix that reshapes metadata already in DataHub** when the connector introduces a breaking change — for example a URN-scheme change (see [Lineage URN casing](./lineage_urn_casing.md)), an aspect-shape change, or a renamed custom property. Without this, existing entities keep the old shape until they happen to be re-ingested, and references to them dangle.

A migration is declared **in the connector code**, DataHub records which migrations have already been applied, and any not-yet-applied migration runs **before ingestion** (when enabled).

## When to use a migration

Use one when a connector change means metadata **already in DataHub** must be transformed — not merely that future ingestion emits a new shape. If the next normal ingestion run naturally overwrites the affected aspects, you do not need a migration.

## How it works

- Migrations are declared by overriding `get_migrations()` on a source that extends `StatefulIngestionSourceBase`.
- The set of applied migration ids is stored in a **ledger** that rides the stateful-ingestion checkpoint, keyed per pipeline. So migrations **require stateful ingestion** to be enabled.
- Before ingestion, a pre-ingestion hook computes the **pending** migrations (declared but not in the ledger) and, when enabled, runs them in declared order, recording each id as it succeeds.

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
                id="mysource-0001-lowercase-urns",  # stable & unique; never reuse an id
                description="Lowercase dataset URNs after the casing fix.",
                run=self._lowercase_urns,
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
    run_transform(graph, LowercaseConverter(), platform="mysource", dry_run=dry_run)
```

For aspect-shape changes, fetch the affected aspects and re-emit them in the new shape from within `run`.

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
```

When migrations are pending but `enabled` is false, the run logs a warning listing the pending ids (and fails if `fail_on_pending` is set), so operators know a migration is waiting.

## Guidelines

- **Ids are permanent.** Once a migration has run anywhere, its id is recorded; never reuse or renumber ids. Prefix with the connector and an increasing number: `mysource-0001-...`.
- **Idempotent, always.** Assume a migration may run more than once against the same data.
- **A migration failure aborts ingestion** — ingestion should not land on half-migrated metadata. The ledger records only successes, so a re-run resumes at the first unapplied migration.
- **Prefer a narrowing filter** so an already-migrated instance is a cheap no-op.
