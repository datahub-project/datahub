# Postgres SqlSetup SQL migrations

Reusable versioned DDL for PostgreSQL features lives under `sqlsetup/<feature>/migrations/` and is applied by
`com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner` from System Update (via
`datahub-upgrade`).

## Module descriptor

Build a `SqlMigrationModule` with:

- `migrationNamespace` — unique id for advisory locks and logs (e.g. `pgqueue`)
- `targetSchema` — PostgreSQL schema (`CREATE SCHEMA` + `search_path`)
- `classpathLocation` — resource path to `V*` / `R*` scripts (e.g. `sqlsetup/pgqueue/migrations`)
- `ledgerTableName` — unqualified ledger table in `targetSchema` (e.g. `metadata_queue_schema_migration`)
- `tokenReplacements` — map of `__TOKEN__` → substituted text (applied before checksum and execution)

## Script types

| Prefix                  | Type       | Behavior                                       |
| ----------------------- | ---------- | ---------------------------------------------- |
| `V001__description.sql` | VERSIONED  | Run once; fail if checksum changes after apply |
| `R__description.sql`    | REPEATABLE | Re-run when script checksum changes            |

**Never edit shipped VERSIONED scripts** — add `V00N+1` for schema changes.

## Ledger inspection

```sql
SELECT * FROM queue.metadata_queue_schema_migration ORDER BY version_rank;
```

(Replace schema/table for your module.)

## pgQueue

See [pgQueue design doc](../../../../../docs/pgqueue-design.md) and `PgQueueSqlMigrationModules`.
