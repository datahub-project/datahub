# Deploying the Snowflake semantic-view migration CLI

How to ship and run `datahub migrate snowflake-semantic-views`. This is a **one-shot ops tool**, not part of the recurring Snowflake ingestion job.

Why this shape (vs ingest / upgrade / reuse p2i): [strategy](./snowflake-semantic-view-migration-strategy.md).  
Design / behavior: [snowflake-semantic-view-migration.md](./snowflake-semantic-view-migration.md).  
CLI surface (same pattern as other migrates): [docs/cli.md](../cli.md) (`dataplatform2instance`, `instance2instance`).

## What gets deployed

| Piece | How it ships |
| --- | --- |
| Migration logic | Inside the `datahub` / `acryl-datahub` CLI package (`metadata-ingestion`) |
| Runtime | Any host that can reach GMS with a token (laptop, bastion, one-off CI/k8s Job) |
| Recurring ingest | Unchanged — Snowflake recipe does **not** call the migrator |

No separate binary, container image, or `datahub-upgrade` step is required. Version the CLI with the connector/GMS release that registers `semanticModel` / `metric`.

## When to run

Only when an operator opts into `semantic_views.emit_semantic_model_entities` (or rolls it back) **and** needs to move DataHub-authored governance off the old URNs.

Do not schedule it on a cron or attach it to the daily Snowflake DAG.

## Cutover sequence (flag ON)

The migrator **does not create** destination entities. Ingest with the flag ON first so semanticModel/metric exist; then copy governance while soft-deleted SV datasets are still readable. Discovery defaults to live-only — pass `--include-soft-deleted` deliberately after reviewing the soft-deleted set (empty discovery prints a hint when only soft-deleted sources exist).

1. Upgrade GMS (and CLI) to a build that supports `semanticModel` / `metric`.
2. Point the CLI at that GMS (`datahub init` / `DATAHUB_GMS_URL` + token).
3. Set `semantic_views.emit_semantic_model_entities: true` on the Snowflake recipe; keep stateful ingestion + `remove_stale_metadata` on.
4. Run the normal Snowflake ingest once; confirm new semanticModels/metrics are active (and old SV datasets soft-deleted if stateful).
5. Dry-run (include soft-deleted sources after flag-ON ingest):
   ```bash
   datahub migrate snowflake-semantic-views \
     --direction dataset-to-sm \
     --platform-instance <same as recipe> \
     --include-soft-deleted \
     --dry-run \
     --report-inbound-refs
   ```
6. Spot-check mapped URNs and field fan-out; entities with missing destinations are reported and skipped. Soft-deleted sources are marked in the report. Optionally pilot with `--urn` / `--urn-file`.
7. Run without `--dry-run` (keep `--include-soft-deleted`; confirm prompt, or `-F` in automation) **before** GC hard-deletes soft-deleted datasets.
8. Fix unrepointable refs (data products, policies, bookmarks) manually; hold GC hard-delete of soft-deleted datasets until sign-off.

Flag OFF: re-ingest with the flag off so datasets exist again, then `--direction sm-to-dataset --env <ENV>`.

## Where to run it

**Preferred:** operator workstation or bastion with network access to GMS — matches existing `datahub migrate *` usage.

**Optional automation:** a **one-off** CI job or Kubernetes Job that:

- Uses the same CLI image/version as the release
- Injects GMS URL + token as secrets
- Runs dry-run, then real migrate (or two separate jobs)
- Archives the CLI report as an artifact
- Exits (no `RestartPolicy: Always`, no cron)

Do not embed the command in the Snowflake ingestion recipe or in connector code (`ctx.graph`).

## Why not other vehicles

| Vehicle | Verdict |
| --- | --- |
| Snowflake ingest recipe / transformer | Rejected — couples migration to every run; hard to dry-run/review |
| `datahub-upgrade` system job | Rejected — those are platform-wide automatic steps; this is tenant opt-in |
| Forever-scheduled Airflow/cron | Rejected — cutover tool, not ongoing sync |
| `examples/library` script only | Insufficient as the sole surface — keep the first-class CLI + docs |

## Version and config coupling

- CLI, connector, and GMS must be on a release that understands `semanticModel` / `metric`.
- `--platform-instance` and `--convert-urns-to-lowercase` must match the Snowflake recipe or URNs will be wrong.
- `--env` is required to rebuild dataset URNs on `sm-to-dataset` (SM/metric keys have no env).

## Idempotence and failure

- Re-runs overwrite destination governance aspects (last-write-wins). Safe to retry after a partial run.
- Soft-delete of old URNs is **not** done by this CLI; failed ingest fail-safes may skip soft-delete — re-run ingest, don’t expect the migrator to delete.
- If GMS is unreachable mid-run, fix connectivity and re-run the same command; already-migrated entities will be rewritten with the same aspects.

## Docs / release checklist

When shipping a release that includes the flag + CLI:

- [ ] Release note in `docs/how/updating-datahub.md` (flag behavior + soft-delete)
- [ ] Connector note in `snowflake_post.md`
- [ ] CLI section in `docs/cli.md` under Migrate (link here + design doc)
- [ ] Runbook owners know: flip flag → ingest (create destinations) → dry-run/`--include-soft-deleted` → migrate — not migrate-first (CLI will not create thin entities)
- [ ] Do not merge before connector PR [#18395](https://github.com/datahub-project/datahub/pull/18395) (URN contract)

## Ownership

| Role | Responsibility |
| --- | --- |
| Connector / CLI maintainers | Ship CLI + docs; keep URN mapping aligned with `SnowflakeIdentifierBuilder` |
| Deployers / admins | Flip recipe flag; ingest; then dry-run + migrate per environment; verify soft-delete |
| Ingestion automation | Unchanged daily job; only recipe flag / image version bump |
