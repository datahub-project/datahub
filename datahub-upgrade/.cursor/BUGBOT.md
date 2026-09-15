# datahub-upgrade Bugbot rules

If an UpgradeStep deletes ES docs or mutates entities in a **non-idempotent** /
at-most-once way, then:

- Require completion recording so `skip()` is true after success.
- Do not demand `skip()`-after-first-success for intentional per-deploy
  reconciliation steps that deliberately re-run.

## ZDU index cleanup vs catch-up

If a PR touches `CleanIndicesStep` (runtime id `CleanUpIndicesStep`),
`IncrementalReindexCatchUpStep`, or ZDU upgrade step ordering, then:

- High / Critical. Cleanup must not delete alias-less old backing indices that
  catch-up still needs as `_reindex` sources.
- Flag missing per-index error isolation / skip-on-success for catch-up.
- Title: "ZDU cleanup must preserve catch-up sources"

## Bootstrap MCP version bumps

If a PR bumps the version of an existing bootstrap MCP YAML that **UPSERTs** /
overwrites org/user UI state (or otherwise re-applies mutable templates), then:

- High. Bootstrap re-runs can overwrite customer customizations.
- Prefer a new bootstrap file, an idempotent/no-op when customized, or an
  explicit migration guard — not a silent version bump of a shared overwrite
  bundle.
- Do not flag CREATE-only bootstrap MCPs (existing entity → create rejected, not
  overwritten) as overwrite risks solely due to a version bump.
- Title: "Destructive bootstrap version bump"
