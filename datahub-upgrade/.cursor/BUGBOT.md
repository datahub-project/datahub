# datahub-upgrade Bugbot rules

If an UpgradeStep deletes ES docs or mutates entities, then:

- Require completion recording so skip() is true after success.
- Consider idempotency under at-most-once MCL delete semantics.

## ZDU index cleanup vs catch-up

If a PR touches `CleanUpIndicesStep`, `IncrementalReindexCatchUpStep`, or ZDU
upgrade step ordering, then:

- High / Critical. Cleanup must not delete alias-less old backing indices that
  catch-up still needs as `_reindex` sources.
- Flag missing per-index error isolation / skip-on-success for catch-up.
- Title: "ZDU cleanup must preserve catch-up sources"

## Bootstrap MCP version bumps

If a PR bumps the version of an existing bootstrap MCP YAML (e.g.
`page-templates.yaml`) or re-runs templates that write org/user UI state, then:

- High. Bootstrap re-runs can overwrite customer customizations.
- Prefer a new bootstrap file, an idempotent/no-op when customized, or an
  explicit migration guard — not a silent version bump of a shared bundle.
- Title: "Destructive bootstrap version bump"
