### Relationship to the Dagster plugin

DataHub also ships a push-based [Dagster plugin](../../../docs/lineage/dagster.md) that runs inside your Dagster deployment as a run-status sensor and emits metadata (including run history) as runs happen. This pull-based source instead scrapes definition-time metadata from the GraphQL API on a schedule, with no code deployed into Dagster.

Both produce the same URNs (platform `dagster`, matching DataFlow/DataJob/asset Dataset conventions), so you can run them together — the pull source for definition-time coverage and the plugin for run-time lineage and execution history.

### Limitations

- Run history (execution instances and statistics) is not yet captured by the pull source; use the plugin for that.
- Ops that do not materialize an asset are not emitted as DataJobs in this version; the asset graph is the source of op-level lineage.
- The OSS GraphQL endpoint is unauthenticated — secure it via your own network/proxy controls.
