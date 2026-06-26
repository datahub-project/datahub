### Capabilities

The connector emits:

- **Software-Defined Assets** as `Dataset` entities (subtype `Asset`), with descriptions, ownership, tags, documentation links (`institutionalMemory`), and table schemas (`schemaMetadata`) when declared.
- **Table-level asset lineage** derived from each asset's upstream/downstream dependencies.
- **Column-level (fine-grained) lineage** when assets expose column-lineage metadata, controlled by `include_column_lineage`.
- **Jobs** as `DataFlow` and **ops** as `DataJob` — **only when `include_jobs: true`**. These are off by default because they are orchestration implementation details; the asset-to-asset graph is emitted independently of them.

#### Relationship to the Dagster plugin

DataHub also ships a push-based [Dagster plugin](../../../../docs/lineage/dagster.md) that runs inside your Dagster deployment as a run-status sensor and emits metadata (including run history) as runs happen. This pull-based source instead scrapes definition-time metadata from the GraphQL API on a schedule, with no code deployed into Dagster.

Both produce the same URNs (platform `dagster`, matching DataFlow/DataJob/asset Dataset conventions), so you can run them together — the pull source for definition-time coverage and the plugin for run-time lineage and execution history.

### Limitations

- Run history (execution instances and statistics) is not yet captured by the pull source; use the plugin for that.
- Ops that do not materialize an asset are not emitted as DataJobs in this version; the asset graph is the source of op-level lineage.
- The OSS GraphQL endpoint is unauthenticated — secure it via your own network/proxy controls.

### Troubleshooting

#### Connection or authentication errors

- For Dagster+, confirm `host`, `deployment`, and `token` are correct and that the token's user has at least **Viewer** access to the deployment. The GraphQL URL is `<host>/<deployment>/graphql`.
- For OSS, confirm the webserver is reachable at `<host>/graphql` (default port `3000`) from where ingestion runs.

#### No assets or lineage appear

- Ensure the assets are loaded in the targeted code locations/repositories and pass the configured `repository_pattern` / `asset_pattern` filters.
- Asset lineage requires the assets to declare dependencies; set `include_asset_lineage: true` (the default).
