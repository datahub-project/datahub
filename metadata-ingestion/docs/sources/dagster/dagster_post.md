### Capabilities

#### Extraction modes

`extraction_mode` controls how the connector writes to DataHub:

- **`enrich` (default)** — the connector only _tops up_ assets that **already exist** in DataHub (typically created by the Dagster plugin). It writes descriptions, tags, ownership, and lineage as non-destructive **PATCH** updates, never overwriting existing values and never creating new entities or entity-defining aspects. Assets not present in DataHub are skipped. This mode requires a DataHub connection (a sink or `datahub_api`). Use it alongside the plugin, which owns the assets and their warehouse-table sibling links.
- **`full`** — the connector creates and owns the assets as standalone `Dataset` entities (schema, subtypes, etc.). Use this for an initial load, or when running the connector without the Dagster plugin.

In both modes the connector emits:

- **Software-Defined Assets** as `Dataset` entities (subtype `Asset`), with descriptions, ownership, tags, documentation links (`institutionalMemory`), and table schemas (`schemaMetadata`) when declared.
- **Table-level asset lineage** derived from each asset's upstream/downstream dependencies. This is lineage **between Dagster assets** (platform `dagster`). Cross-platform lineage to the underlying warehouse tables (e.g. Snowflake/BigQuery/Databricks) is **not** produced by this source — it relies on the Dagster plugin emitting the asset → warehouse-table `siblings` link (see below).
- **Column-level (fine-grained) lineage** when assets expose column-lineage metadata, controlled by `include_column_lineage`.
- **Jobs** as `DataFlow` and **ops** as `DataJob` — **only when `include_jobs: true`**. These are off by default because they are orchestration implementation details; the asset-to-asset graph is emitted independently of them.

#### Relationship to the Dagster plugin

DataHub also ships a push-based [Dagster plugin](../../../../docs/lineage/dagster.md) that runs inside your Dagster deployment as a run-status sensor and emits metadata (including run history) as runs happen. This pull-based source instead scrapes definition-time metadata from the GraphQL API on a schedule, with no code deployed into Dagster.

Both produce the same URNs (platform `dagster`, matching DataFlow/DataJob/asset Dataset conventions), so you can run them together — the pull source for definition-time coverage and the plugin for run-time lineage and execution history.

### Limitations

- **Cross-platform lineage depends on the Dagster plugin.** This source links Dagster assets to each other, but it does not connect them to the real warehouse tables they materialize. That asset → warehouse-table relationship (emitted as a `siblings` link, so the two render as one entity) is resolved at materialization time and produced by the [Dagster plugin](../../../../docs/lineage/dagster.md). Run the plugin if you need lineage/siblings into Snowflake, BigQuery, Databricks, etc.
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
