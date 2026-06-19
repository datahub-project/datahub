### Capabilities

The connector extracts the following metadata:

- **Cubes and views** as datasets, grouped under a container representing the deployment. The container links back to the deployment UI (derived from `api_url`, or set `deployment_url`).
- **Schema** — each measure and dimension becomes a schema field. Measures carry their aggregation type (e.g. `count`, `sum`) in the native data type; primary-key dimensions are flagged as part of the key. Fields are tagged `Measure` or `Dimension` — and `Temporal` for time dimensions (disable with `tag_measures_and_dimensions: false`).
- **Descriptions and properties** — titles, descriptions, segment names, source file name, and any custom `meta` defined in the model.
- **Structural metadata** — joins (with relationship), hierarchies (with levels), folders/nested folders (with members), and pre-aggregation names are captured as dataset custom properties (disable with `emit_member_details: false`).
- **Measure presentation hints** — each measure's `format`, drill-down members, and cumulative flag are stored on the schema field as `jsonProps`.
- **Hidden members** — cubes, views, and members marked `public: false` / `isVisible: false` are skipped by default; set `include_hidden: true` to ingest them.
- **Tags, glossary terms, owners, domains, and documentation links** — derived from the `meta` defined on cubes/views via `meta_mapping`, and from member `meta` via `column_meta_mapping` (same syntax as the dbt connector). Domains can also be assigned by name pattern via the `domain` config.
- **Reports and workbooks** (Cube Cloud only) — saved [reports](https://docs.cube.dev/api-reference/reports/list-reports) become DataHub **charts** with input lineage to the cubes/views they query, and [workbooks](https://docs.cube.dev/api-reference/workbooks/get-workbooks) become DataHub **dashboards** containing those charts. Owners and titles are carried across. Disable with `include_reports: false` / `include_workbooks: false`, and filter with `report_pattern` / `workbook_pattern`.

#### Lineage

Lineage is emitted when `ingest_lineage` is enabled (the default):

- **View to cube** — views are linked to the cubes they are built on, including column-level lineage derived from each member's `aliasMember`.
- **Cube to warehouse** — on Cube Cloud with the Metadata API, table and column references are read directly. On Cube Core, table-level lineage is parsed from each cube's SQL definition when `parse_sql_for_lineage` and `warehouse_platform` are set.
- **Report and workbook to view** — on Cube Cloud, charts (reports) carry input lineage to the cubes/views in their query, and dashboards (workbooks) contain those charts, extending the chain to `warehouse → cube → view → chart → dashboard`.

Disable column-level lineage with `include_column_lineage: false`.

#### Cube Cloud authentication and metadata merging

On Cube Cloud the connector reads both endpoints and merges them: `/v1/meta` supplies the structural and presentation metadata (joins, hierarchies, folders, formats, visibility), while the [Metadata API](https://docs.cube.dev/reference/control-plane-api) (`/v1/entities`, `/v1/data-sources`) supplies warehouse and column-level lineage. This gives a Cloud ingestion the union of both.

The Metadata API requires a metadata-scoped JWT. You can either:

- Provide a pre-generated token in `api_token`, or
- Let the connector mint one automatically: set `cloud_api_key` (a Cube Cloud API key from Account → API keys) together with `deployment_id` and `environment_id`. The connector calls the Control Plane `tokens-for-meta-sync` endpoint to obtain a short-lived, metadata-only token. Override the Control Plane host with `cloud_api_url` if it differs from the `api_url` host, and embed a `security_context` to scope multi-tenant visibility.

If the Metadata API cannot be reached, the connector logs a warning and continues with `/v1/meta` only (structural metadata and view-to-cube lineage, but no warehouse lineage).

#### Reports and workbooks (Cube Cloud Platform API)

Reports and workbooks are read from the Cube Cloud [Platform API](https://docs.cube.dev/api-reference/introduction), which is authenticated with a Cube Cloud API key as a `Bearer` token. Set `cloud_api_key` and `deployment_id` to enable this (`environment_id` is _not_ required for reports/workbooks — it is only needed when minting a Metadata API token). When these are absent, or for Cube Core, report/workbook ingestion is skipped silently. A failed Platform API call logs a warning and does not abort the run.

#### Multi-tenancy and context variables

Cube [context variables](https://cube.dev/docs/reference/data-model/context-variables) (`COMPILE_CONTEXT`, `SECURITY_CONTEXT`, `FILTER_PARAMS`, `FILTER_GROUP`, `SQL_UTILS`) are data-model authoring constructs, not metadata the APIs expose as structured fields — there is nothing separate to ingest. They affect the connector only indirectly:

- **`COMPILE_CONTEXT` (multi-tenancy).** Cube compiles a different data model per security context. The connector ingests the single compiled model that matches the security context carried by its token: set `security_context` when minting a token via the Control Plane API, or rely on the claims baked into a directly-supplied `api_token`. To catalog multiple tenants, run one ingestion per tenant — but their cubes and views share names, so distinguish them with `platform_instance` / `env` (or `cube_pattern` / `view_pattern`) to avoid URN collisions.
- **`FILTER_PARAMS` / `SQL_UTILS` in cube SQL.** The SQL returned by `/v1/meta` is already compiled (`FILTER_PARAMS` render to their defaults and `COMPILE_CONTEXT` is resolved), so Cube Core SQL lineage parsing operates on the resolved SQL and is wrapped defensively if a template still cannot be parsed. On Cube Cloud the Metadata API returns resolved `table_references` / `column_references`, so templating is irrelevant there.

### Limitations

- The `/v1/meta` endpoint does not return cubes or views marked `public: false`. On Cube Cloud the Metadata API may still return them (and the connector merges them in); on Cube Core such cubes are not ingested as datasets, though lineage edges to them are still emitted.
- Warehouse lineage on Cube Cloud requires a metadata-scoped token for the Metadata API (supplied via `api_token`, or minted automatically with `cloud_api_key` + `deployment_id` + `environment_id`). Without it, the connector falls back to `/v1/meta` and only view-to-cube lineage is available.
- The Control Plane **audit-logs export** and **Orchestration API** (pre-aggregation build jobs) are intentionally not used — they are operational/governance surfaces rather than data-catalog metadata, and the audit-logs export is an Enterprise-only CSV stream.
- Column-level lineage from Cube Core SQL parsing is not produced; Core lineage is table-level only.
- **Usage statistics and query profiling are not ingested.** Cube does not expose query history through a pull API — it is only available via [Query History export](https://cube.dev/docs/product/administration/monitoring/query-history-export), which pushes logs to an external sink (e.g. S3). Ingesting that exported data would be a separate pipeline rather than a Metadata API feature.
- Pre-aggregation definitions are not exposed by Cube Core's `/v1/meta` (it returns only `measures`, `dimensions`, `segments`, `hierarchies`, and `folders`); they are an internal caching concern. Where a payload does include them, their names are captured as custom properties.

### Troubleshooting

#### "Required scope is missing" / Metadata API falls back to `/v1/meta`

The configured `api_token` is a regular REST/data token rather than a metadata-scoped token. Either set `cloud_api_key` + `deployment_id` + `environment_id` so the connector mints a metadata token via the Control Plane API, supply a pre-generated metadata token in `api_token`, or set `use_metadata_api: false` to silence the fallback warning.

#### No warehouse lineage appears

Confirm `warehouse_platform` is set (or auto-detected), and that the upstream datasets were ingested with the same `warehouse_platform_instance` and `warehouse_env` you configured here.

#### Warehouse lineage edges do not connect to existing datasets

When run against a DataHub instance (the usual case), the connector reconciles the casing of upstream warehouse table URNs and column names against what the warehouse connector actually ingested — it looks up the real schema in DataHub and snaps Cube's reported identifiers to it. This handles platforms that fold identifiers differently (Postgres/Redshift lower-case, Snowflake upper-case, BigQuery case-sensitive) without per-platform configuration.

When the upstream schema is not yet in DataHub (e.g. the warehouse has not been ingested, or a dry run with no server), there is nothing to reconcile against, so the connector falls back to its configured behaviour: it lowercases upstream warehouse table and column names by default. If the warehouse connector was configured with `convert_urns_to_lowercase: false`, set `convert_lineage_urns_to_lowercase: false` here so the fallback URNs match. Ingesting the warehouse first is the most reliable fix.
