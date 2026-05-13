### Capabilities

The connector emits the following aspects on every supported entity:

- **`subTypes`** — `Liveboard`, `Visualization`, `Answer`, `Worksheet`, `View`, or `Table`, so the DataHub UI shows the ThoughtSpot-native label rather than a generic Dashboard/Chart/Dataset badge.
- **`browsePathsV2`** — entities are browsable under their parent Workspace when workspace enumeration is available.
- **`status`** — emitted by the SDK V2 entity classes so stateful stale-removal can soft-delete entities that disappear between runs.
- **`dataPlatformInstance`** — set when `platform_instance` is configured.
- **`ownership`** — populated from each entity's `author.name`; UUIDs (which TS sometimes substitutes for the username) are skipped.
- **`globalTags`** — every tag attached to a Liveboard/Answer/Worksheet is resolved to `urn:li:tag:<name>`. Tag colors and other tag-side metadata are not propagated.

#### Container Hierarchy

ThoughtSpot Workspaces (Orgs) emit as Container entities and parent the Liveboards, Answers, and Worksheets that live in them. Enumerating Orgs requires **Has administration privileges** at the system org (orgID 0); without it, the connector logs an INFO and emits a flat catalog (entities directly under `platform: thoughtspot` with no Workspace container above them).

#### Lineage Coverage

- **Worksheet → Connection tables** — table-level and column-level lineage from TS's pre-resolved `columns[*].sources` field on `metadata/search`. Each source reference becomes a `FineGrainedLineage` edge linking the Worksheet column to the underlying physical table column.
- **Worksheet → External warehouse (cross-platform)** — federated Worksheets emit upstream lineage past ThoughtSpot to the underlying Databricks / Snowflake / BigQuery / Redshift / etc. table at both table and column level. The URN is built from TS's `physicalDatabaseName` / `physicalSchemaName` / `physicalTableName` and routed via `external_connections` (see below).
- **Visualization → Worksheet** — table-level lineage from TML's `liveboard.visualizations[].answer.tables[].fqn`, plus column-level lineage from the bracketed `[col]` tokens in `answer.search_query` (rendered via the chart `inputFields` aspect).
- **Answer → Worksheet** — table-level lineage from TML's `answer.tables[].fqn`, plus column-level lineage from the bracketed `[col]` tokens in `answer.search_query` (rendered via the chart `inputFields` aspect). Same shape as the Visualization path, sourced from each Answer's TML export.
- **Liveboard → Worksheet (fallback)** — used when per-viz TML export is forbidden. The connector falls back to `reportContent...pinboardFilterDetails` for dashboard-level dataset lineage; the chart layer is lost but dashboard-to-dataset edges remain intact.

#### Cross-Platform Lineage

Cross-platform lineage is on by default (`include_external_lineage: true`). For the edges to land on the matching DataHub upstream dataset, configure `external_connections` with the `platform_instance` (and optionally `env`) that your existing Databricks / Snowflake / BigQuery / etc. ingestion uses:

```yaml
external_connections:
  # Keys are the TS connection GUID (stable across renames) or display name.
  conn-guid-prod-dbx:
    platform_instance: prod-databricks # match your DataHub Databricks ingestion
    env: PROD
  "Prod BigQuery":
    platform_instance: prod-bq
    # BigQuery preserves source case in column URNs — opt in here.
    preserve_column_case: true
```

Each entry accepts three optional fields:

- **`platform_instance`** — DataHub `platform_instance` to attach to the upstream URN. Match what your existing upstream ingestion uses, or omit to emit URNs without an instance prefix.
- **`env`** — Per-connection `env` override (e.g. `STAGING`). Defaults to the connector-level `env`.
- **`preserve_column_case`** — Default `false`, which lowercases external column names to match the canonical schemaField URN convention used by Databricks, Snowflake, Postgres, MySQL, Redshift, and Hive. Set to `true` for BigQuery, SQL Server, or any Databricks tenant using case-quoted identifiers.

Unlisted connections still emit cross-platform edges using connector-level defaults. Sub-field typos (e.g. `platfom_instance`) fail loud at config-load time. Connection-key typos surface as an `External Lineage Config Keys Unmatched` warning at ingestion time. Disable cross-platform lineage entirely with `include_external_lineage: false`.

#### Multi-Org Tenants

ThoughtSpot supports multi-org tenancy where a single cluster hosts multiple isolated orgs (each with their own liveboards, worksheets, and connections). By default the connector ingests from whichever org the principal is logged into. To scope a run to a single org, set `connection.org_identifier` to the numeric org id (e.g. `"615000845"`) or the org name (e.g. `"production"`). Multi-org coverage requires one recipe per org — there is no single-recipe multi-org mode, since that would conflict with the per-object permission gates ThoughtSpot enforces at the org boundary.

#### Usage Statistics

Per-Liveboard / per-Chart view counts emit by default as `DashboardUsageStatistics` / `ChartUsageStatistics` aspects. The count comes from `metadata_search`'s `include_stats` response — the same global counter the TS UI's "Views" column displays. No extra permission or API round-trip is required; the data piggybacks on the `metadata_search` calls that build each entity. Set `include_usage_stats: false` to skip emitting the aspects.

The connector also emits two custom properties when available:

- `thoughtspot_favorites` — number of users who have favourited the entity.
- `thoughtspot_last_accessed` — most recent access time (ISO 8601 UTC).

Counts are lifetime-cumulative. ThoughtSpot doesn't expose a time-bounded counter on this endpoint, so the `start_time`/`end_time` knobs from other connectors don't apply. For windowed usage, diff DataHub's own `dashboardUsageStatistics` timeseries across runs.

#### Visualizations vs. Answers

Each Liveboard contains one or more Visualizations (chart tiles), emitted as Chart entities with the `Visualization` subtype. Standalone Answers (independent saved searches) emit the `Answer` subtype. Both share the `Chart` entity type so the DataHub UI handles them uniformly.

#### Tags

The connector pulls the full tag list once per run via `/api/rest/2.0/tags/search` and emits a `GlobalTags` aspect on each entity whose `metadata_header.tags` references a known tag id. If the principal can't enumerate tags (the endpoint returns 403 or 404), the connector logs an INFO and continues — entities are emitted without tag aspects rather than failing the pipeline.

#### Tag-Based Filtering

To restrict ingestion to a subset of tagged entities, set `liveboard_tag_filter` and/or `answer_tag_filter` to a list of TS tag names. The filter is applied server-side on `metadata_search` — cheaper than fetching everything and discarding client-side, and useful for tenants where the ingestion principal can see far more entities than the desired subset.

```yaml
liveboard_tag_filter:
  - "Production"
  - "Curated"
answer_tag_filter:
  - "KPI"
```

Both filters are optional and independent — set one, the other, or both. Tag names are **case-sensitive** (`"Production"` and `"production"` are different tags); a typo'd filter that matches zero entities surfaces a `Tag filter matched 0 ...` warning in the run report so it doesn't silently produce an empty catalog. Tag-based filtering only narrows Liveboards / Answers; Worksheets and Tables are unaffected and stay subject to `worksheet_pattern`.

#### Stateful Ingestion

Set `stateful_ingestion.remove_stale_metadata: true` (with `enabled: true`) in your recipe to soft-delete entities that disappear between runs. The handler diffs each run's emitted URN set against the prior checkpoint and emits `status(removed=true)` MCPs for missing entities.

### Limitations

#### Container hierarchy requires system-org admin

Enumerating Workspaces uses ThoughtSpot's `/orgs/search` endpoint, which is gated by **Has administration privileges** at the system org (orgID 0). Regular org admins get a 403; the connector logs an INFO and continues with a flat catalog. This is intentional — peer connectors (Atlan, OpenMetadata) follow the same "use the native container API or fall back to flat" pattern rather than inventing synthetic containers.

#### Pinboard ⇄ Liveboard naming

ThoughtSpot renamed Pinboards to Liveboards around v8.4, but the REST API rename is incomplete. The connector handles the inconsistency in three places: the top-level entity (`metadata_type: LIVEBOARD`), the embedded `reportContent.sheets[].sheetContent.pinboardFilterDetails` JSON path used for fallback liveboard-level lineage, and UI deep-link URLs (still served under `/#/pinboard/{id}`). Older tutorials that reference Pinboards map cleanly to DataHub Dashboards with the `Liveboard` subtype.

#### Folders / Collections are not modelled

ThoughtSpot Collections (formerly Folders) are not exposed via the public v2 `metadata/search` API and are not currently emitted as containers.

#### Bearer-token auth is not supported

Recipes must use trusted auth (`username + secret_key`) or password auth (`username + password`). Pre-generated bearer tokens are deliberately rejected because their minute-to-hour TTL fails scheduled ingestion at the first expiry. Both supported modes mint fresh bearer tokens per run via `auth_token_full`.

### Troubleshooting

#### Charts disappear with a `Liveboard Visualization Fetch Failed` warning

This is the most common failure mode. ThoughtSpot's TML export endpoint enforces per-object ACLs that override admin-level privileges — the principal must have **direct sharing** on each Liveboard, its underlying Worksheets, the Worksheets' Connections, and every joined Table. A single missing share anywhere in the dependency tree triggers `FORBIDDEN: Cannot download TML due to lack of access to objects`.

Fix: in the ThoughtSpot UI, open each affected object → **Share** → grant the ingestion user (or its group) at least **Can view** access. `Has administration privileges` does **not** bypass per-object ACLs for TML export.

#### `0 workspaces scanned` in the ingestion report

Expected when the principal isn't a system-org admin. See [Container hierarchy requires system-org admin](#container-hierarchy-requires-system-org-admin) above — the catalog is flat in this case.

#### Multi-Org tenants ingest from the wrong org

Set `connection.org_identifier` to the numeric org id (e.g. `"615000845"`) or the org name (e.g. `"production"`). Without it, ingestion runs against the principal's primary org.

#### Usage statistics are missing

Usage statistics are emitted by default. If view counts are absent on emitted Liveboards/Charts:

- The recipe may have `include_usage_stats: false`. Remove that line or set it to `true`.
- The entity may have zero views. The connector deliberately skips emitting `viewsCount: 0` to avoid clobbering a non-zero count from a prior run.
- The principal may lack read access to the entity. Without read access the entity itself wouldn't be ingested — check the run report for parse failures or skipped entities.

If counts look lower than the TS UI on a recent TS version, file an issue with the version and a sample entity GUID.

#### Cross-platform upstream edges dangle in the lineage UI

The Worksheet shows the table-level edge to the warehouse but column-level edges resolve to `dangling` schemaField URNs. Two common causes:

1. **`platform_instance` mismatch.** The Worksheet's external URN must match the dataset URN your existing warehouse ingestion emits. Find the upstream's `platform_instance` in any of its DataHub URNs (the segment between the platform name and the table name) and set the matching value in `external_connections.<conn>.platform_instance`.
2. **Column-case mismatch.** TS often emits column names in upper case (`PET_ID`) but most upstream connectors lowercase column URNs (`pet_id`). The default behaviour lowercases external column names to match. If your upstream connector preserves case — BigQuery, SQL Server, or a Databricks tenant using case-quoted identifiers — set `preserve_column_case: true` on that connection's `external_connections` block.

#### `External Lineage Resolution Failed` warning

The connector found Worksheets that reference TS connections it couldn't read. Either grant `Can view` on the connection page in the TS UI, or remove the dead reference upstream.

#### `External Lineage Config Keys Unmatched` warning

A key in `external_connections` doesn't match any real TS connection — usually a typo. Verify the GUID or display name in the ThoughtSpot UI matches exactly. The warning lists the offending keys.
