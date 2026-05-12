### Capabilities

The connector emits the following aspects on every supported entity:

- **`subTypes`** — `Liveboard`, `Visualization`, `Answer`, `Worksheet`, `View`, or `Table` so the DataHub UI renders the ThoughtSpot-native label rather than the generic Dashboard/Chart/Dataset badge.
- **`browsePathsV2`** — every entity is browsable under its parent Workspace when workspace enumeration is available.
- **`status`** — emitted by the SDK V2 entity classes so stateful stale-removal can soft-delete entities that disappear between runs.
- **`dataPlatformInstance`** — set when `platform_instance` is configured in the recipe.
- **`ownership`** — populated from each entity's `author.name` field unless that field contains a UUID (which TS sometimes substitutes for the username).
- **`globalTags`** — every tag attached to a Liveboard/Answer/Worksheet is resolved to a DataHub tag URN. Tag colors and other tag-side metadata are not propagated.

#### Lineage Coverage

- **Worksheet → Connection tables**: column-level lineage from TML's `column.sources[]` arrays. Each source reference becomes a `FineGrainedLineage` edge linking the Worksheet column to the underlying physical table column.
- **Worksheet → External warehouse (cross-platform)**: federated Worksheets emit upstream lineage past ThoughtSpot to the underlying Databricks / Snowflake / BigQuery / Redshift / etc. table — both table-level and column-level. The URN is built from TS's `physicalDatabaseName` / `physicalSchemaName` / `physicalTableName` and routed to the right DataHub platform-instance via `external_connections` (see the Cross-Platform Lineage section in Prerequisites). Each edge carries an `auditStamp` sourced from the TS table's `modified` ms-epoch and author URN so the lineage UI shows when the edge was last observed.
- **Visualization (chart inside a Liveboard) → Worksheet**: table-level lineage from TML's `liveboard.visualizations[].answer.tables[].fqn` plus column-level lineage from the bracketed tokens in `answer.search_query` (rendered via the chart `inputFields` aspect).
- **Answer (standalone saved search) → Worksheet**: same shape as the Visualization path, sourced from the Answer's TML export.
- **Liveboard → Worksheet (fallback)**: when per-viz TML export is forbidden, the connector falls back to `reportContent.sheets[].sheetContent.pinboardFilterDetails` to extract dashboard-level dataset lineage. This loses the chart layer but keeps dashboard-to-dataset edges intact.

#### Stateful Ingestion

Set `stateful_ingestion.remove_stale_metadata: true` in your recipe to soft-delete entities that disappear between runs. The handler diffs each run's emitted URN set against the prior checkpoint and emits `status(removed=true)` MCPs for missing entities. Requires `stateful_ingestion.enabled: true`.

### Limitations

#### Container hierarchy requires system-org admin

The connector models Workspaces as Containers using ThoughtSpot's Org concept. Enumerating orgs requires the cluster-level **Has administration privileges** granted at the system org (orgID 0). Regular org admins get a 403 on `/orgs/search` and the connector logs an INFO and continues — entities ingest correctly but without container hierarchy.

If your ingestion principal can't be granted system-org admin, the resulting catalog is flat (all entities directly under `platform: thoughtspot` with no Workspace container above them). This is intentional: peer connectors (Atlan, OpenMetadata) follow the same "use the native container API or fall back to flat" pattern rather than inventing synthetic containers.

#### Pinboard ⇄ Liveboard naming

ThoughtSpot renamed Pinboards → Liveboards around v8.4 (2020–2021) but the REST API rename was incomplete. The connector handles the inconsistency in three places:

- **Top-level entity**: requests and parses `metadata_type: LIVEBOARD` from REST API v2.
- **Embedded TML fields**: the legacy `reportContent.sheets[].sheetContent.pinboardFilterDetails` JSON path is still emitted by v2 TML export; the connector reads it for liveboard-level upstream lineage.
- **UI deep-link URLs**: external links use `{base_url}/#/pinboard/{id}` because the TS web app still serves Liveboards under the legacy `/#/pinboard/` route.

If you're following older ThoughtSpot tutorials that reference Pinboards, the connector maps them to DataHub Dashboards with the `Liveboard` subtype.

#### Folders / Collections are not modelled

ThoughtSpot's Collections (formerly Folders) for organizing content are not exposed via the v2 `metadata/search` API under any documented type. The connector does not currently model them as Containers — folder hierarchy is not available in DataHub even when the principal has admin access. This may change if ThoughtSpot exposes Collections through the public REST surface.

#### Bearer-token auth is not supported

Recipes must use `username + secret_key` (trusted auth) or `username + password` (basic auth). Pre-generated bearer tokens are deliberately not accepted because they have minute-to-hour TTLs that fail scheduled ingestion at the first expiry. Both supported modes mint fresh bearer tokens per run via `auth_token_full`.

### Troubleshooting

#### Charts disappear with `Liveboard Visualization Fetch Failed` warning

This is the most common failure mode. ThoughtSpot's TML export endpoint enforces per-object ACLs that override admin-level privileges — the principal must have **direct sharing** on each Liveboard, its underlying Worksheets, the Worksheets' Connections, and every joined Table. A single missing share anywhere in the dependency tree triggers `FORBIDDEN: Cannot download TML due to lack of access to objects`.

Fix: in the ThoughtSpot UI, open each affected object → **Share** → grant the ingestion user (or its group) at least **Can view** access. Granting **Has administration privileges** does NOT override per-object ACLs for TML export.

#### `0 workspaces scanned` in the ingestion report

Expected when the principal isn't a system-org admin. The connector falls back to flat hierarchy. See the "Container hierarchy requires system-org admin" section in Limitations.

#### Multi-Org tenants ingest from the wrong org

Set `connection.org_identifier` to the numeric org id (e.g. `"615000845"`) or the org name (e.g. `"production"`). Without it, ingestion runs against the principal's primary org. Multi-org runs require one recipe per org.

#### Usage statistics are missing

Set `include_usage_stats: true` in the recipe. If view counts are still missing on emitted Liveboards/Charts:

- The entity may have zero views. The connector deliberately skips emitting `viewsCount: 0` to avoid clobbering a non-zero count from a prior run.
- The ingestion principal may lack read access to the entity. Without read access the entity itself wouldn't be ingested either — check the run report for parse failures or skipped entities.

If counts look lower than the TS UI: the `metadata_search` counter should match the UI's "Views" column exactly. If you see drift on a recent TS version, file an issue with the version and a sample entity GUID.

#### Cross-platform upstream edges dangle in the lineage UI

A Worksheet shows the table-level edge to the external warehouse but the column-level edges resolve to `dangling` schemaField URNs. Two common causes:

1. **`platform_instance` mismatch.** The Worksheet's external URN must match the dataset URN your existing Databricks / Snowflake / etc. ingestion emits. Find the upstream's `platform_instance` in any of its DataHub URNs (the segment between the platform name and the table name) and set the matching value in `external_connections.<conn>.platform_instance`. Without this, table-level edges land on a different `platform_instance` slot from the real upstream.
2. **Column-case mismatch.** TS often emits column names in upper-case (`PET_ID`) but most upstream connectors lowercase column URNs (`pet_id`). The default behaviour now lowercases external column names to match. If your upstream connector preserves case — BigQuery, SQL Server, or a Databricks tenant using `` `Pet_ID` `` -style case-quoted identifiers — set `preserve_column_case: true` on that connection's `external_connections` block.

#### `External Lineage Resolution Failed` warning

The connector found Worksheets that reference TS connections it couldn't read. Either the principal lacks `Can view` on the connection page, or the connection has been deleted from TS but Worksheet references remain. The warning names the count; fix by sharing the connections with the ingestion principal in the TS UI.

#### `External Lineage Config Keys Unmatched` warning

A key in `external_connections` doesn't match any real TS connection. Usually a typo — verify the GUID or display name in the ThoughtSpot UI matches exactly. The warning lists the offending keys.
