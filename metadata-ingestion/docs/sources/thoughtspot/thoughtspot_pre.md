### Overview

The `thoughtspot` module pulls metadata from ThoughtSpot via REST API v2.0. Liveboards become DataHub Dashboards, Answers and Visualizations become Charts, and Worksheets and Tables become Datasets. If the ingestion principal can enumerate Orgs, those entities are organised under Workspace containers; if not, the catalog is flat.

### Prerequisites

Before running ingestion, ensure:

- Network connectivity to your ThoughtSpot instance.
- Valid authentication credentials: username + secret_key (trusted auth, recommended) or username + password (basic auth).
- A ThoughtSpot user (or service account) that satisfies the privilege and per-object access requirements described in [Required Permissions](#required-permissions).

#### Required Permissions

ThoughtSpot enforces two independent access checks on the APIs this connector calls. Both must be satisfied for end-to-end ingestion.

**1. User-level privileges**

The ingestion principal needs at minimum:

- `Has data download permissions` (`DATADOWNLOADING`) — required to read metadata.
- `Can manage data` or membership in a group that has it — required if you want to ingest objects across organizations (`all_orgs_override` scenarios).

For most deployments, granting `Has administration privileges` (`ADMINISTRATION`) is the simplest path and covers all of the above.

**2. Per-object sharing**

This one trips people up: the user must be able to **read every object you want to ingest**, and — for lineage — must be able to read each object's underlying connections and tables.

ThoughtSpot's TML export endpoint (`POST /api/rest/2.0/metadata/tml/export`), which this connector uses for cross-object lineage, walks the full dependency graph of a worksheet — its connection, every joined table, every referenced answer — and returns `FORBIDDEN: Cannot download TML due to lack of access to objects` if any one is not visible to the calling user. Granting `ADMINISTRATION` does **not** override per-object ACLs for TML export.

In practice this means:

- Either make the ingestion user the owner of the objects you want to ingest, **or**
- Share each connection, worksheet, table, and liveboard with that user (or with a group the user belongs to). In the ThoughtSpot UI: open the object → **Share** → add the user/group with at least **Can view** access.

**Failure mode and what to do about it**

If TML access is missing, the connector emits a structured warning per object that names the failing GUID and the underlying error (typically `FORBIDDEN` or `SAGE_INVALID_QUESTION` for orphaned references). For example:

```
TML Export Failed: Cannot download TML for ANSWER 'My Answer'.
  id=…, error_code=…, error=Error Code: FORBIDDEN …
```

Schema (column lists) is fetched from `metadata/search` and is **not** subject to the TML per-object gate, so column-level metadata is emitted as soon as the user can read the worksheet itself. Lineage that depends on TML may be partial until the per-object sharing is fixed.

#### Tags

The connector pulls the full tag list once per run via `/api/rest/2.0/tags/search` and emits a DataHub `GlobalTags` aspect on each entity (liveboard / chart / dataset) whose `metadata_header.tags` references a known tag id. Tag colors and other tag-side metadata are not currently propagated — only the tag name, which becomes the DataHub tag URN (`urn:li:tag:<name>`).

If your principal can't enumerate tags (the `tags/search` endpoint returns 403 or 404), the connector logs an INFO and continues — entities are emitted without tag aspects rather than failing the pipeline. This is the same graceful-degradation pattern used for `/orgs/search`.

#### Multi-Org Deployments

ThoughtSpot supports multi-org tenancy where a single cluster hosts multiple isolated orgs (each with their own liveboards, worksheets, and connections). By default the connector ingests from whichever org the ingestion principal is logged into.

To scope ingestion to a specific org, set `connection.org_identifier` to either:

- The numeric org id (e.g. `"615000845"`)
- The org name (e.g. `"datahub"`)

```yaml
source:
  type: thoughtspot
  config:
    connection:
      base_url: "https://your-cluster.thoughtspot.cloud"
      username: "service-account@example.com"
      secret_key: "..."
      org_identifier: "production" # ingest only from the 'production' org
```

To ingest multiple orgs, run the connector once per org with different recipes. There's no single-recipe multi-org mode — that would conflict with the per-object permission gates ThoughtSpot enforces at the org boundary.

#### Authentication Methods

The connector supports two authentication methods, declared in the recipe as an `auth` block under `connection`. Both mint a short-lived bearer token per ingestion run via ThoughtSpot's `auth_token_full` endpoint, so the credentials stored on disk remain stable across token expiry.

1. **Trusted Authentication** (recommended for production)

   ```yaml
   connection:
     base_url: "https://your-cluster.thoughtspot.cloud"
     auth:
       type: trusted
       username: "user@example.com"
       secret_key: "${THOUGHTSPOT_SECRET_KEY}"
   ```

   Generate a secret key in ThoughtSpot under **Develop > Customizations > Security Settings > Enable Trusted Authentication**. The connector mints fresh bearer tokens per run from the secret key.

2. **Password Authentication**

   ```yaml
   connection:
     base_url: "https://your-cluster.thoughtspot.cloud"
     auth:
       type: password
       username: "user@example.com"
       password: "${THOUGHTSPOT_PASSWORD}"
   ```

For backwards compatibility, the legacy flat shape (`username` / `secret_key` / `password` directly under `connection:` without an `auth:` wrapper) continues to work — existing recipes do not need to be updated. New recipes should use the explicit `auth:` block.

#### Cross-Platform Lineage

When a ThoughtSpot Worksheet is backed by a federated connection (Databricks, Snowflake, BigQuery, Redshift, etc.), the connector emits upstream lineage edges from the Worksheet dataset to the underlying physical table — both table-level and column-level — so the lineage view continues past ThoughtSpot into the warehouse.

Cross-platform lineage is on by default (`include_external_lineage: true`). For the edges to land on the matching DataHub source dataset, configure `external_connections` with the `platform_instance` (and optionally `env`) that your existing Databricks / Snowflake / etc. ingestion uses:

```yaml
external_connections:
  # Key by TS connection GUID (stable across renames) or display name.
  conn-guid-prod-dbx:
    platform_instance: prod-databricks # matches your DataHub Databricks ingestion
    env: PROD
  Prod BigQuery:
    platform_instance: prod-bq
    # BigQuery preserves source case in column URNs — opt in here.
    preserve_column_case: true
```

Each value is an `ExternalConnectionConfig` with three optional fields:

- **`platform_instance`** — DataHub `platform_instance` to attach to the upstream URN. Match whatever your existing source ingestion uses or omit to emit URNs without an instance prefix.
- **`env`** — Per-connection `env` override (e.g. `STAGING`). Defaults to the connector-level `env`.
- **`preserve_column_case`** — Default `false`. When `false`, external column names are lowercased to match the canonical schemaField URN convention used by the Databricks, Snowflake, Postgres, MySQL, Redshift, and Hive connectors. Set to `true` for BigQuery, SQL Server, or any Databricks tenant using case-quoted identifiers — anywhere the upstream connector preserves source case.

Connections that aren't listed in `external_connections` still emit cross-platform edges using connector-level defaults (no `platform_instance` prefix, connector `env`, lowercased columns).

Typos in the connection-key are caught and warned at ingestion time: if `external_connections` references a name or GUID that doesn't match any real TS connection, the run emits an `External Lineage Config Keys Unmatched` warning naming the offending key. Sub-field typos (e.g. `platfom_instance`) fail loud at config-load time — `ExternalConnectionConfig` uses `extra='forbid'`.

To disable cross-platform lineage entirely (keep lineage scoped to ThoughtSpot only), set `include_external_lineage: false`.

#### Entity Hierarchy and Container Relationships

ThoughtSpot content is organized hierarchically:

```
Workspace (Container)
├── Liveboards (Dashboards)
│   └── Visualizations (Charts)
├── Answers (Charts)
└── Worksheets/Tables (Datasets)
```

The connector preserves this hierarchy by:

- Creating Container entities for Workspaces using `gen_containers()` helper
- Linking Liveboards, Answers, and Datasets to their parent Workspace containers via `_set_container()`
- Establishing relationships between Visualizations and their parent Liveboards via `set_charts()`

#### Visualization Support

Each Liveboard contains one or more Visualizations (chart tiles), emitted as Chart entities with the `Visualization` subtype. Visualization types (e.g., `LINE_CHART`, `BAR_CHART`, `TABLE`), the natural-language search query that produced the chart, and the parent Liveboard relationship are captured. Source-table lineage at both table and column level is extracted from the Visualization's TML payload — see the Lineage Coverage section in the post-config docs for the full edge taxonomy.

Visualizations are distinct from standalone Answers: they live inside a Liveboard and emit the `Visualization` subtype, while Answers (independent saved searches) emit the `Answer` subtype. Both share the `Chart` entity type so the DataHub UI handles them uniformly.

#### Usage Statistics

Set `include_usage_stats: true` to emit per-Liveboard / per-Chart view counts as `DashboardUsageStatistics` / `ChartUsageStatistics` aspects. The view count comes from TS REST v2 `metadata_search`'s `include_stats` response — the same global counter shown in the TS UI's "Views" column.

The connector also emits two custom properties on each Liveboard / Chart when the data is available:

- `thoughtspot_favorites` — number of users who have marked the entity as a favourite.
- `thoughtspot_last_accessed` — most-recent access time (ISO 8601 UTC).

The ingestion principal needs ordinary read access on the entity. There's no `searchdata` or BI-Server permission to grant separately, since the view counts ride on the same `metadata_search` call that builds the entity.

Counts are lifetime-cumulative. ThoughtSpot doesn't expose a time-bounded view counter on this endpoint, so the `start_time`/`end_time` knobs you may have seen on other connectors don't apply here. If you need windowed usage, diff DataHub's own `dashboardUsageStatistics` timeseries across runs.
