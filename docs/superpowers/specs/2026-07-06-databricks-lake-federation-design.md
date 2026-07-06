# Databricks Lakehouse Federation Lineage — Design

**Date:** 2026-07-06
**Ticket:** ING-2968 (Beta feedback — DataHub not capturing Databricks Lake Federation lineage to external SQL databases)
**Connector:** `metadata-ingestion/src/datahub/ingestion/source/unity/`
**Status:** Design (implementation-ready)

## Problem

Databricks Lakehouse Federation lets a Unity Catalog **foreign catalog** act as a read-only
mirror of an external database (PostgreSQL, SQL Server, MySQL, Snowflake, Redshift, BigQuery,
Oracle, Teradata, another Databricks workspace, Hive/Glue metastores, …). The link is defined by
a Unity Catalog **connection** object plus the catalog's `catalog_type = FOREIGN_CATALOG`.

The Unity Catalog connector is structurally blind to this:

- `Proxy._create_catalog()` (`proxy.py`) copies `catalog_type` into the model but only ever compares
  it against `HIVE_METASTORE_CATALOG`. `FOREIGN_CATALOG` is never handled.
- `CatalogInfo.connection_name` and `CatalogInfo.options` (which carry the link to the external DB)
  are read from the SDK but dropped.
- The connector never calls `WorkspaceClient.connections.list()/.get()`, so it never learns a
  connection's type, host, or target database.

Result: a foreign catalog is emitted as an ordinary Databricks catalog — no `FOREIGN_CATALOG`
subtype, no external reference, no lineage to the external dataset. The customer's
`include_table_lineage` / `include_column_lineage` flags do not help, because federation is a
**structural** relationship, not a query event — it never appears in `system.access` query logs.

The existing `include_external_lineage` path (`source.py`) is **S3-path only** ("Only external S3
tables are supported at the moment") — it handles external *storage locations*, not federated
*databases*.

## Goal

Natively detect foreign catalogs and emit:

1. **Structured properties** on the catalog container marking it federated (facetable).
2. A single, configurable cross-platform **link** to the external dataset — `siblings` (default,
   correct for a mirror) or `lineage` (with optional column-level) — never both.

## Prior art in the codebase

This is the same "a local catalog mirrors an external database" shape DataHub already solves twice:

- **Trino/Presto** (`sql/trino.py`): `KNOWN_CONNECTOR_PLATFORM_MAPPING` maps connector →
  DataHub platform; `catalog_to_connector_details` supplies per-catalog remote database / env /
  platform_instance; `_get_source_dataset_urn()` builds the external URN handling two-tier vs
  three-tier platforms; emits both `Siblings` and `UpstreamLineage` (+ optional column-level),
  gated by `ingest_lineage_to_connectors` / `ingest_siblings_to_connectors`.
- **Redshift** (`redshift/redshift_schema.py`, `lineage.py`): external schemas carry
  `external_platform` + `external_database`; lineage builds the upstream URN and resolves the
  platform instance via `platform_instance_map`.

The design mirrors Trino, adapted to the Databricks SDK.

## Approach

### A. Detection & modelling

Catalogs are **already** emitted as containers today (`gen_catalog_containers()` →
`gen_containers(..., sub_types=[DatasetContainerSubTypes.CATALOG])`). We reuse that container — no
new entity, and the `CATALOG` subtype stays unchanged (subtypes encode the *hierarchy level*
Metastore / Catalog / Schema, which is orthogonal to "is this federated").

- Extend the `Catalog` dataclass (`proxy_types.py`) with `connection_name: Optional[str]` and
  `options: Optional[Dict[str, str]]`; populate them in `Proxy._create_catalog()` from
  `CatalogInfo.connection_name` / `CatalogInfo.options`.
- When `catalog.type == CatalogType.FOREIGN_CATALOG`, mark the catalog container with
  **structured properties** — typed, first-class, and *facetable* in the UI (unlike plain custom
  properties, which are searchable but not a filter facet). Modelled on the existing Snowplow /
  dbt / Snowflake pattern (`snowplow/services/property_manager.py`):
  - **Definitions** — emit a `StructuredPropertyDefinition` once per run for each property
    (`entityTypes=[container]`, `valueType=datahub.string`, `cardinality=SINGLE`):
    `databricks_catalog_type`, `databricks_federation_platform` (with `allowedValues` = the known
    platform ids, giving a clean facet), `databricks_federation_connection`,
    `databricks_federation_remote_database`. Qualified-name namespace is configurable.
  - **Idempotency** — like Snowplow's `_property_exists`, check the definition via the graph
    before (re)writing so a centrally-managed definition is never clobbered. Definition emission
    needs a `DataHubGraph` connection; if unavailable, skip definitions with a warning (values
    still emit — they just won't render until the definition exists).
  - **Assignment** — set values on the foreign-catalog container via
    `gen_containers(..., structured_properties={urn: value})`.

  The existing `CATALOG` subtype and container are untouched — no new subtype, no new entity.

### B. Connection resolution & platform mapping

- Add `Proxy.connections() -> Dict[str, ConnectionInfo]` calling
  `WorkspaceClient.connections.list()` once and caching by connection name. (Verified the API
  exists: `ConnectionsAPI.list(max_results, page_token) -> Iterator[ConnectionInfo]` and
  `.get(name)`.)
- Add a platform map, keyed by `ConnectionType`:

  ```python
  CONNECTION_TYPE_TO_PLATFORM = {
      ConnectionType.MYSQL: "mysql",
      ConnectionType.POSTGRESQL: "postgres",
      ConnectionType.SQLSERVER: "mssql",
      ConnectionType.SQLDW: "mssql",
      ConnectionType.SNOWFLAKE: "snowflake",
      ConnectionType.REDSHIFT: "redshift",
      ConnectionType.BIGQUERY: "bigquery",
      ConnectionType.GLUE: "glue",
      ConnectionType.ORACLE: "oracle",
      ConnectionType.TERADATA: "teradata",
      ConnectionType.DATABRICKS: "databricks",
      ConnectionType.HIVE_METASTORE: "hive",
  }
  ```

  (All platform ids confirmed against `bootstrap_mcps/data-platforms.yaml`.) Unmapped or
  `UNKNOWN_CONNECTION_TYPE` connections are skipped with a report warning — no silent drop.

### C. External URN construction

The remote database name lives in the **connection-type-specific** `options` key on the foreign
catalog (confirmed from the `CREATE FOREIGN CATALOG` reference):

| Connection type | Platform | Namespace | Remote-DB source | URN dataset name |
| --- | --- | --- | --- | --- |
| POSTGRESQL, SQLSERVER, SQLDW, SNOWFLAKE, REDSHIFT, ORACLE, TERADATA | postgres / mssql / snowflake / redshift / oracle / teradata | 3-tier | `options["database"]` | `{db}.{schema}.{table}` |
| MYSQL | mysql | 2-tier (no db option) | — | `{schema}.{table}` |
| BIGQUERY | bigquery | project + dataset | `options["dataProjectId"]` | `{dataProjectId}.{schema}.{table}` |
| DATABRICKS → DATABRICKS | databricks | 3-tier | `options["catalog"]` | `{catalog}.{schema}.{table}` |
| GLUE, HIVE_METASTORE | glue / hive | 2-tier | — | `{schema}.{table}` |

Resolution via a `DATABASE_OPTION_KEY: Dict[ConnectionType, Optional[str]]` map
(`"database"` / `"dataProjectId"` / `"catalog"` / `None`). A `None` key means two-tier — no DB
prefix. Schema and table names map **1:1** to the remote system (a foreign catalog "mirrors a
database"; BigQuery datasets map to Databricks schemas), so the schema/table names the connector
already iterates over are the remote names — no translation needed beyond the DB prefix.

Precedence for each field: **config override > auto-detected value**. `env` and
`platform_instance` come from the override map, else the connector defaults.

**URN normalization must match the target source's ingestion.** The synthesized external URN has
to byte-match the URN the external source's own connector emitted, or the sibling/lineage link
silently dangles. Two axes:

- **Case**: if the external source was ingested with `convert_urns_to_lowercase: true`, the
  external URN name must be lowercased identically. Apply the Unity source's own
  `convert_urns_to_lowercase` (the mirror is on the same side), and expose a per-connection
  override for the rare case the two sources disagree.
- **Platform instance**: must equal the `platform_instance` the external source was ingested under
  — supplied via `federation_connection_details.<conn>.platform_instance`.

This is validated against the real customer (Moneybox): they ingest both the Databricks catalog
*and* the mssql database `prod00-moneybox`, both with `convert_urns_to_lowercase: true`, and the
Databricks side carries a `platform_instance`. This same customer already suffered a
PowerBI↔Databricks lineage break from a lowercase + platform-instance URN mismatch, so getting
this exactly right is the feature's highest-risk requirement.

### D. Emission — one link type, not both

A foreign catalog is a read-only **mirror** — same physical data, no transformation. That makes
`Siblings` and `UpstreamLineage` two *mutually exclusive* ways to express the same link, not two
things to emit together (once siblings merge the two entities in the UI, a lineage edge between
them is self-referential). A single `federation_link_type: siblings | lineage | none` selects one.

- **`siblings`** (default): bidirectional `Siblings` aspects between the Databricks foreign table
  and the external dataset — the UI merges them into one logical dataset. Semantically correct for
  a mirror, and the most direct answer to "show the link to the SQL database." Primary side
  configurable (external dataset primary by default).
- **`lineage`**: one `Upstream` (`DatasetLineageType.COPY`) from the foreign table to the external
  dataset — distinct nodes with a data-flow edge, for teams who prefer a lineage arrow or whose
  external source isn't co-ingested. This is the only mode where column-level lineage is
  meaningful: when `include_column_lineage` is enabled, emit `fineGrainedLineages` as identity
  (1:1) field mappings.
- **`none`**: structured properties only (Section A), no cross-platform link.

Reuses the shape of Trino's `gen_siblings_workunit` / `gen_lineage_workunit`, but collapses Trino's
two independent flags into a single exclusive mode since the mirror relationship is unambiguous.

### E. Config surface

Added to `UnityCatalogSourceConfig`:

```yaml
federation_link_type: siblings         # siblings | lineage | none  (default: siblings)
emit_federation_structured_properties: true   # define+assign structured props (default True)
federation_connection_details:         # optional per-connection override map
  <connection_name>:
    platform: mssql                     # override auto-detected platform
    platform_instance: prod-sql         # align with a separately-ingested source
    env: PROD
    database: <remote_db>               # override when the options key is empty/unexpected
```

Auto-detect fills everything; the map only overrides. This lets the customer align
`platform_instance` with their separately-ingested SQL Server source and provides a fallback for
any source whose `options` key or permissions differ.

## Risks & mitigations

1. **`connections.list()` permissions.** Listing connections requires metastore-admin or
   connection ownership; the customer already hit permission walls on the system catalog.
   `catalog.connection_name` comes free from `catalogs.list()` (already readable), but the
   connection *type* needs the connections API.
   **Mitigation:** catch permission/API errors on `Proxy.connections()`, report a clear warning,
   and let `federation_connection_details.<conn>.platform` supply the platform explicitly so
   federation works with zero connections-API access.
2. **Non-standard `options` keys** (e.g. Salesforce, Synapse, exotic sources).
   **Mitigation:** the `federation_connection_details.database` override absorbs any key we don't
   map; unmapped connection types are skipped with a warning rather than emitting a wrong URN.
3. **Platform-instance mismatch** with a separately-ingested external source (URNs won't line up).
   **Mitigation:** `platform_instance` / `env` override map (same purpose as Trino's
   `catalog_to_connector_details` and Redshift's `platform_instance_map`).
4. **Two-tier vs three-tier URN mistakes** produce dangling lineage.
   **Mitigation:** explicit `DATABASE_OPTION_KEY` map drives tier selection; unit tests cover each
   connection type.
5. **URN mismatch with the external source** (highest risk) — wrong case or platform_instance
   makes the sibling/lineage link dangle silently. This customer has already hit this exact class
   of bug (PowerBI↔Databricks).
   **Mitigation:** apply the source's `convert_urns_to_lowercase` to the external URN, take
   `platform_instance` from the override map, and cover both axes in unit tests with a
   lowercase + platform-instance golden case.
6. **Structured-property definitions are org-global** and require a `DataHubGraph` connection;
   auto-creating them could collide with centrally-managed definitions.
   **Mitigation:** idempotent existence-check before write (Snowplow pattern), a configurable
   qualified-name namespace, and an `emit_federation_structured_properties` off-switch. When no
   graph is available, definitions are skipped with a warning and lineage/siblings still emit.

## Testing

- **Unit** (`tests/unit/...`, mocked `CatalogInfo` / `ConnectionInfo`, no live Databricks):
  - `CONNECTION_TYPE_TO_PLATFORM` and `DATABASE_OPTION_KEY` resolution.
  - URN construction: two-tier (mysql/glue/hive) vs three-tier (postgres/mssql/snowflake/redshift)
    vs BigQuery (`dataProjectId`) vs Databricks-to-Databricks (`catalog`).
  - Override-map precedence over auto-detected values.
  - `Foreign Catalog` subtype emitted for `FOREIGN_CATALOG`, not for managed catalogs.
  - Graceful handling when the connections API raises / returns nothing.
- **Integration** (extend the existing Unity mock-proxy fixture): add a `FOREIGN_CATALOG` catalog +
  a connection, assert golden lineage / siblings / subtype workunits.

## Out of scope

- Non-federation external-storage lineage (already covered by `include_external_lineage` for S3).
- Ingesting the external source's own metadata (that remains the job of the dedicated connector for
  PostgreSQL / SQL Server / etc.); this feature only stitches lineage/siblings between them.

## Interim workaround for the customer

A file-based `type: lineage` recipe stitching the URNs manually works today and can be recommended
while native support ships — but it is manual and brittle, which is exactly why native support is
warranted.
