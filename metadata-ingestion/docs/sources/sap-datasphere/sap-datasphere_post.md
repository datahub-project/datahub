### Capabilities

#### Lineage extraction

When `include_lineage: true` is set on the recipe, the connector emits both
table-level and column-level lineage by walking the CSN (Core Schema Notation)
returned by the supported per-object-type endpoint under
`/dwaas-core/api/v1/spaces/{space}/{views,analyticmodels}/{name}` — the same
surface the official `datasphere` CLI uses.

**Table-level lineage** is derived from the asset's `query.SELECT.from` clause
(direct refs, joins, and inline subqueries) plus `@remote.source` annotations on
federated remote tables. For Analytic Models, the star-schema fact and dimension
sources come from the business layer, which is authoritative over the query's
`FROM`.

**Column-level lineage** walks `query.SELECT.columns[]` and emits one
`FineGrainedLineage` entry per downstream column with at least one resolvable
upstream column. Supported expression shapes:

| CSN shape          | Example                                         | Result                                  |
| ------------------ | ----------------------------------------------- | --------------------------------------- |
| Direct ref         | `{ref: [AMOUNT]}`                               | `IDENTITY` transformation               |
| Aliased ref        | `{ref: [BASE, X], as: y}`                       | `RENAME`                                |
| Aggregate function | `{func: SUM, args: [{ref: [X]}], as: total}`    | `AGGREGATE`                             |
| Arithmetic / xpr   | `{xpr: [{ref: [A]}, "+", {ref: [B]}], as: sum}` | `EXPRESSION`                            |
| Function           | `{func: UPPER, args: [{ref: [X]}], as: u}`      | `TRANSFORMATION`                        |
| Literal            | `{val: 5, as: const}`                           | No lineage emitted (no upstream column) |

Unresolvable refs (e.g., `SELECT *` star expansion, or columns referring to
unknown aliases) are skipped and logged to `report.column_lineage_unresolved`.

Lineage extraction adds one HTTP call per asset (the per-object-type CSN
fetch). At 1M assets with `max_workers_assets=10`, this adds ~3 hours to the
total run time. The column-level walker itself is essentially free (in-memory
tree walk over an already-fetched JSON document).

#### View definitions

With `include_view_definitions: true` (default), a View's or Analytic Model's
definition is emitted as the `viewProperties` aspect — `viewLanguage="SQL"` for
SQL-editor views (the raw SQL the modeler wrote) and `viewLanguage="CSN"` for
graphical/modeled views (the CSN/CQN query tree).

#### Analytic Models

Assets with `supportsAnalyticalQueries: true` are cataloged as the
`Analytic Model` subtype. Column schema for exposed assets is read from the
OData EDMX relational-metadata URL.

#### Tags (SAP CDS semantic annotations)

With `emit_sap_semantics_as_tags: true` (default), the connector emits DataHub
tags for the SAP CDS semantic annotations it parses from EDMX, so they're
searchable and filterable in DataHub Search and render as colored badges in the
UI:

| Tag URN                                                            | Source annotation          | Where attached                   |
| ------------------------------------------------------------------ | -------------------------- | -------------------------------- |
| `urn:li:tag:Dimension`                                             | `@Analytics.Dimension`     | dataset (entity-level) or column |
| `urn:li:tag:Measure`                                               | `@Analytics.Measure`       | dataset (entity-level) or column |
| `urn:li:tag:sap:semantic:currency`                                 | `@Common.IsCurrency`       | column                           |
| `urn:li:tag:sap:semantic:unit`                                     | `@Common.IsUnit`           | column                           |
| `urn:li:tag:sap:calendar:{year,month,quarter,week,date,yearmonth}` | `@Common.IsCalendar*`      | column                           |
| `urn:li:tag:sap:dimension_type:{Time,Customer,...}`                | `@Analytics.DimensionType` | dataset                          |

`Dimension` and `Measure` URNs are flat (no namespace) so they collide with the
equivalent tags from other DataHub connectors (Looker, Snowflake semantic views,
Mode, ThoughtSpot, ...). Customers get cross-connector pivot on a single
"Measure" tag. SAP-specific concepts are namespaced under `sap:` to avoid
collision.

The existing `sap_*` custom properties (`sap_is_dimension`, `sap_calendar_type`,
`sap_semantic`, `sap_dimension_type`, ...) continue to be emitted on every
asset for backward compat — tag emission is purely additive. The same
annotations also appear in field descriptions with `[sap_calendar_type=date]`-style
suffixes. Set `emit_sap_semantics_as_tags: false` to suppress tag emission entirely.

#### API support

All endpoints called by this connector are SAP-supported public APIs:

- `/api/v1/datasphere/consumption/catalog/...` — asset discovery (Catalog API)
- `/api/v1/datasphere/consumption/relational/.../$metadata` — EDMX schema
- `/api/v1/datasphere/spaces/X/connections` — Connections API
- `/dwaas-core/api/v1/spaces/X/{views,analyticmodels,localtables}/{name}` —
  per-object-type CSN read (with `Accept: application/vnd.sap.datasphere.object.content+json`),
  the same surface the official `datasphere` CLI uses

Previous releases of this connector used the `/deepsea/repository/` endpoint
for lineage extraction, which SAP marks as internal-use-only in
[KBA #3517441](https://userapps.support.sap.com/sap/support/knowledge/en/3517441).
That dependency was removed in this release — `include_lineage: true` is now
a fully supported feature.

The catalog service caps a page at **500 records** (both the default and the
documented maximum). The connector paginates by following the server's
`@odata.nextLink` cursor (falling back to advancing its offset by the number of
records actually returned), so spaces with more than 500 exposed assets are
fully captured.

#### Performance and scale

The dominant cost is **HTTP calls, not memory**. For each exposed asset the
connector makes roughly **2 HTTP calls** — one CSN definition fetch (per-object
type endpoint) plus one EDMX `$metadata` schema fetch — on top of paginated
catalog listing. That works out to **~2N HTTP calls for N assets** (e.g.
~2,000,000 calls for 1M datasets), so expect long runtimes at very large scale.

Levers to reduce cost and scope:

- **`space_pattern` / `asset_pattern`** — filtering is applied **before** the
  per-asset HTTP calls, so filtered-out assets are nearly free. Scope the run to
  the spaces / assets you actually need.
- **`expose_for_consumption_only: true`** — skip assets that have no consumption
  exposure URL. By default the connector catalogs all assets in each Space
  regardless of whether they are exposed for OData consumption.
- **`include_view_definitions: false` AND `include_lineage: false`** — together
  these skip the per-asset CSN fetch, roughly **halving** the HTTP calls for
  users who only need catalog + schema (no lineage / view definitions).
- **`max_workers_assets`** — default 10; raise toward 64 if the tenant tolerates
  the request rate. EDMX schema and CSN lineage are fetched per-asset (the SAP
  Datasphere API has no batch metadata endpoint), so this is the main parallelism
  lever for large catalogs.

**Memory is bounded** regardless of catalog size:

- Workunits stream out with backpressure (the connector never buffers the whole
  run).
- The ingestion report uses capped `LossyList`s, so per-run diagnostics don't
  grow unbounded.
- Each space's assets are processed in batches of `asset_batch_size` (default 5000) before being submitted to the parallel executor, so peak memory does not
  grow with the number of assets in a single space.

### Limitations

#### Catalog business-enrichment metadata is not extracted

SAP Datasphere's **Catalog & Marketplace** application surfaces governance
metadata that this connector does **not** ingest:

- **Responsible Team**, **Business Contact Person**, **Purpose**
- User-defined **Tags** and related **Glossary Terms** / **KPIs**
- **Published By** / created-by / published-on (user-level ownership & audit)

These fields are **not exposed by any public, OAuth-accessible SAP API**. They
are served only by the Catalog's internal UI backend under `/deepsea/catalog/`
and `/deepsea/repository/`, which:

1. **Rejects OAuth.** Called with an XSUAA Bearer token (the connector's auth),
   `/deepsea/...` returns an HTML login page, not JSON — it only accepts
   interactive browser **session cookies**.
2. **Is an internal UI endpoint**, not a supported REST API (its OData service
   names are explicitly marked `...private...`), so it may change without notice.

The supported public surface the connector uses
(`/api/v1/datasphere/consumption/catalog/...`) returns an asset's technical name,
business name (label), description, space, consumption URLs, and capability flags
— but none of the governance-enrichment fields above. There is therefore no
supported path to ingest them today. Support will be added if and when SAP
publishes a public Catalog metadata API.

Note: the connector **does** extract the SAP CDS semantic annotations that _are_
available via the supported EDMX/CSN surface (Dimension/Measure, currency/unit,
calendar, dimension type) — see **Capabilities → Tags**. The gap is
specifically the Catalog-app governance fields, not all business metadata.

#### Schema and type limitations

These follow from what the supported consumption surface exposes:

- **Geospatial and binary types are reported as strings.** Per SAP's "OData
  Annotation Limitations", the consumption `$metadata` (EDMX) overwrites several
  CDS types to `Edm.String`: `cds.hana.ST_GEOMETRY`, `cds.hana.ST_POINT`,
  `cds.Binary`, `cds.LargeBinary`, `cds.hana.BINARY`, `cds.LargeString`,
  `cds.UUID`. Because the connector reads column types from this EDMX surface,
  such columns appear as string types in DataHub — the original geo/binary/UUID
  type cannot be recovered from the consumption API.
- **View input parameters are not enumerated.** The connector records whether an
  asset has parameters (the `sap_*` flag derived from the catalog's
  `hasParameters`) and surfaces Analytic Model **variables** (as the
  `sap_variables` custom property), but it does not yet emit the individual
  **input-parameter** definitions of a view as schema/metadata.

#### Lineage limitations

- Scalar subqueries in column expressions ARE supported (resolved against the
  subquery's own FROM clause). Correlated subqueries that reference outer-scope
  aliases are NOT — their refs land in `report.column_lineage_unresolved`.
- Cross-Space upstream resolution: column lineage assumes the upstream lives
  in the same Space and resolves to the same storage platform as the
  downstream. Cross-Space references will appear with the correct upstream
  qualified-name but routed to the downstream's platform.
- `SELECT *` star expansion is not unrolled — those columns are listed in
  `report.column_lineage_unresolved`.
- A `SELECT FROM T AS t` with unqualified column refs (e.g. `{ref: [X]}`) does
  NOT auto-resolve through the alias; users either need to qualify the ref
  (`t.X`) or remove the explicit `AS` alias.

#### Stateful ingestion with large catalogs

The connector uses DataHub's standard `StaleEntityRemovalHandler` for soft-delete
detection. This handler keeps a checkpoint of every emitted URN in a single MCP
that is sent to GMS at the end of each run.

At default GMS / Kafka payload limits (1 MB Kafka message, 16 MB GMS REST), the
checkpoint can hold approximately **80,000 dataset URNs** before exceeding
limits. If your Datasphere catalog has more than ~50,000 datasets we recommend:

- Disable `stateful_ingestion` (set `enabled: false`) and use manual soft-delete
  cleanup via the DataHub UI / CLI.
- Or, partition the ingestion by `space_pattern` so each run stays under the
  ceiling.

The connector emits a `report.warning` when emitted entity count crosses 50,000
during a single run (via `_check_scale_warning`).

### Troubleshooting

#### Fewer objects than the Data Builder shows

This is the **#1 reason DataHub captures fewer objects than the Data Builder
shows**: a human browsing the Data Builder sees spaces they personally belong
to, while the ingestion principal only sees the spaces _it_ belongs to. If a
space is missing from DataHub, check the `report` warnings for the
_"Not a member of SAP Datasphere space"_ message and add the principal to that
space (see **Prerequisites → Space membership**).

#### HTTP 429 throttling

SAP Datasphere rate-limits the OData/consumption APIs to **~300 requests per
user per minute** (per the OData API documentation); exceeding it returns HTTP
429 with a `Retry-After` header. The connector's HTTP layer already retries 429
with exponential backoff (honoring `Retry-After`, bounded by `max_retries`), but
setting `max_workers_assets` too high can sustain throttling and slow the run —
tune it down if you see repeated 429 retries in the logs.

#### Local testing with a mock Datasphere (no tenant required)

You can exercise the full connector path — including the OAuth handshake —
without a real SAP tenant, using the bundled standalone mock server. It serves
**real recorded tenant responses** (catalog spaces/assets/connections,
per-view CSN, EDMX, local tables) plus a **permissive `/oauth/token`** endpoint,
so the connector's OAuth cold-start, data fetch, and lineage all run end-to-end.

Start the mock server (it prints a ready-to-run recipe and the command to run):

```bash
# Push into a running DataHub:
python tests/integration/sap_datasphere/mock_datasphere_server.py --sink http://localhost:8080

# Or omit --sink for a file sink (writes to /tmp/sap_datasphere_mock_out.json):
python tests/integration/sap_datasphere/mock_datasphere_server.py
```

Then, in another terminal, run ingestion against the generated recipe:

```bash
datahub ingest -c /tmp/sap_datasphere_mock_recipe.yml
```

> `mock_datasphere_server.py` needs `pytest-httpserver` (a test dependency). If
> it isn't importable, install it with `pip install pytest-httpserver` or
> install this package's test extra.

**UI-ingestion caveat:** the DataHub UI's ingestion executor runs in a
container, so `localhost` / `127.0.0.1` in the recipe won't reach a mock server
running on the host. Bind the server to all interfaces with `--host 0.0.0.0`
and point the recipe's `base_url` / `xsuaa_url` at `host.docker.internal:18000`
(Docker Desktop) or the host's LAN IP instead.
