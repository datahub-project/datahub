# SAP Datasphere connector — developer guide

Maintainer-facing notes on what this connector extracts, how it works
internally, and how its integration tests exercise it without a live tenant.
For user/recipe documentation see `metadata-ingestion/docs/sources/sap-datasphere/`.

---

## What it extracts

| SAP Datasphere object                | DataHub entity                                             | Notes                                                              |
| ------------------------------------ | ---------------------------------------------------------- | ------------------------------------------------------------------ |
| **Space**                            | Container, subtype `Space`                                 | 2-tier model (Space → dataset); no synthetic folder layer          |
| **View** (graphical / SQL)           | Dataset on `sap-datasphere`, subtype `View`                | `viewProperties` carries the CSN query tree                        |
| **Analytic Model**                   | Dataset, subtype `Analytic Model`                          | star-schema lineage + measure/dimension tags + variables           |
| **Local Table** (base table)         | Dataset stub, subtype `Local Table`                        | opt-in via `include_local_tables`                                  |
| **Remote Table** (federation)        | Dataset stub, subtype `Remote Table` + external upstream   | opt-in via `include_remote_tables`; upstream = external source URN |
| **Data Flow**                        | `DataJob` (subtype `Data Flow`) under per-space `DataFlow` | opt-in via `include_data_flows`; IO datasets + column mappings     |
| **Replication Flow**                 | `DataJob` (subtype `Replication Flow`)                     | opt-in via `include_replication_flows`; source↔target + columns   |
| **Transformation Flow / Task Chain** | `DataJob` (experimental)                                   | opt-in; experimental — see Flows below                             |
| **Columns**                          | `SchemaField`                                              | from OData EDMX `$metadata`, decorated with CDS semantic tags      |
| **Lineage**                          | `UpstreamLineage` + `FineGrainedLineage`                   | table-level + column-level (see Lineage below)                     |

Also emitted: CDS semantic annotations as DataHub **tags** (Dimension/Measure,
currency, unit, calendar, dimension-type), `sap_*` custom properties, and
stateful soft-delete of entities that disappear between runs.

**Not extracted** — the Catalog & Marketplace governance fields (Responsible
Team, Purpose, Business Contact Person, user tags, ownership). These have no
public/OAuth-accessible SAP API; they live only behind the internal,
cookie-authenticated `/deepsea/` UI backend. See the limitations section of the
user docs for the full rationale.

---

## How it works

```
config.py ──► client.py ──► source.py ──► lineage.py / *_parser.py ──► workunits
 (auth +      (HTTP/OAuth    (orchestrate    (CSN walk, EDMX/CSN parse,
  filters)     pagination)    per-asset emit)  business-layer parse)
```

| File                               | Responsibility                                                                                                                                                                                                               |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `config.py`                        | `SapDatasphereConfig` (Pydantic). Auth fields (`SecretStr`), `AllowDenyPattern` filters, federation maps, numeric bounds. Validators derive `xsuaa_url`, enforce credential completeness, validate `base_url` scheme.        |
| `client.py`                        | HTTP client over a shared `requests.Session` + retry adapter (429/5xx). OAuth token acquisition, catalog pagination (`@odata.nextLink`), CSN + EDMX fetch. `EdmxFetchResult`/`EdmxFetchReason` distinguish 403/404/OK/error. |
| `platform_mapping.py`              | Resolves each asset's connection to a concrete platform/instance/env (`ResolvedPlatform`) or a tagged `ResolveSkipReason`.                                                                                                   |
| `source.py`                        | Orchestration: per-space → per-asset emit, lineage-aspect assembly, business-layer application, SDK V2 emission, report.                                                                                                     |
| `lineage.py`                       | CSN `query.SELECT` walker → table-level + column-level (`FineGrainedLineage`).                                                                                                                                               |
| `analytic_model.py`                | Parses `businessLayerDefinitions` → fact/dimension star-schema lineage + measures/attributes/variables.                                                                                                                      |
| `csn_parser.py` / `edmx_parser.py` | CSN element → schema fields; EDMX `$metadata` → fields + SAP semantic annotations.                                                                                                                                           |
| `report.py` / `tags.py`            | Custom report (counters, LossyLists, bounded API-timing heap); tag URN/definition constants.                                                                                                                                 |

### Authentication

Three methods, priority-ordered (`config.py` validators enforce completeness):

1. **Raw bearer `token`** — dev/testing.
2. **OAuth `refresh_token`** (+ `client_id`, `xsuaa_url`) — authorization-code flow.
3. **XSUAA `client_id` + `client_secret`** — client-credentials (recommended for prod).

The shared session retries 429/5xx with backoff. On a 401 mid-run the client
refreshes the token once, then fails with an actionable credentials error.

### Discovery & fetch

Uses only SAP-supported public endpoints:

- `/api/v1/datasphere/consumption/catalog/{spaces,assets}` — inventory (paginated, 500/page cap, `@odata.nextLink`).
- `/api/v1/datasphere/consumption/relational/.../$metadata` — EDMX schema.
- `/dwaas-core/api/v1/spaces/{space}/{views,analyticmodels,localtables,remotetables}/{name}` — per-object CSN (the surface the official `datasphere` CLI uses).
- `/dwaas-core/api/v1/spaces/{space}/{dataflows,replicationflows,transformationflows,taskchains}/{name}` — per-flow design-time definitions (opt-in).
- `/api/v1/datasphere/spaces/{space}/connections` — connection → platform routing.

### Platform model (managed vs. federated)

Managed objects (Views, Analytic Models, Local Tables) emit on the dedicated
`sap-datasphere` platform. Federated Remote Tables emit on their **real storage
platform** so a Datasphere view's upstream resolves to the same URN the native
Snowflake/HANA/… connector emits — no Siblings config. Routing is driven by
`connection_to_platform_map` (per connection) and `platform_type_defaults`
(per typeId).

### Lineage

`include_lineage: true` walks the CSN query tree:

- **Table-level** — `query.SELECT.from` (direct refs, joins, subqueries) + `@remote.source`.
- **Column-level** — one `FineGrainedLineage` per resolvable downstream column (direct / alias=RENAME / aggregate=AGGREGATE / arithmetic=EXPRESSION / func=TRANSFORMATION).
- **Analytic-model star schema** — fact/dimension sources from `businessLayerDefinitions`.
  Unresolvable refs are counted in `report.column_lineage_unresolved`, never silently dropped.

### Flows & Remote Tables (opt-in ETL lineage)

Beyond the query-derived lineage above, the connector can discover the
design-time objects that _populate_ tables, closing the gap where a Local Table
appears only as a phantom upstream (`flows.py`):

- **Data Flows** (`include_data_flows`) and **Replication Flows**
  (`include_replication_flows`) are emitted as `DataJob`s grouped under one
  per-space `DataFlow`. Each job's `*.consumer`/`*.producer` processes (data
  flows) or `sourceObject`/`targetObject` tasks (replication flows) become
  inlets/outlets; bare-quoted `attributeMappings` become `FineGrainedLineage`.
  Column lineage is only attributed when a producer has a single input source
  (multi-input flows stay table-level to avoid guessing).
- **Remote Tables** (`include_remote_tables`) are emitted as `sap-datasphere`
  dataset stubs (matching the name views reference them by) with an
  `UpstreamLineage` edge to the external source object parsed from the CSN
  `@DataWarehouse.remote.connection` / `@DataWarehouse.remote.entity`
  annotations, routed to a platform via the connection maps.
- **Transformation Flows / Task Chains** (`include_transformation_flows`,
  `include_task_chains`) are **experimental**: no live payload was available to
  verify their grammar. Transformation flows reuse the data-flow process-graph
  reader; task chains are surfaced as IO-less jobs (presence + subtype only).
  Enable only to catalogue the objects, and report payload samples so lineage
  can be hardened.

Flow-endpoint resolution reuses the connection maps via
`PlatformMappingResolver.resolve_external`, which also accepts an endpoint's own
`connectionType` so a replication-flow system that isn't in the space's
connections list still routes. Endpoints/sources that don't resolve are counted
in `report.flow_endpoints_unresolved` / `report.remote_table_source_unresolved`.

### Scale & safety

Workunits stream as generators; per-space assets are processed in
`asset_batch_size` chunks across `max_workers_assets` threads, so peak memory is
bounded regardless of catalog size. Per-asset/per-space errors warn-and-continue
(layered isolation) rather than aborting the run. Diagnostics use capped
`LossyList`s and a bounded slowest-API-call heap.

---

## How the integration tests work (no live tenant)

CI never touches a real tenant. SAP Datasphere is simulated three increasingly
faithful ways, each asserting against a golden file
(`mce_helpers.check_golden_file`):

| Test file                                | Mechanism                                          | What it uniquely proves                                                                                                                                                                                                         |
| ---------------------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `test_sap_datasphere.py`                 | `requests_mock` (intercepts `requests`)            | End-to-end golden; federated lineage + FGL; determinism (run twice, incl. `max_workers=4`); stateful soft-delete (run twice with a dropped asset).                                                                              |
| `test_sap_datasphere_mock_service.py`    | `pytest_httpserver` (real in-process socket)       | Real HTTP stack: OAuth **401 → refresh → retry**, column-lineage over the wire.                                                                                                                                                 |
| `test_sap_datasphere_recorded_replay.py` | `pytest_httpserver` + **recorded tenant fixtures** | Highest fidelity: serves real captured tenant payloads and forces a full cold-start `client_credentials` OAuth grant; asserts the first token call is `grant_type=client_credentials` and the bearer is attached to data calls. |

Recorded fixtures live in `tests/integration/sap_datasphere/fixtures/recorded/`
(catalog spaces/assets/connections, per-view CSN, EDMX, local tables — host
scrubbed to `https://RECORDED_TENANT`). Golden files: 58 KB/78 events,
39 KB/56 events (with lineage + FGL), 106 KB/94 events (recorded-replay) — all
deterministic.

Regenerate goldens with:

```bash
pytest tests/integration/sap_datasphere/ --update-golden-files
```

### Running ingestion against a mock (no tenant)

`tests/integration/sap_datasphere/mock_datasphere_server.py` reuses the same
recorded fixtures + handlers as the replay test, but as a standalone server, so
you can drive real CLI/UI ingestion:

```bash
# serve recorded data + a permissive /oauth/token, print a ready recipe:
python tests/integration/sap_datasphere/mock_datasphere_server.py --sink http://localhost:8080
# then, in another shell:
datahub ingest -c /tmp/sap_datasphere_mock_recipe.yml
```

Needs the `pytest-httpserver` test dependency. For UI ingestion (executor runs
in a container) bind `--host 0.0.0.0` and point the recipe at
`host.docker.internal` instead of `localhost`.

```

```
