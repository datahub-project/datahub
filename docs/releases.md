
# DataHub Releases

## Summary

| Version | Release Date | Links |
| ------- | ------------ | ----- |
| **v1.7.0** | 2026-08-04 |[Release Notes](#v1-7-0), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.7.0) |
| **v1.6.0** | 2026-05-21 |[Release Notes](#v1-6-0), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.6.0) |
| **v1.5.0.7** | 2026-05-19 |[Release Notes](#v1-5-0-7), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.7) |
| **v1.5.0.6** | 2026-05-11 |[Release Notes](#v1-5-0-6), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.6) |
| **v1.5.0.5** | 2026-05-07 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.5) |
| **v1.5.0.4** | 2026-05-06 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.4) |
| **v1.5.0.3** | 2026-04-25 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.3) |
| **v1.5.0.2** | 2026-04-13 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.2) |
| **v1.5.0.1** | 2026-03-25 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.1) |
| **v1.5.0** | 2026-03-24 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0) |


## [v1.7.0](https://github.com/datahub-project/datahub/releases/tag/v1.7.0) {#v1-7-0}

Released on 2026-08-04 by [@david-leifker](https://github.com/david-leifker).

### DataHub v1.7.0

**Requirements**

- CLI / Python SDK: **1.7.0**
- Helm Chart: **1.1.0**

Full upgrade guidance, including every breaking change and migration step: [Updating DataHub — v1.7.0](https://github.com/datahub-project/datahub/blob/v1.7.0/docs/how/updating-datahub.md#v170).

**Upgrade path / ZDU:** You **must upgrade to [v1.6.0](https://github.com/datahub-project/datahub/blob/v1.7.0/docs/how/updating-datahub.md#v160) before upgrading to v1.7.0** — do not skip 1.6.0. Deploy v1.6.0 with Helm chart **1.0.3**, let system-update complete, then upgrade to v1.7.0 with Helm chart **1.1.0**. Enable Elasticsearch/OpenSearch ZDU (`global.datahub.systemUpdate.zdu`) with the **1.1.0** chart on a subsequent OpenSearch/Elasticsearch version bump — not during the v1.6.0 install.

---

#### Feature highlights

##### UI and experience

- **Metrics and Semantic Models** — first-class `metric` and `semanticModel` entities with dedicated pages, a metrics home/sidebar experience, autocomplete, modular summary tabs, and lineage wiring ([#18134](https://github.com/datahub-project/datahub/pull/18134), [#18350](https://github.com/datahub-project/datahub/pull/18350)–[#18407](https://github.com/datahub-project/datahub/pull/18407), [#18442](https://github.com/datahub-project/datahub/pull/18442)–[#18459](https://github.com/datahub-project/datahub/pull/18459), [#18462](https://github.com/datahub-project/datahub/pull/18462), [#18482](https://github.com/datahub-project/datahub/pull/18482), [#18701](https://github.com/datahub-project/datahub/pull/18701), and related).
- **Logical Models UI** — create, link, edit, and delete logical models from the UI, in addition to API/SDK paths ([#18498](https://github.com/datahub-project/datahub/pull/18498)).
- **Data Product lineage** — data products participate directly in the lineage graph ([#18463](https://github.com/datahub-project/datahub/pull/18463)).
- **Multi-language (i18n) default on** — `I18N_ENABLED` defaults to on in OSS; UI follows the browser locale. New Beta locales include French, Italian, Norwegian Bokmål, Swedish, Hungarian, and Finnish ([#18285](https://github.com/datahub-project/datahub/pull/18285), [#18282](https://github.com/datahub-project/datahub/pull/18282), [#18221](https://github.com/datahub-project/datahub/pull/18221), [#18222](https://github.com/datahub-project/datahub/pull/18222), [#18263](https://github.com/datahub-project/datahub/pull/18263), [#18265](https://github.com/datahub-project/datahub/pull/18265), [#18339](https://github.com/datahub-project/datahub/pull/18339), [#18520](https://github.com/datahub-project/datahub/pull/18520)).
- **Lineage graph** — always the latest lineage experience; `LINEAGE_GRAPH_V2` / `LINEAGE_GRAPH_V3` flags removed.

##### Ingestion — new sources

- **Cube** — semantic-layer connector ([#17964](https://github.com/datahub-project/datahub/pull/17964))
- **AWS Kinesis** — Kinesis Data Streams and Amazon Data Firehose ([#17592](https://github.com/datahub-project/datahub/pull/17592))
- **MicroStrategy** — BI connector ([#18158](https://github.com/datahub-project/datahub/pull/18158))
- **Open Data Contract Standard (ODCS)** — contracts from S3, GCS, HTTP, and Git ([#17331](https://github.com/datahub-project/datahub/pull/17331), [#18474](https://github.com/datahub-project/datahub/pull/18474), [#18477](https://github.com/datahub-project/datahub/pull/18477))
- **ThoughtSpot** ([#17400](https://github.com/datahub-project/datahub/pull/17400))
- **SAP Datasphere** ([#17802](https://github.com/datahub-project/datahub/pull/17802))
- **DocumentDB platform** — opt-in `platform: documentdb` on the MongoDB source for AWS DocumentDB ([#17443](https://github.com/datahub-project/datahub/pull/17443))

##### Ingestion — major connector improvements

- **Hex** — major in-place upgrade: table- and column-level lineage from Hex APIs, Project → Component links, run history, optional AI context documents; Components ingested as **Chart** entities (see breaking changes) ([#17376](https://github.com/datahub-project/datahub/pull/17376)).
- **Snowflake** — Semantic Views can emit first-class `semanticModel` / `metric` / logical-dataset entities (`semantic_views.emit_semantic_model_entities`; OSS default off / auto-resolve) ([#18395](https://github.com/datahub-project/datahub/pull/18395), [#18509](https://github.com/datahub-project/datahub/pull/18509)).
- **Databricks Unity Catalog** — Lakehouse Federation (foreign catalogs); usage/ops/queries from `system.query.history` via the shared SQL parsing aggregator; ML model ingestion controls fixed ([#18213](https://github.com/datahub-project/datahub/pull/18213), [#17971](https://github.com/datahub-project/datahub/pull/17971), [#18220](https://github.com/datahub-project/datahub/pull/18220), and related).
- **Matillion** — foldered container hierarchy, environment-scoped lineage, run history at pipeline and component levels, corrected console links ([#17927](https://github.com/datahub-project/datahub/pull/17927)).
- **Glue** — Lake Formation resource-link schema resolution (default on), column-level LF tags, cross-account platform instances ([#17963](https://github.com/datahub-project/datahub/pull/17963), [#17812](https://github.com/datahub-project/datahub/pull/17812)).
- **Redshift** — multi-line SQL no longer dropped from lineage/usage; per-query popularity stats; `table_pattern` applied to SQL-parsing path ([#18542](https://github.com/datahub-project/datahub/pull/18542), [#18001](https://github.com/datahub-project/datahub/pull/18001), [#18065](https://github.com/datahub-project/datahub/pull/18065)).
- **BigQuery** — table stats from `INFORMATION_SCHEMA.PARTITIONS` (see breaking changes); usage window fields consolidated to top-level ([#18367](https://github.com/datahub-project/datahub/pull/18367), [#18133](https://github.com/datahub-project/datahub/pull/18133)).
- **Power BI / Mode** — column-level lineage preserves original upstream column casing ([#18181](https://github.com/datahub-project/datahub/pull/18181)).
- **Spark** — Apache Spark **4.x** support (Scala 2.13 agent); OpenLineage **1.50** with full shading for EMR/DataZone coexistence ([#14911](https://github.com/datahub-project/datahub/pull/14911)).
- **S3 / ABS** — profile data-lake files without PySpark; optional `emit_folders_only` for object-store folder cataloging ([#18347](https://github.com/datahub-project/datahub/pull/18347), [#18599](https://github.com/datahub-project/datahub/pull/18599), [#18437](https://github.com/datahub-project/datahub/pull/18437)).
- **Airbyte** — Public API stream namespace recovery ([#18727](https://github.com/datahub-project/datahub/pull/18727)).
- **Kafka** — profiling support ([#14367](https://github.com/datahub-project/datahub/pull/14367)).
- **Great Expectations** — GX Core 1.x action path ([#18706](https://github.com/datahub-project/datahub/pull/18706)).

##### Search, auth, and metadata

- **View authorization overhaul** — entity types restricted by default when VBAC is on; `VIEW_UNRESTRICTED_*` overlays; documents view-restricted by default; schemaField can inherit VIEW from parent dataset ([#18612](https://github.com/datahub-project/datahub/pull/18612), [#18664](https://github.com/datahub-project/datahub/pull/18664), and related).
- **Structured properties** — ES field-name collision rejection; keyword max-length validation; type-mismatch reindex detection.
- **Configurable search entity-type defaults** — `SEARCH_*_ENTITY_TYPES` env overlays for GraphQL search/autocomplete/browse defaults.
- **File upload / object storage** — config path moved to `datahub.objectStorage` (see breaking changes); documentation file attach/download guide.

##### Operations and platform

- **Secrets caller guard** — `SECRET_SERVICE_CALLER_GUARD_MODE` defaults to **ENFORCE**; human PATs can no longer decrypt UI secrets via GraphQL.
- **Primary storage read pool** — optional Ebean/Cassandra read pool for entity-aspect reads (`EBEAN_READ_POOL_*` / `CASSANDRA_READ_POOL_*`).
- **Ebean transaction conflicts** — stable retryable **503** / `DATABASE_TRANSACTION_CONFLICT` instead of opaque 500s on deadlock exhaustion.
- **Docker tags** — floating `:head` removed; coordinated `:quickstart` and immutable `:sha-*` tags.
- **Optional Loki log shipping** — `LOG_AGGREGATOR_ENDPOINT` for core services and the frontend.
- **jose4j** shipped for Kafka SASL/OAUTHBEARER JWT validation.
- **Airflow plugin** — Airflow **2.x dropped**; Airflow **3.0+** required. Prefect plugin requires Prefect **3.x**.
- **Orchestration plugins** — Airflow / Dagster / Prefect / GX default emit mode is **ASYNC**.
- **Built-in column classifier removed** — `DataHubClassifier` / `acryl-datahub-classify` no longer shipped.

---

#### Breaking changes

Review the full [Breaking Changes](https://github.com/datahub-project/datahub/blob/v1.7.0/docs/how/updating-datahub.md#v170) section in [Updating DataHub](https://github.com/datahub-project/datahub/blob/v1.7.0/docs/how/updating-datahub.md#v170) before upgrading. Summary of items that may require action:

| Area | What changed | Who is affected |
| --- | --- | --- |
| **Must install v1.6.0 first** | Do not skip 1.6.0; ZDU enablement uses Helm **1.1.0** after 1.6.0 + system-update | All upgraders from pre-1.6.0 |
| **Secrets ENFORCE** | Human PATs/browser can no longer decrypt UI secrets; use datahub-actions / system client or `AUDIT` temporarily | Anyone using user PATs for `getSecretValues` |
| **Airflow 2 dropped** | Plugin requires Airflow 3.0+ | Airflow 2.x deployments — pin plugin `&lt;= 1.6.0` or upgrade Airflow |
| **Prefect 3 required** | `datahub-prefect` requires Prefect 3.x | Prefect 2.x users |
| **Classifier removed** | Built-in `DataHubClassifier` gone; recipes with `classification.enabled: true` fail fast | Classification-enabled recipes |
| **Plugin emit ASYNC** | Airflow/Dagster/Prefect/GX default emit is async | Operators needing sync/raise-on-reject — set `SYNC_PRIMARY` |
| **Hex Components → Chart** | Component entity type/URNs change; tags/policies may need reapply | Hex workspaces with Components |
| **Spark OL 1.50 trimmers** | Partition dirs stripped from FS/object-store dataset names by default | Spark lineage without `path_spec_list` / `file_partition_regexp` |
| **Power BI / Mode CLL casing** | Upstream column paths keep source casing | Re-ingest; remove lowercase workarounds on SQL sources |
| **BigQuery table stats** | Stats from `PARTITIONS`; empty/external/views/snapshots lose some timestamps | Set `use_legacy_table_stats: true` to restore |
| **Object storage YAML** | `datahub.s3` → `datahub.objectStorage` | Custom YAML overrides (env vars largely unchanged) |
| **View authorization** | Restricted-by-default + `VIEW_UNRESTRICTED_*`; documents restricted | Deployments with `VIEW_AUTHORIZATION_ENABLED=true` |
| **Lineage graph flags** | `LINEAGE_GRAPH_V2` / `V3` removed | Anyone still setting those env vars |
| **Docker `:head`** | Use `quickstart` / immutable `sha-*` / release tags | Compose and production pin practices |
| **Workunit processors** | Helper functions → processor classes; several renames | Custom ingestion code calling old helpers |
| **Relationship edge uniqueness** | One aspect per `(source, dest, relationship)` signature ([#18845](https://github.com/datahub-project/datahub/pull/18845)) | Custom / plugin entity registries |
| **Logical parent auth** | Edit Entity required on child **and** parent when linking | Logical-model operators |

**Potential downtime:** Structured-property Elasticsearch type-mismatch reindex when both system-update flags are on — see [Updating DataHub — v1.7.0](https://github.com/datahub-project/datahub/blob/v1.7.0/docs/how/updating-datahub.md#v170).

**Deprecations:** Hex lineage time/page-size recipe fields; BigQuery `usage.*` window / formatting fields migrated to top-level — see the v1.7.0 Deprecations section in [Updating DataHub](https://github.com/datahub-project/datahub/blob/v1.7.0/docs/how/updating-datahub.md#v170).

---

#### Contributors

Thank you to everyone who contributed to v1.7.0. For the complete changelog, compare [v1.6.0...v1.7.0](https://github.com/datahub-project/datahub/compare/v1.6.0...v1.7.0).


## [v1.6.0](https://github.com/datahub-project/datahub/releases/tag/v1.6.0) {#v1-6-0}

Released on 2026-05-21 by [@david-leifker](https://github.com/david-leifker).

### DataHub v1.6.0

Full upgrade guidance, including every breaking change and migration step: [Updating DataHub — v1.6.0](https://github.com/datahub-project/datahub/blob/v1.6.0/docs/how/updating-datahub.md#v160).

---

#### Feature highlights

##### UI and experience

- **V2 UI only** — Legacy V1 UI code is removed; V2 is the sole interface with an updated collapsible navigation and home hero toggler ([#17468](https://github.com/datahub-project/datahub/pull/17468)).
- **Play 3 frontend** — `datahub-frontend` runs on Play 3 and Apache Pekko for improved security and maintainability ([#17214](https://github.com/datahub-project/datahub/pull/17214)).
- **Theming** — Broader migration to semantic color tokens across entity, lineage, home, and settings surfaces.
- **Security hardening** — Content-Security-Policy on the frontend ([#17277](https://github.com/datahub-project/datahub/pull/17277)), sanitized API error responses ([#17452](https://github.com/datahub-project/datahub/pull/17452)), URL validation before rendering links ([#17489](https://github.com/datahub-project/datahub/pull/17489)), and home page template/module scope checks ([#17487](https://github.com/datahub-project/datahub/pull/17487)).

##### Ingestion — new sources

- **Airbyte** ([#13217](https://github.com/datahub-project/datahub/pull/13217))
- **Aerospike** ([#11838](https://github.com/datahub-project/datahub/pull/11838))
- **Apache Flink** — metadata and lineage ([#16218](https://github.com/datahub-project/datahub/pull/16218))
- **dlt** (data load tool) ([#16426](https://github.com/datahub-project/datahub/pull/16426))
- **Matillion** ([#15966](https://github.com/datahub-project/datahub/pull/15966))
- **Microsoft Fabric Data Factory** ([#16646](https://github.com/datahub-project/datahub/pull/16646))
- **Omni** BI platform — INCUBATING ([#16564](https://github.com/datahub-project/datahub/pull/16564))
- **Pinecone** Vector DB ([#16472](https://github.com/datahub-project/datahub/pull/16472))
- **StarRocks** ([#15913](https://github.com/datahub-project/datahub/pull/15913))
- **Informatica Cloud (IDMC)** ([#17051](https://github.com/datahub-project/datahub/pull/17051))

##### Ingestion — major connector improvements

- **Sigma** — Data Models on by default, formula-resolved chart lineage, customSQL warehouse lineage, cross-DM fine-grained lineage, per-connection `connection_to_platform_map`, and workbook element-to-element edges ([#17276](https://github.com/datahub-project/datahub/pull/17276), [#17196](https://github.com/datahub-project/datahub/pull/17196), [#17296](https://github.com/datahub-project/datahub/pull/17296), [#17347](https://github.com/datahub-project/datahub/pull/17347), [#17369](https://github.com/datahub-project/datahub/pull/17369), [#17370](https://github.com/datahub-project/datahub/pull/17370), [#17086](https://github.com/datahub-project/datahub/pull/17086), and related).
- **Databricks Unity Catalog** — Opt-in Metric Views (`include_metric_views`), UPSERT ownership/properties by default ([#17380](https://github.com/datahub-project/datahub/pull/17380), [#16873](https://github.com/datahub-project/datahub/pull/16873)).
- **BigQuery** — Faster policy-tag extraction via `INFORMATION_SCHEMA`; richer external table metadata ([#17407](https://github.com/datahub-project/datahub/pull/17407), [#16348](https://github.com/datahub-project/datahub/pull/16348)).
- **Glue** — JDBC upstream lineage, Iceberg lineage, job subtype, structured properties on schema fields ([#16505](https://github.com/datahub-project/datahub/pull/16505), [#16562](https://github.com/datahub-project/datahub/pull/16562), [#16636](https://github.com/datahub-project/datahub/pull/16636), [#17325](https://github.com/datahub-project/datahub/pull/17325)).
- **Kafka Connect** — Column-level lineage for sink connectors ([#16515](https://github.com/datahub-project/datahub/pull/16515)).
- **Power BI** — `browsePathsV2` hierarchy, `Sql.Databases` M-Query support, workspace external URLs ([#16621](https://github.com/datahub-project/datahub/pull/16621), [#16616](https://github.com/datahub-project/datahub/pull/16616), [#16934](https://github.com/datahub-project/datahub/pull/16934)).
- **Fivetran** — Per-destination platform discovery in hybrid API + log mode ([#17217](https://github.com/datahub-project/datahub/pull/17217)).
- **Athena** — Correct upstream URNs for Glue- and Iceberg-backed tables ([#16842](https://github.com/datahub-project/datahub/pull/16842)).
- **Fabric OneLake** — View ingestion with column-level lineage; query usage from `queryinsights` ([#17215](https://github.com/datahub-project/datahub/pull/17215), [#17284](https://github.com/datahub-project/datahub/pull/17284)).
- **dbt** — Configurable URN lowercasing, stats from `catalog.json`, assertion `severity` and improved ERROR vs FAILURE mapping ([#16358](https://github.com/datahub-project/datahub/pull/16358), [#16044](https://github.com/datahub-project/datahub/pull/16044), assertion PRs).
- **SQL profiling** — SQLAlchemy profiler is the default for SQL connectors (faster, no Great Expectations dependency by default) ([#17465](https://github.com/datahub-project/datahub/pull/17465)).
- **Postgres** — Stored-procedure SQL bodies and lineage improvements ([#16871](https://github.com/datahub-project/datahub/pull/16871)).
- **Confluence** — Page HTML converted to Markdown ([#17475](https://github.com/datahub-project/datahub/pull/17475)).

##### Search, assertions, and metadata

- **Semantic search** — Elasticsearch 8.18+ semantic search; Vertex AI and local Ollama embedding providers ([#17230](https://github.com/datahub-project/datahub/pull/17230), [#17255](https://github.com/datahub-project/datahub/pull/17255), [#17201](https://github.com/datahub-project/datahub/pull/17201)).
- **Assertions** — Failure severity in APIs and UI; failure configuration SDK ([#17335](https://github.com/datahub-project/datahub/pull/17335), [#17355](https://github.com/datahub-project/datahub/pull/17355), [#17457](https://github.com/datahub-project/datahub/pull/17457)).
- **Structured properties** — Stricter GMS validation; CSV enricher support ([#16779](https://github.com/datahub-project/datahub/pull/16779)).
- **Search filters** — `Criterion` / `FacetFilterInput` use `values` arrays only (see breaking changes).
- **GraphQL** — Request bodies minified on the wire ([#17392](https://github.com/datahub-project/datahub/pull/17392)).

##### Operations and platform

- **Java 25 LTS** in official Docker images; Java 21 build toolchain ([#17340](https://github.com/datahub-project/datahub/pull/17340), [#16912](https://github.com/datahub-project/datahub/pull/16912)).
- **Spring Boot 4** on GMS and Java services ([#16816](https://github.com/datahub-project/datahub/pull/16816), [#17351](https://github.com/datahub-project/datahub/pull/17351)) — see breaking changes if you ship custom extensions.
- **Micrometer / Prometheus** — Actuator on port **4319** by default; JMX agent 1.0.1 with `/metrics` scrape path (see breaking changes).
- **Elasticsearch ZDU** — Optional zero-downtime side upgrade path via Helm ([#16887](https://github.com/datahub-project/datahub/pull/16887)).
- **Helm** — Cluster-wide `metricsMode`, `Cleanup` pre-delete hook, consolidated system-update path (deprecations in upgrade doc).
- **REST emitter** — Configurable connection pool ([#16486](https://github.com/datahub-project/datahub/pull/16486)).
- **Multi-entity** domain and ownership transformers ([#16798](https://github.com/datahub-project/datahub/pull/16798)).

---

#### Breaking changes

Review the [Breaking Changes](https://github.com/datahub-project/datahub/blob/v1.6.0/docs/how/updating-datahub.md#v160) section in [Updating DataHub](https://github.com/datahub-project/datahub/blob/v1.6.0/docs/how/updating-datahub.md#v160) before upgrading. Summary of items that may require action:

| Area                                 | What changed                                                                                                                               | Who is affected                                                                                                                                         |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Spring Boot 4**                    | GMS and Java services move to Spring Boot 4.0.5/4.0.6 (Spring Framework 7, Spring Kafka 4).                                                | **Custom GMS plugins or Spring extensions** — recompile and retest. Standard Docker/Helm installs: no change if you do not extend the server classpath. |
| **V1 UI removed**                    | V1 UI code deleted; V2 required (`THEME_V2_*` / Helm `theme_v2`).                                                                          | Anyone still on V1 env flags.                                                                                                                           |
| **Play 3 + `DATAHUB_SECRET`**        | Frontend on Play 3; secret must be ≥32 bytes or startup fails.                                                                             | Compose / hand-crafted short secrets (Helm usually OK).                                                                                                 |
| **SQL profiling default**            | Default profiler is `sqlalchemy`, not Great Expectations.                                                                                  | Recipes with `method: ge` need `acryl-datahub[profiling-ge]`.                                                                                           |
| **Search `value` → `values`**        | Singular `value` on filters removed; use `values` array only.                                                                              | Custom REST/GraphQL/SDK clients.                                                                                                                        |
| **Sigma**                            | Data models on by default; lineage URN/field behavior changes; `connection_to_platform_map` for Redshift.                                  | Sigma ingestion operators.                                                                                                                              |
| **Structured properties**            | Orphan assignments dropped by default (configurable).                                                                                      | Ingestion pipelines with stale property URNs.                                                                                                           |
| **Athena / Fivetran / two-tier SQL** | Upstream URN changes (Athena, Fivetran hybrid multi-destination); stored-procedure URN shape (MySQL, MariaDB, Hive, ClickHouse, Teradata). | Lineage keyed on old URNs.                                                                                                                              |
| **Unity Catalog**                    | Ownership/properties UPSERT by default.                                                                                                    | Manual owners merged via PATCH.                                                                                                                         |
| **BigQuery policy tags**             | New extraction path only; old path removed.                                                                                                | `extract_policy_tags_from_catalog: true`.                                                                                                               |
| **Micrometer / JMX**                 | Actuator on **4319**; JMX scrape at **`/metrics`**.                                                                                        | Prometheus/Grafana scrape configs.                                                                                                                      |
| **Actions / Kafka**                  | Default async offset commits (higher throughput; possible redelivery).                                                                     | Custom actions needing sync commits.                                                                                                                    |
| **Auth**                             | `corpUserInfo.active` ignored for sessions.                                                                                                | Login gating on deprecated `active`.                                                                                                                    |
| **Vertex AI**                        | Model version set URNs scoped per project.                                                                                                 | Orphaned version sets after upgrade.                                                                                                                    |
| **dbt assertions**                   | Infrastructure failures → `ERROR` not `FAILURE`; new `severity`.                                                                           | Dashboards filtering `FAILURE` only.                                                                                                                    |
| **Dataplex**                         | Renamed filter config fields.                                                                                                              | Recipes using old keys.                                                                                                                                 |
| **Docker build**                     | `BASE_IMAGE`, `apkRepositoryUrl` build args.                                                                                               | Custom image builds.                                                                                                                                    |

**Potential downtime:** Reindexing, optional Elasticsearch ZDU, first system-update after bootstrap moves, and aspect schema version sweep on large catalogs — documented under v1.6.0 in [Updating DataHub](https://github.com/datahub-project/datahub/blob/v1.6.0/docs/how/updating-datahub.md#v160).

**Deprecations:** Helm per-workload monitoring → `global.datahub.monitoring`, consolidated system-update, Great Expectations profiler legacy, Glossary Term AI automation — see the v1.6.0 Deprecations section in [Updating DataHub](https://github.com/datahub-project/datahub/blob/v1.6.0/docs/how/updating-datahub.md#v160).

---

#### Contributors

Thank you to everyone who contributed to v1.6.0. For the complete changelog, compare [v1.5.0.7...v1.6.0](https://github.com/datahub-project/datahub/compare/v1.5.0.7...v1.6.0).

## [v1.5.0.7](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.7) {#v1-5-0-7}

Released on 2026-05-19 by [@david-leifker](https://github.com/david-leifker).

Security Update

**Full Changelog**: https://github.com/datahub-project/datahub/compare/v1.5.0.6...v1.5.0.7

## [v1.5.0.6](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.6) {#v1-5-0-6}

Released on 2026-05-11 by [@david-leifker](https://github.com/david-leifker).

Remove kubectl binary

**Full Changelog**: https://github.com/datahub-project/datahub/compare/v1.5.0.5...v1.5.0.6

## [v1.5.0.5](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.5) {#v1-5-0-5}

Released on 2026-05-07 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.5) for v1.5.0.5 on GitHub.

## [v1.5.0.4](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.4) {#v1-5-0-4}

Released on 2026-05-06 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.4) for v1.5.0.4 on GitHub.

## [v1.5.0.3](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.3) {#v1-5-0-3}

Released on 2026-04-25 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.3) for v1.5.0.3 on GitHub.

## [v1.5.0.2](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.2) {#v1-5-0-2}

Released on 2026-04-13 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.2) for v1.5.0.2 on GitHub.

## [v1.5.0.1](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.1) {#v1-5-0-1}

Released on 2026-03-25 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.1) for v1.5.0.1 on GitHub.

## [v1.5.0](https://github.com/datahub-project/datahub/releases/tag/v1.5.0) {#v1-5-0}

Released on 2026-03-24 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0) for v1.5.0 on GitHub.

