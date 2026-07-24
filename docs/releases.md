
# DataHub Releases

## Summary

| Version | Release Date | Links |
| ------- | ------------ | ----- |
| **v1.6.0** | 2026-05-21 |[Release Notes](#v1-6-0), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.6.0) |
| **v1.5.0.7** | 2026-05-19 |[Release Notes](#v1-5-0-7), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.7) |
| **v1.5.0.6** | 2026-05-11 |[Release Notes](#v1-5-0-6), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.6) |
| **v1.5.0.5** | 2026-05-07 |[Release Notes](#v1-5-0-5), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.5) |
| **v1.5.0.4** | 2026-05-06 |[Release Notes](#v1-5-0-4), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.4) |
| **v1.5.0.3** | 2026-04-25 |[Release Notes](#v1-5-0-3), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.3) |
| **v1.5.0.2** | 2026-04-13 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.2) |
| **v1.5.0.1** | 2026-03-25 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.1) |
| **v1.5.0** | 2026-03-24 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0) |
| **v1.4.0.3** | 2026-02-19 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.4.0.3) |


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

Security Update

**Full Changelog**: https://github.com/datahub-project/datahub/compare/v1.5.0.4...v1.5.0.5

## [v1.5.0.4](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.4) {#v1-5-0-4}

Released on 2026-05-06 by [@david-leifker](https://github.com/david-leifker).

Security Update

**Full Changelog**: https://github.com/datahub-project/datahub/compare/v1.5.0.3...v1.5.0.4

## [v1.5.0.3](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.3) {#v1-5-0-3}

Released on 2026-04-25 by [@david-leifker](https://github.com/david-leifker).

Security Patch Release

**Full Changelog**: https://github.com/datahub-project/datahub/compare/v1.5.0.2...v1.5.0.3

## [v1.5.0.2](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.2) {#v1-5-0-2}

Released on 2026-04-13 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.2) for v1.5.0.2 on GitHub.

## [v1.5.0.1](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.1) {#v1-5-0-1}

Released on 2026-03-25 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.1) for v1.5.0.1 on GitHub.

## [v1.5.0](https://github.com/datahub-project/datahub/releases/tag/v1.5.0) {#v1-5-0}

Released on 2026-03-24 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0) for v1.5.0 on GitHub.

## [v1.4.0.3](https://github.com/datahub-project/datahub/releases/tag/v1.4.0.3) {#v1-4-0-3}

Released on 2026-02-19 by [@jjoyce0510](https://github.com/jjoyce0510).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.4.0.3) for v1.4.0.3 on GitHub.

