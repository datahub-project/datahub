
# DataHub Releases

## Summary

| Version | Release Date | Links |
| ------- | ------------ | ----- |
| **v1.6.0.1** | 2026-08-13 |[Release Notes](#v1-6-0-1), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.6.0.1) |
| **v1.7.0** | 2026-08-04 |[Release Notes](#v1-7-0), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.7.0) |
| **v1.6.0** | 2026-05-21 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.6.0) |
| **v1.5.0.7** | 2026-05-19 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.7) |
| **v1.5.0.6** | 2026-05-11 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.6) |
| **v1.5.0.5** | 2026-05-07 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.5) |
| **v1.5.0.4** | 2026-05-06 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.4) |
| **v1.5.0.3** | 2026-04-25 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.3) |
| **v1.5.0.2** | 2026-04-13 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.2) |


## [v1.6.0.1](https://github.com/datahub-project/datahub/releases/tag/v1.6.0.1) {#v1-6-0-1}

Released on 2026-08-13 by [@david-leifker](https://github.com/david-leifker).

### What's Changed
* fix(security): backport security fixes to v1.6.0 by [@supersingh05](https://github.com/supersingh05) in https://github.com/datahub-project/datahub/pull/18613
* fix(release): backport v1.6.0 platform bugfixes from master by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/18683
* fix(deps): bump lz4-java to 1.11.1 and jackson to 2.21.5 (security, v1.6.0) by [@supersingh05](https://github.com/supersingh05) in https://github.com/datahub-project/datahub/pull/18741
* fix(backport): v1.6.0 — transaction retry backoff/conflict API, PostgreSQL lock ordering, Spring 7.0.8 by [@supersingh05](https://github.com/supersingh05) in https://github.com/datahub-project/datahub/pull/18798
* fix(sec): bump wire-runtime 5.2.0 → 6.3.0 for CVE-2026-45799 by [@supersingh05](https://github.com/supersingh05) in https://github.com/datahub-project/datahub/pull/19100
* fix(sec): backport security dependency bumps to v1.6.0 by [@supersingh05](https://github.com/supersingh05) in https://github.com/datahub-project/datahub/pull/19179
* fix(deps): bump Apache Parquet stack to 1.18.0 for shaded Jackson CVEs by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/19191
* fix(deps): bump Log4j to 2.25.5 for CVE-2026-49844 by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/19194


**Full Changelog**: https://github.com/datahub-project/datahub/compare/v1.6.0...v1.6.0.1

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

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.6.0) for v1.6.0 on GitHub.

## [v1.5.0.7](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.7) {#v1-5-0-7}

Released on 2026-05-19 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.7) for v1.5.0.7 on GitHub.

## [v1.5.0.6](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.6) {#v1-5-0-6}

Released on 2026-05-11 by [@david-leifker](https://github.com/david-leifker).

View the [release notes](https://github.com/datahub-project/datahub/releases/tag/v1.5.0.6) for v1.5.0.6 on GitHub.

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

