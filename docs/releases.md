
# DataHub Releases

## Summary

| Version | Release Date | Links |
| ------- | ------------ | ----- |
| **v1.8.0** | 2026-10-12 |[Release Notes](#v1-8-0), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.8.0) |
| **v1.6.0.3** | 2026-09-25 |[Release Notes](#v1-6-0-3), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.6.0.3) |
| **v1.7.0.1** | 2026-09-03 |[Release Notes](#v1-7-0-1), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.7.0.1) |
| **v1.6.0.2** | 2026-09-01 |[Release Notes](#v1-6-0-2), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.6.0.2) |
| **v1.6.0.1** | 2026-08-13 |[Release Notes](#v1-6-0-1), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.6.0.1) |
| **v1.7.0** | 2026-08-04 |[Release Notes](#v1-7-0), [View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.7.0) |
| **v1.6.0** | 2026-05-21 |[View on GitHub](https://github.com/datahub-project/datahub/releases/tag/v1.6.0) |


## [v1.8.0](https://github.com/datahub-project/datahub/releases/tag/v1.8.0) {#v1-8-0}

Released on 2026-10-12 by [@datahub-release-bot[bot]](https://github.com/apps/datahub-release-bot).

### v1.8.0

#### Release Availability Date

22 Oct 2026

#### Requirements

- **CLI / Python SDK:** 1.7.0.14
- **Helm chart:** 1.1.8

Upgrade actions are in [Updating DataHub](https://docs.datahub.com/docs/how/updating-datahub#v180).

#### Highlights

- **OpenSearch 3.x.** Supported next to OpenSearch 2.x and Elasticsearch 8.x. The engine version is detected automatically. OpenSearch 3.x will not create new indexes with the `nmslib` kNN engine, and it enforces the 512-byte document-id limit on bulk writes.
- **Metrics and Semantic Models.** Charts, dashboards, and datasets can take a Metric as an upstream, with column-level lineage through metric upstreams. Metrics and Semantic Models are included in global search. The feature ships **off**. Set `METRICS_ENABLED=true` on GMS to show it.
- **Dark mode.** A dark theme is in this release and stays **off** until `THEME_DARK_MODE_ENABLED=true`. With that set, the toggle is under **Settings → Appearance**.
- **Structured Properties.** Creating, editing, and viewing a structured property is a page rather than a side drawer. Allowed values can be reordered, each with an optional description. Leaving the page with unsaved edits prompts before discarding them. The properties table adds **Type** and **Platforms** columns.
- **Data Products.** Data Products can nest under a parent Data Product. Data Products are filterable in global search. On upgrade, a one-time system update fills the asset-side `dataProducts` search field from stored membership so that filter matches existing members. It is on by default (`BOOTSTRAP_SYSTEM_UPDATE_DATA_PRODUCT_ASSETS_ENABLED=true`).
- **New ingestion sources.** [IBM/HCL Informix](https://docs.datahub.com/docs/generated/ingestion/sources/informix), [Monte Carlo](https://docs.datahub.com/docs/generated/ingestion/sources/montecarlo), and SQLMesh.

#### Product

- **Compliance forms** can be assigned to owners of a specific ownership type.
- **Deprecation date** input is editable.
- **Languages in public beta:** Japanese, Simplified Chinese, Traditional Chinese, and Russian.
- **Sidebars.** Domains, glossary, metrics, and documents sidebars can sort and filter. The home page Domains module lists up to 25 domains.
- **Views** can filter on structured properties.
- **Incidents** can be raised on ML models, ML features, ML feature tables, and schema fields.

#### Platform

- **Search document ids (Search V3).** Search V3 documents use a SHA-256 of the entity URN as `_id`. V2 indexes are unchanged. If an environment already wrote V3 documents under URL-encoded ids, hashed ids are not an in-place rewrite. Run RestoreIndices with `-a clean`, or reindex into a fresh index. A fill without clean leaves the old `_id` in place and the next update creates a duplicate.
- **GraphQL worker pool.** When `GRAPHQL_CONCURRENCY_SEPARATE_THREAD_POOL=true`, pool sizes no longer follow the node’s CPU count. Defaults are the 8-core sizes: core 40, max 800. The separate pool stays off by default.
- **AWS IRSA.** GMS keeps one process-wide default credentials provider, built once and closed on shutdown.
- **Timeseries reads.** Dataset profile, usage, and operations, and dashboard usage statistics, require the matching View privileges on Rest.li and OpenAPI as well as GraphQL.
- **Access tokens.** Creating a token with no expiry is disabled. Allowed durations default to 1 hour, 1 day, 7 days, 30 days, 90 days, 180 days, and 365 days. Tokens that already exist, including never-expiring ones, keep working.
- **Query SQL.** Reading a query, a view definition, or `topSqlQueries` requires **View Entity Queries** on a subject dataset, or the platform privilege **View All Queries**. On upgrade, policies that already grant View Entity Page are backfilled with both. A policy created after the upgrade that only grants View Entity Page does not reveal query SQL.

#### Ingestion

**New sources:** IBM/HCL Informix, Monte Carlo, SQLMesh.

**Connectors:**

- **Snowflake.** Logical tables in semantic views keep their stored casing. `preserve_column_case` keeps Snowflake’s column casing in field paths and column-level lineage. It is off by default. Turning it on changes every column’s `schemaField` URN.
- **BigQuery.** Sharing linked datasets, multi-column partition info, and dataset profiles for materialized views. The profiler lives in its own package.
- **Cube.** Cube views are ingested as Semantic Models. `emit_semantic_model_entities: true` emits first-class semantic model, metric, and semantic-model dataset entities. That path needs `METRICS_ENABLED=true`.
- **Dataplex.** Metadata Export API extraction.
- **Metabase.** Column-level lineage, model ingestion, and collection tags and containers.
- **MicroStrategy.** Project schema can be ingested as a Semantic Model.
- **Power BI.** Lineage for BigQuery `EXTERNAL_QUERY`.
- **SAP Analytics Cloud.** Column-level lineage to SAP Datasphere for DWC live models, and SAC to SAP Datasphere live-model lineage.
- **SAP Datasphere.** Calculated column formulas are written into the column description.
- **Sigma.** Dataset lineage is recovered from Sigma’s connection metadata. See Breaking Changes.
- **Kafka Connect.** SQL Server is a three-level platform. See Breaking Changes.
- **Looker.** Per-field explore usage from System Activity.
- **Airbyte.** Inactive connections can be ingested by opting in. Doris can ingest Iceberg tables in non-default catalogs.

**Ingestion infrastructure:**

- Connector support tiers are **Alpha**, **Beta**, and **GA**. The old names Certified, Incubating, and Testing are removed.
- SQL lineage timeout is configurable.
- Structured property settings can be ingested from the Python SDK.
- Stdin envelope secrets are registered for masking when they arrive.
- `confluent-kafka` is 2.15.1. A custom `oauth_cb` must return an absolute expiry time, not a lifetime in seconds.

#### Breaking Changes

Full actions are in [Updating DataHub](https://docs.datahub.com/docs/how/updating-datahub#v180).

- **Sigma ingestion.** Sigma Dataset lineage URNs are built from `connection_to_platform_map` rather than `chart_sources_platform_mapping`. Add the Sigma `connectionId` to that map with the `env` and `platform_instance` your warehouse connector used. Set `convert_urns_to_lowercase: true` if you want the previous spelling for those table names.
- **Structured property hard-delete.** An active structured property cannot be hard-deleted, and its `propertyDefinition` aspect cannot be deleted directly, until the property is soft-deleted. The UI does both steps. Automation that issued one hard delete must soft-delete, then hard-delete.
- **Timeseries read authorization.** API clients that read dataset profile, usage, or operations with only entity GET need **View Dataset Profile**, **View Dataset Usage**, or **View Dataset Operations**. Dedicated timeseries APIs also require **Get Timeseries Aspect API** when REST API authorization is enabled.
- **Role and group membership.** Adding a role (`roleMembership`) requires **Manage Policies**. Adding a group member requires **Edit Group Members**. Adding a group owner requires **Edit Owners**. When the acting user is the one being added, both require **Manage Users & Groups**. **Edit Entity** on a user is no longer enough to grant that user Admin.
- **Analytics API.** `POST /openapi/v2/analytics/datahub_usage_events/_search` requires **Analytics API access** or **Manage System Operations**. The in-app analytics dashboard still uses **View Analytics**.
- **Compliance form assignment.** `batchAssignForm`, `batchRemoveForm`, `createDynamicFormAssignment`, and writes that change which forms are on an asset, require **Manage Compliance Forms**. Completing an assigned form is unchanged.
- **Asset summary settings.** `updateAssetSettings` and writes to `assetSettings` require **Edit Entity** or **Manage Asset Summary** on the asset. OpenAPI and Rest.li still treat the write as a generic entity update, so **Manage Asset Summary** alone is not enough there.
- **Never-expiring access tokens.** Disabled unless `ACCESS_TOKEN_ALLOW_NO_EXPIRY=true`. Set `ACCESS_TOKEN_ALLOWED_DURATIONS` to change the allowed TTLs.
- **OpenAPI create is per URN.** A principal with only **Create Entity** cannot overwrite an existing entity through create or UPSERT. Grant **Edit Entity** to callers that update existing entities.
- **Domain-scoped create.** Domain-scoped Create and Edit policies are evaluated against the domains proposed on a new entity. A `domains` PATCH requires **Edit Entity**, and domain-scoped Edit must allow both the previous and the new domain membership.
- **Group member removal.** `removeGroupMembers` removes members who only have the legacy `groupMembership` aspect.
- **Documents.** The default Editor role no longer has **Manage Documents**. Editors can create, edit, and move documents, and can delete documents they own.
- **Password hashes.** Reading `corpUserCredentials` requires **Manage User Credentials**.
- **Kafka Connect.** SQL Server sink URNs gain a `dbo` schema when `schema.name` is unset (`database.table` becomes `database.dbo.table`). A SQL Server source dataset with no resolvable schema is dropped with a warning. Oracle JDBC sinks emit `schema.table`.
- **Elasticsearch ingestion.** `acryl-datahub[elasticsearch]` requires `opensearch-py` 3.x. The 3.x client still talks to OpenSearch 2 clusters.
- **Great Expectations SQL profiler.** Removed. The SQLAlchemy profiler is the only SQL profiler. `profiling.method` and the `acryl-datahub[profiling-ge]` extra are gone. This does not affect the Great Expectations integration that ingests GX validation results.
- **MySQL, MariaDB, Doris, and TiDB profiling limits.** `profiling.profile_table_row_limit` and `profiling.profile_table_size_limit` are enforced. They were previously accepted and ignored. Both still default to unset on these sources.
- **`sql-queries`.** A run where every input line is unparseable, or every query fails, exits non-zero. `enable_lazy_schema_loading` is removed. Report fields `num_queries_processed_sequential` and `num_temp_tables_detected` are renamed.
- **Python SDK circuit breakers.** `Assertion` and `Operation` clients take `graph=` instead of `transport=`. GraphQL errors raise `GraphError`.
- **Connector support status names.** `SupportStatus.CERTIFIED`, `INCUBATING`, and `TESTING` are removed. Use `GA`, `BETA`, and `ALPHA`.
- **Python extras.** `notion`, `confluence`, `unstructured`, and `datahub-documents` require Python 3.11+ and are no longer installed by `acryl-datahub[all]`.
- **Kafka OAuth callbacks.** Since `confluent-kafka` 2.13, the second value returned by `oauth_cb` is an absolute expiry in epoch seconds. A callback that returns a lifetime such as `3600` now looks already expired.
- **Search indexing startup.** GMS refuses to start when it consumes MCLs and both UI indexing paths are off (`PRE_PROCESS_HOOKS_UI_ENABLED=false` and `PRE_PROCESS_HOOKS_REPROCESS_ENABLED` unset or false).
- **pgQueue idle sleep.** `DATAHUB_PGQUEUE_EMPTY_POLL_SLEEP_MS` and `DATAHUB_PGQUEUE_MCL_EMPTY_POLL_SLEEP_MS` are removed. Use the new min and per-topic max settings. pgQueue maintenance intervals must divide the clock (for example 15 minutes, not 7).
- **OpenAPI token recipes.** `get_token` is a typed object and cannot be combined with `token` or `bearer_token`.
- **Actions Kafka.** A pipeline whose Kafka source cannot connect stops, and the actions process exits, instead of staying up while consuming nothing.
- **Log line format.** `datahub-frontend`, `datahub-upgrade`, and GMS log lines no longer include the source line number.

#### Deprecations

- **OpenAPI v2 entity and relationship APIs** (`/openapi/v2/entity`, `/openapi/v2/relationship`) are deprecated in favor of `/openapi/v3/entity` and `/openapi/v3/relationship`. Timeseries, platform entity ingest, and timeline under v2 are not deprecated by this.
- **`semanticModelInfo.datasets`.** Membership is stored on the member. Datasets use `semanticModelProperties.semanticModel`. Metrics use `metricInfo.semanticModel`. Re-ingest Semantic Models after upgrade so metric-to-dataset lineage is populated. It is missing between the upgrade and that ingest.

#### Security

- Spring Framework 7.0.9, Spring Boot 4.0.8, Spring Security 7.0.7.
- Apache Parquet 1.18.1 (CVE-2026-73334).
- Apache Log4j 2.25.5 (CVE-2026-49844).
- Logback 1.5.38 (CVE-2026-9828, CVE-2026-10532).
- Apache Thrift `libthrift` 0.24.0 (CVE-2026-48586).
- pac4j 6.5.8 and play-pac4j 13.0.3 (CVE-2026-82461 through CVE-2026-82465).
- `acryl-datahub` no longer caps `setuptools`, so it installs alongside `setuptools>=83` (CVE-2026-59890).

#### Known Issues

None recorded in the v1.8.0 upgrade notes.


## [v1.6.0.3](https://github.com/datahub-project/datahub/releases/tag/v1.6.0.3) {#v1-6-0-3}

Released on 2026-09-25 by [@david-leifker](https://github.com/david-leifker).

### DataHub v1.6.0.3

Patch release for the **v1.6.0 LTS** line. Focus is security and authorization hardening, plus a few ingestion and platform fixes.

**Full diff:** https://github.com/datahub-project/datahub/compare/v1.6.0.2...v1.6.0.3

---

#### Operator notes

- **Python 3.11 is required** on this hotfix line (ingestion CLI/images and related packages). Python 3.10 is no longer supported for v1.6.0.x ([#19703](https://github.com/datahub-project/datahub/pull/19703)).
- **SecretService caller guard** defaults to `ENFORCE`. Secret encrypt/decrypt now requires an `OperationContext`; misconfigured callers fail closed instead of silently succeeding ([#17995](https://github.com/datahub-project/datahub/pull/17995)).
- **Opt-in frontend security headers.** Set `DATAHUB_SECURITY_HEADERS_FRAME_OPTIONS`, `DATAHUB_SECURITY_HEADERS_CONTENT_TYPE_OPTIONS`, and/or `DATAHUB_SECURITY_HEADERS_REFERRER_POLICY` to emit `X-Frame-Options`, `X-Content-Type-Options`, and `Referrer-Policy`. Unset vars omit those headers ([#19848](https://github.com/datahub-project/datahub/pull/19848)).

---

#### Security

Dependency floors and library bumps, including:

| Area | Fix |
| --- | --- |
| Snowflake connector | Floor raised to **4.7.3** (CVE-2026-15925, plus 4.7.1/4.7.2 TLS SAN regression for account locators with `_`) |
| aiohttp | **3.14.3** (CVE-2026-69244 / 59881 / 69243) |
| pip | **26.2+** (CVE-2026-13346) |
| click | **≥8.3.3** (CVE-2026-7246) |
| deepdiff | **≥8.6.2** (CVE-2026-33155) |
| unstructured | **0.24.1** (CVE-2026-71428) |
| libthrift | **0.24.0** (CVE-2026-48586) |
| MariaDB Connector/J | **2.7.14+** (CVE-2026-55856/557/558) |
| Bouncy Castle | **1.86** |
| Wolfi actions image | `WOLFI_PKG_REFRESH` bump (CVE-2026-85091 / zlib) |
| Java stack | Spring Framework **7.0.9**, Boot **4.0.8**, Security **7.0.7**; Jackson **2.22.x**; Parquet **1.18.1**; JJWT **0.13.0**; httpclient5, OTel, GitPython, cryptography / pyOpenSSL, anyio, soupsieve |

Also: setuptools **≥83** is allowed so consumers can take CVE-2026-59890 ([#19601](https://github.com/datahub-project/datahub/pull/19601)).

---

#### Authorization and secrets

- `patchEntity` / `patchEntities` authorize against the **URN’s entity type**, not the client-supplied `entityType` argument ([#19872](https://github.com/datahub-project/datahub/pull/19872)).
- **Manage User Credentials** is required to write `corpUserCredentials` (GraphQL, OpenAPI, Rest.li) ([#19967](https://github.com/datahub-project/datahub/pull/19967)).
- Asset settings, compliance form assignment, and raw analytics search require the intended privileges ([#19750](https://github.com/datahub-project/datahub/pull/19750)).
- `getSecretValues` no longer fails the whole batch when one secret cannot be decrypted ([#19539](https://github.com/datahub-project/datahub/pull/19539)).

---

#### Features

- **`semanticText` aspect** — optional curated text used as the embedding source for semantic search, instead of the entity’s full body ([#18654](https://github.com/datahub-project/datahub/pull/18654)).

---

#### Ingestion

- **confluent-kafka** raised to **2.15.1**.
- urllib3 2.x (needed by unstructured 0.24.1); vcrpy **≥8.2.0** for aiohttp 3.14 compatibility.

---

**Full Changelog:** https://github.com/datahub-project/datahub/compare/v1.6.0.2...v1.6.0.3

## [v1.7.0.1 🖖](https://github.com/datahub-project/datahub/releases/tag/v1.7.0.1) {#v1-7-0-1}

Released on 2026-09-03 by [@david-leifker](https://github.com/david-leifker).

### DataHub v1.7.0.1 🖖

Patch release for the v1.7.0 line: authorization tightening, CVE dependency bumps, entity-graph-cache and lineage correctness fixes, optional optimistic locking for aspect writes, and the remainder of the custom-assertion stack.

**Requirements**

- CLI / Python SDK: **1.7.0.9**
- Helm Chart: **1.1.0**

Full upgrade guidance, including every breaking change and migration step: [Updating DataHub — v1.7.0.1](https://docs.datahub.com/docs/how/updating-datahub#v1701).

**Upgrade path**

- **From v1.7.0:** drop-in image bump. No ZDU or Helm chart change is required. System-update runs a resumable backfill that copies embedded assertion notes to the new `assertionNote` aspect (tunable with `SYSTEM_UPDATE_ASSERTION_NOTE_MIGRATION_ENABLED`, `_BATCH_SIZE`, `_DELAY_MS`, `_LIMIT`).
- **From pre-1.7.0:** the v1.7.0 path still applies — you **must upgrade to v1.6.0 first** (Helm chart **1.0.3**, let system-update complete), then upgrade to v1.7.0.1 with Helm chart **1.1.0**. Do not skip 1.6.0.

**Model changes:** additive only. The `assertionNote` aspect is now registered on the `assertion` entity so ingestion can fully upsert `assertionInfo` without clobbering user-authored notes. Assertion URNs and run history are unchanged.

---

#### Action required before upgrading

| Area | What changed | Who is affected |
| --- | --- | --- |
| **System client secret** | The hardcoded `systemClientSecret` default (`JohnSnowKnowsNothing`) is removed from the server configs ([#18886](https://github.com/datahub-project/datahub/pull/18886)) | Set `DATAHUB_SYSTEM_CLIENT_SECRET` on GMS, MAE/MCE/PE consumers, the frontend, and Actions **before** upgrading if you still rely on the built-in default |
| **Timeseries reads** | Dataset profile / usage / operations timeseries (and dashboard usage stats) now require the matching **View Dataset Profile / Usage / Operations** privileges on Rest.li and OpenAPI as well as GraphQL ([#19508](https://github.com/datahub-project/datahub/pull/19508)) | API clients that previously read these aspects with only entity GET — grant the privileges or rely on the default `view-dataset-sensitive` policy |
| **Role & group membership writes** | `roleMembership` requires **Manage Policies**; `groupMembership` / `nativeGroupMembership` requires **Edit Group Members**; corpGroup `ownership` requires **Edit Owners** — checked at the aspect layer across GraphQL, OpenAPI, and Rest.li ([#19360](https://github.com/datahub-project/datahub/pull/19360)) | Automation writing membership outside the UI. Only additions are checked; removals, unchanged re-ingestion, and system writes pass. Toggle with `metadataChangeProposal.validation.aspectAuthorization.privilegeGrant.enabled` (default `true`) |
| **View authorization (VBAC)** | `container` and `schemaField` are view-restricted by default; schema-field View Entity Page inherits from the parent dataset ([#19212](https://github.com/datahub-project/datahub/pull/19212)) | Deployments with `VIEW_AUTHORIZATION_ENABLED=true` — ensure View Entity Page policies cover containers and parent datasets |

---

#### Security and authorization

- **Default self policy** ([#19297](https://github.com/datahub-project/datahub/pull/19297)) — limited to `VIEW_ENTITY_PAGE` and `GET_ENTITY` on the actor's own entity.
- **GMS `/auth` helpers** ([#19316](https://github.com/datahub-project/datahub/pull/19316)) — `signUp`, `resetNativeUserCredentials`, `verifyNativeUserCredentials`, and `getSsoSettings` now require system client credentials, matching `generateSessionTokenForUser`. Frontend login, signup, password reset, and SSO are unchanged.
- **System-write identification** ([#19384](https://github.com/datahub-project/datahub/pull/19384)) — aspect authorization no longer treats client-supplied `appSource` as proof of a system write.
- **Schema-field incidents** ([#19405](https://github.com/datahub-project/datahub/pull/19405)) — authorized against the parent entity.
- **Lineage scroll** ([#19463](https://github.com/datahub-project/datahub/pull/19463)) — unauthorized OpenAPI lineage scroll endpoints are redacted as restricted.
- **UI sanitization** ([#19298](https://github.com/datahub-project/datahub/pull/19298), [#19299](https://github.com/datahub-project/datahub/pull/19299), [#19300](https://github.com/datahub-project/datahub/pull/19300)) — query and incident descriptions, documentation PDF preview iframe `src`, and embed `renderUrl` schemes.
- **Ingestion secret masking** ([#19515](https://github.com/datahub-project/datahub/pull/19515), [#19526](https://github.com/datahub-project/datahub/pull/19526), [#19561](https://github.com/datahub-project/datahub/pull/19561), [#19332](https://github.com/datahub-project/datahub/pull/19332)) — masking is unified, idempotent, and fail-closed (mask before truncation; install once per process). Recorded HTTP cassettes are scrubbed.
- **AWS credentials** ([#19483](https://github.com/datahub-project/datahub/pull/19483)) — OpenSearch IAM signing, Bedrock embeddings, STS, and object storage share a process-wide credential provider and client lifecycle.

#### Reliability and performance

- **Entity-graph cache** ([#19498](https://github.com/datahub-project/datahub/pull/19498), [#19288](https://github.com/datahub-project/datahub/pull/19288), [#19301](https://github.com/datahub-project/datahub/pull/19301), [#19412](https://github.com/datahub-project/datahub/pull/19412)) — relationships are no longer silently dropped; indexed search fields are fetched for the cache; null relationship endpoints are skipped; hierarchy graph fallback batches frontier scrolls.
- **Container contents** ([#19489](https://github.com/datahub-project/datahub/pull/19489)) — `Container.relationships(types: [IsPartOf], direction: INCOMING)` again returns all contained entities from the live graph.
- **Lineage traversal** ([#19413](https://github.com/datahub-project/datahub/pull/19413)) — `scrollAcrossLineage` is bounded to prevent GMS OOM on wide graphs.
- **Case-insensitive `relatedEntityTypes`** ([#19268](https://github.com/datahub-project/datahub/pull/19268)).
- **Hot-path caching and batching** ([#19404](https://github.com/datahub-project/datahub/pull/19404), [#19476](https://github.com/datahub-project/datahub/pull/19476), [#18266](https://github.com/datahub-project/datahub/pull/18266)) — corpGroup `roleMembership` cached via `SystemEntityClient`, per-request CorpUserFlags served from a cached aspect, and Dashboard `usageStats` buckets and metrics batch-loaded across a page.
- **Optional optimistic locking for aspect writes** ([#19249](https://github.com/datahub-project/datahub/pull/19249)) — `OPTIMISTIC_LOCKING_ENABLED` replaces `SELECT FOR UPDATE` with compare-and-set on `SystemMetadata.version` for Ebean storage. `SCOPED_RETRY_ENABLED` retries only the conflicted URN's branch, and `ENTITY_WRITE_LOCK_BACKEND=hazelcast` serializes same-`(urn, aspect)` writers off the DB connection pool. PostgreSQL aspect writes also take row locks in consistent primary-key order. **All of these default off.**

#### Assertions

- **Custom-assertion stack completed on the 1.7 line** ([#18882](https://github.com/datahub-project/datahub/pull/18882)) — API/SDK, UI, the `assertionNote` aspect, and the dbt / Great Expectations / YAML data-contract writers. Those writers emit `AssertionType.CUSTOM` instead of the deprecated `DATASET` shape on the next run; **assertion URNs are unchanged and run history is preserved**. Assertions that are never re-ingested keep their stored `DATASET` aspects and continue to render. See [Custom Assertions](https://docs.datahub.com/docs/api/tutorials/custom-assertions).

#### Ingestion

- **nltk 3.10.1 excluded** ([#18889](https://github.com/datahub-project/datahub/pull/18889)) — that version silently breaks document chunking.
- **GitPython floor raised to 3.1.58** ([#19506](https://github.com/datahub-project/datahub/pull/19506)).
- **Bundled default CLI / Python SDK is 1.7.0.9** ([#19605](https://github.com/datahub-project/datahub/pull/19605)).

#### Dependency CVE bumps

- **Java:** httpclient5 5.6.3 (CVE-2026-64607), Spring Boot / Spring Kafka 4.0.7 (CVE-2026-41001), OpenTelemetry Java agent 2.28.0 (CVE-2026-54704), mariadb-java-client 2.7.14 (CVE-2026-55856 / 55857 / 55858), Logback 1.5.38 (CVE-2026-9828, CVE-2026-10532), Log4j 2.25.5 (CVE-2026-49844), Netty 4.2.17.Final (CVE-2026-59902), libthrift 0.23.0 (CVE-2026-43869), Jetty 12.1.10 (CVE-2026-10050), httpcore5 5.4.3 (CVE-2026-54399), micrometer-core 1.16.6 (CVE-2026-40983, CVE-2026-40984), wire-runtime 6.3.0 (CVE-2026-45799), reactor-netty-core 1.3.6, netty-reactive-streams 3.0.9.
- **Apache Parquet 1.18.0**, which refreshes its shaded Jackson to jackson-databind 2.22.1 (CVE-2026-54512, CVE-2026-54513).
- **Python / images:** nltk 3.10.3 (CVE-2026-12075); the `datahub-actions` image python layer is cache-busted so a stale cached layer stops shipping setuptools 70.3.0 (CVE-2025-47273).

---

#### Quickstart

`datahub docker quickstart` (and `--version v1.7.0`) resolves to the `v1.7.0.1` images once the version mapping update lands.

---

#### What's Changed

* fix(ingest): exclude nltk 3.10.1, which silently breaks document chunking by [@nwadams](https://github.com/nwadams) in https://github.com/datahub-project/datahub/pull/18889
* chore(release): backport custom assertion stack to v1.7.0 by [@AdrianMachado](https://github.com/AdrianMachado) in https://github.com/datahub-project/datahub/pull/18882
* feat(optimistic-locking): optimistic locking, aspect advisory locks, and Hazelcast write gate to v1.7.0 by [@Dutt23](https://github.com/Dutt23) in https://github.com/datahub-project/datahub/pull/19249
* fix(sec): bump wire-runtime 5.2.0 → 6.3.0 for CVE-2026-45799 by [@max-datahub](https://github.com/max-datahub) in https://github.com/datahub-project/datahub/pull/19317
* fix(sec): backport security fixes and CVE dependency bumps to v1.7.0 by [@supersingh05](https://github.com/supersingh05) in https://github.com/datahub-project/datahub/pull/19415
* fix(auth): backport graph-cache, timeseries auth, and related GMS fixes to v1.7.0 by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/19548
* fix(deps): bump mariadb-java-client to 2.7.14 for CVE-2026-55856/557/558 by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/19557
* fix(deps): bump httpclient5, Spring Boot, and OTel agent for v1.7.0.1rc1 CVEs by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/19565
* fix(gms): backport IRSA credential leak fix to v1.7.0 by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/19581
* fix(sec): backport ingestion secret-masking, GitPython, and image cache-bust fixes to v1.7.0 by [@supersingh05](https://github.com/supersingh05) in https://github.com/datahub-project/datahub/pull/19591
* chore(cli): bump default cli by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/19605

**Full Changelog**: https://github.com/datahub-project/datahub/compare/v1.7.0...v1.7.0.1

## [v1.6.0.2](https://github.com/datahub-project/datahub/releases/tag/v1.6.0.2) {#v1-6-0-2}

Released on 2026-09-01 by [@david-leifker](https://github.com/david-leifker).

### What's Changed
* fix(sec): backport security fixes and CVE dependency bumps to v1.6.0 by [@supersingh05](https://github.com/supersingh05) in https://github.com/datahub-project/datahub/pull/19414
* fix(policies): apply validator level aspect edit controls for user and group aspects ([#19360](https://github.com/datahub-project/datahub/pull/19360)) by [@RyanHolstien](https://github.com/RyanHolstien) in https://github.com/datahub-project/datahub/pull/19438
* fix(sec): backport GMS /auth helper gate and timeseries view privileges to v1.6.0 by [@david-leifker](https://github.com/david-leifker) in https://github.com/datahub-project/datahub/pull/19529


**Full Changelog**: https://github.com/datahub-project/datahub/compare/v1.6.0.1...v1.6.0.2

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

