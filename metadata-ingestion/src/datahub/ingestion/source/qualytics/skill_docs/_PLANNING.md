# Qualytics Connector — Planning Document

**Created**: 2026-09-09
**Status**: APPROVED
**API contract**: `tests/unit/qualytics/fixtures/openapi.json` — Qualytics `20260910-6599e5c`, trimmed to the
7 paths and 121 schemas the connector uses (of 331 and 690). Planned against the
`20260909-74dac74` spec.

Produced with `/datahub-connector-planning` after `/load-standards`. Where this document and
the initial internal plan disagree, **this document wins** — that plan's mapping was a
hypothesis written before the OpenAPI spec was read, and the spec corrected several parts
of it.

---

## Overview

Qualytics is a data quality platform. It connects to a customer's warehouses and lakes,
profiles the tables it finds, infers and enforces quality checks against them, and records
the anomalies those checks produce.

This connector pulls that signal into DataHub. Its purpose is **not** to catalog the
customer's tables — their Snowflake/BigQuery/S3 source already does that. It is to attach
Qualytics' quality judgements to those already-catalogued datasets, so the Validation and
Stats tabs on a dataset a user already browses reflect Qualytics' coverage and history.

---

## Research Summary

### Source Classification

- **Type**: API (REST)
- **Source Category**: **none of the skill's eleven** — Qualytics is a data-quality /
  observability platform. Its primary output is `Assertion` entities, not Datasets,
  Dashboards or Models. The closest analogue in the standards is nothing; the closest
  analogue in the _codebase_ is `montecarlo`, which is also uncategorised.
- **Interface**: REST + OpenAPI 3.1, per-deployment spec at `GET {base_url}/openapi.json`
- **Auth**: HTTP Bearer (`AsyncHTTPBearer` in the spec). Tokens minted at
  `POST /api/user-tokens`; validated as JWTs from the Qualytics issuer.
- **Pagination**: `fastapi-pagination` envelope — `?page=N&size=M` →
  `{items, page, pages, size, total}`. Uniform across every list endpoint.
- **Standards file**: `standards/api.md` (no source-type file applies)
- **Docker image for testing**: none, and none possible — see Known Limitations.

### Similar DataHub Connectors

| Connector                              | Relevance     | Key patterns to copy                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| -------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `montecarlo`                           | **Very high** | Same problem exactly: a data-quality SaaS attaching assertions to datasets a warehouse source already emitted. Copy its module layout (`config.py`, `client.py`, `source.py`, `report.py`, `assertion.py`, `mcon_resolver.py`), its `connection_to_platform_map` config shape, its `_validate_platform_value()` against `get_known_data_platforms()`, and its `_emit()` error-isolation helper (per-item failures → warning + skip; run-level failures propagate). |
| `dbt` (`dbt_tests.py`)                 | Medium        | A second, differently-shaped take on tests → assertions. Useful when the Qualytics rule taxonomy doesn't fit Monte Carlo's shape.                                                                                                                                                                                                                                                                                                                                  |
| `snowflake` (`snowflake_assertion.py`) | Medium        | Assertion emission from inside a warehouse source.                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `sqlmesh` (`assertions.py`)            | Medium        | Recent, clean assertion code.                                                                                                                                                                                                                                                                                                                                                                                                                                      |

### Correction, recorded during Phase 4

The prior art below turned out to be **much less reusable than this document
originally claimed**. `_build_dataset_urn` handles JDBC only — it returns `None` for
`dfs` and `native` — hardcodes `env="PROD"`, has no concept of `platform_instance`, and
builds URNs by string concatenation instead of the typed builders the standards
require. What genuinely carried over is the `database.schema.table` shape.

Its platform table also needed correcting: it maps `mariadb → mysql`, which predates
DataHub shipping a dedicated `mariadb` source that emits `mariadb` URNs. Copying it
would attach assertions to a `mysql` URN that a MariaDB-sourced DataHub never emitted.
The resolver maps `mariadb → mariadb`; the two integrations will therefore disagree on
MariaDB datastores, which belongs in the push/pull coexistence discussion.

### Prior art we own

Qualytics already ships a **push** integration (Qualytics → DataHub, configured inside
Qualytics). Two things to reuse rather than reinvent:

1. **Its source-platform dataset and schemaField URN reconstruction**, already proven
   against customer deployments.
2. **The 12 structured-property `qualifiedName`s** — a published contract. Renaming any of
   them orphans values already pushed into customer DataHubs:
   `qualytics.url`, `qualytics.qualityScoreTotal`, and `qualityScore{Completeness,
Coverage, Conformity, Consistency, Precision, Timeliness, Volumetrics, Accuracy}`, plus
   `qualytics.activeAnomalyCount` and `qualytics.activeCheckCount`. These map 1:1 onto the
   spec's `QualityScore` schema fields.

---

## What the spec corrected

Four findings that changed the plan. All four came from reading
`tests/unit/qualytics/fixtures/openapi.json`; none were visible from the initial plan.

1. **49 rule types, not 47.** And the initial plan's grouping omitted `containsCreditCard` —
   the exact silent-drop failure the design is supposed to prevent. The mapping table below
   covers all 49, and a test enumerates `RuleType` from the committed spec so a 50th fails
   CI rather than vanishing at runtime.
2. **Profile endpoints are `/containers/{id}/profile` and `/containers/{id}/field-profiles`.**
   The `latest-profile` / `latest-field-profiles` names guessed in Phase 0 do not exist.
3. **Every spec path carries the deployment's API root path** (`/api/...`), which each
   deployment configures — so `/api` is a default, not a constant.
4. **Rich server-side filtering exists and should be used.** `/api/quality-checks` accepts
   `container`, `datastore`, `rule_type`, `status`, `archived`; `/api/anomalies` accepts
   `container`, `start_date`, `end_date`, `timeframe`, `status`. Filtering server-side is
   how assertion-volume blowup gets solved — do not fetch-then-discard.

---

## Entity Mapping

The template has no assertion-centric table, so this one is purpose-built. **Mode A** is the
default and attaches to the underlying platform's existing dataset URNs. **Mode B** is
opt-in and emits `urn:li:dataPlatform:qualytics` entities only for assets with no
source-side equivalent.

### v1 (this PR)

| Qualytics concept             | Source endpoint                       | DataHub entity / aspect                               | Notes                                                                                                                                                             |
| ----------------------------- | ------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Datastore                     | `GET /datastores`                     | _(not emitted)_                                       | Mode A: the resolution key that maps containers onto the source platform. Discriminated on `store_type` ∈ jdbc / dfs / native.                                    |
| Container (table, view, file) | `GET /containers`                     | **Dataset** — resolved to the _source platform's_ URN | Discriminated on `container_type`. `subTypes` from `table_type`.                                                                                                  |
| Field                         | `GET /containers/{id}/fields`         | ~~`schemaMetadata`~~ — **dropped from v1**, see below | Field-level signal still reaches DataHub via field profiles and schemaField-scoped tags.                                                                          |
| Container profile             | `GET /containers/{id}/profile`        | `datasetProfile`                                      | `records_count` → `rowCount`, `field_profiles_count` → `columnCount`.                                                                                             |
| Field profile                 | `GET /containers/{id}/field-profiles` | `datasetFieldProfile`                                 | min/max/mean/median/std_dev/q1/q3, `approximate_distinct_values` → `uniqueCount`, `completeness` → null counts, `histogram_buckets` → `distinctValueFrequencies`. |
| Quality check                 | `GET /quality-checks`                 | **Assertion** + `assertionInfo`                       | One assertion per check. See rule-type mapping.                                                                                                                   |
| Anomaly / check result        | `GET /anomalies` (windowed)           | `assertionRunEvent`                                   | SUCCESS/FAILURE, `anomalous_records_count` → `rowCount`, Qualytics `message` in `nativeResults`.                                                                  |
| Quality score + 8 dimensions  | on container/field                    | **Structured properties**                             | Reuse the push integration's `qualifiedName`s verbatim.                                                                                                           |
| Global tag                    | `GET /global-tags`                    | `globalTags`                                          | Optional (`emit_tags`, default on).                                                                                                                               |
| —                             | —                                     | `dataPlatformInstance`                                | Required on **every** entity. SDK V2 emits it.                                                                                                                    |

### Correction, recorded during Phase 5: no `schemaMetadata` in Mode A

This document originally had v1 emitting `schemaMetadata` built from Qualytics fields.
**That is wrong in the default mode and has been dropped.**

In Mode A the dataset belongs to the customer's warehouse source, and that source owns
its schema. Qualytics' view is a _subset_ of it — the connector honours `exclude_fields`,
and fields carry `status` values of `excluded`, `missing` and `masked`. Emitting our
copy would overwrite an authoritative schema with a partial one on every run: the same
last-writer-wins problem as push/pull coexistence, but against metadata we have no claim to.

Nothing is lost by dropping it. Field profiles reference `fieldPath` strings and
field-level tags reference `schemaField` URNs; neither requires owning `schemaMetadata`.
The precedent agrees — montecarlo, the closest analogue, emits assertions and run events
and no schema at all.

`schemaMetadata` becomes correct again in **Mode B**, where Qualytics-native computed
containers have no other source and we are the only writer. It moves there with the rest
of Mode B.

Consequences already applied: the `SCHEMA_METADATA` capability decorator is removed (the
connector would have been advertising something it does not do), and the containers read
moved from a capability probe into basic connectivity in `test_connection`, where it
belongs — nothing works without it.

### Deferred to a follow-up PR

Lineage (`/lineage/edges`, `/lineage/graph`) → `upstreamLineage`; field connections →
`fineGrainedLineage`; teams and check owners → `ownership`; active anomalies → `Incident`
(owned by the push integration, which already raises and resolves them); Mode B native datasets for
`computed_table` / `computed_file` / `computed_join` and enrichment datastores; scan and
profile operations → `Operation`.

### Rule type → assertion mapping (all 49)

Grouped, not special-cased. Every group maps to an `AssertionInfo` shape; anything
unrecognised becomes a `CUSTOM` assertion carrying `rule_type` and serialized `properties`,
counted in `report.unmapped_rule_types`.

| Group                       | Rule types                                                                                                                              | Assertion shape                                  |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| Null / emptiness            | `notNull`, `anyNotNull`, `notEmpty`                                                                                                     | FIELD — null count                               |
| Uniqueness                  | `unique`, `distinctCount`                                                                                                               | FIELD — unique count                             |
| Numeric range               | `between`, `minValue`, `maxValue`, `greaterThan`, `lessThan`, `positive`, `notNegative`, `sum`                                          | FIELD — value with operator                      |
| Pattern / PII               | `matchesPattern`, `isCreditCard`, **`containsCreditCard`**, `containsEmail`, `containsUrl`, `containsSocialSecurityNumber`, `isAddress` | FIELD — regex / native                           |
| Set membership              | `expectedValues`, `requiredValues`, `existsIn`, `notExistsIn`                                                                           | FIELD — IN / NOT_IN                              |
| Length                      | `minLength`, `maxLength`                                                                                                                | FIELD — length                                   |
| Temporal                    | `afterDateTime`, `beforeDateTime`, `betweenTimes`, `notFuture`                                                                          | FIELD — temporal                                 |
| Volume                      | `volumetric`, `minPartitionSize`, `maxPartitionSize`, `fieldCount`                                                                      | VOLUME                                           |
| Freshness                   | `freshness`, `timeDistributionSize`                                                                                                     | FRESHNESS                                        |
| Schema                      | `expectedSchema`, `isType`                                                                                                              | DATASET — schema                                 |
| SQL / expression            | `satisfiesExpression`, `aggregationComparison`, `metric`                                                                                | SQL — logic in `customAssertion.logic`           |
| Cross-field / cross-dataset | `equalTo`, `equalToField`, `greaterThanField`, `lessThanField`, `isReplicaOf`, `dataDiff`, `entityResolution`, `predictedBy`            | DATASET — CUSTOM with `rule_type` + `properties` |

**49 accounted for.** `tests/unit/test_assertion_mapping.py` will assert
`set(RuleType from spec) == set(mapped) | set(explicitly_triaged_custom)`.

---

## Architecture Decisions

### Base class

**`StatefulIngestionSourceBase` + `TestableSource`** — the standard for API connectors
(`standards/api.md`). Already scaffolded.

### Module layout

Mirrors montecarlo so the port and the review both go easily:

| File              | Responsibility                                                                                                 |
| ----------------- | -------------------------------------------------------------------------------------------------------------- |
| `config.py`       | `QualyticsSourceConfig`, `QualyticsPlatformDetail` — done in Phase 0                                           |
| `report.py`       | `QualyticsSourceReport` — done in Phase 0                                                                      |
| `constants.py`    | `PLATFORM`, API paths, `CONSUMED_PATHS` — done in Phase 0                                                      |
| `client.py`       | Bearer auth, `page`/`size` pagination, retry/backoff, TLS options, one reused session                          |
| `models.py`       | Pydantic models generated from the spec, hand-trimmed; discriminated unions on `store_type` / `container_type` |
| `urn_resolver.py` | Datastore → source-platform dataset URN (the montecarlo `mcon_resolver.py` slot)                               |
| `assertion.py`    | The 49-rule-type mapper                                                                                        |
| `profile.py`      | Container + field profiles                                                                                     |
| `source.py`       | Orchestration — done in Phase 0, filled in here                                                                |

**Emission: SDK V2 throughout** (`datahub.sdk.Dataset`, `.as_workunits()`). Mandatory for
new connectors, and it emits `dataPlatformInstance` for us. Assertions have no SDK V2
wrapper yet, so those stay MCP-based — note that explicitly in the PR so a reviewer doesn't
read it as inconsistency.

### URN resolution — the highest-consequence unit

Order of resolution for each datastore:

1. **`datastore_to_platform_map`** — explicit. **Key accepts either the datastore name or
   its integer id**: name first, then id. Names are readable in recipes but renamable; ids
   are immutable but opaque. Supporting both gives readable recipes with an escape hatch,
   and the ambiguity gets documented. (Standards require URN ids derive from immutable
   identifiers; this key is config, not a URN component, but a rename silently breaking the
   mapping is the same class of failure.)
2. **`infer_source_platform`** (default true) — from connection metadata: JDBC
   driver/product, DFS URI scheme, native catalog type. Uses top-level `platform_instance`
   and `env`, so it is only safe with one instance per platform. Documented as such.
3. **Neither** → skip, warn naming the datastore, count in `report.datastores_unresolved`.
   **Never guess.** An assertion on a hallucinated URN is worse than no assertion — it is
   invisible and it silently misleads.

Configured platform names validate at parse time against `get_known_data_platforms()`
(montecarlo's `_validate_platform_value`), so `snowflke` fails the recipe instead of
producing orphaned assertions.

**`platform_instance` is never inferred from the `base_url` host.** Standards forbid
deriving URN ids from mutable values, and a hostname is one. The initial plan proposed
exactly this; it is wrong and is not being done.

### Config

Already built in Phase 0 and standards-clean: `SecretStr` token, `AllowDenyPattern` with
`default_factory`, pydantic v2 validators, full `Field(description=...)` coverage,
`StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig]`.

Additions for v1:

- `assertion_results: BaseTimeWindowConfig` — **default window 30 days**. Uses DataHub's
  standard time-window class; a hand-rolled `*_lookback_days` field is an explicit
  anti-pattern, which is what the initial plan proposed.
- `datastore_to_platform_map` key semantics extended to accept name-or-id.

### Capabilities

| Capability                        | v1       | Notes                                                                      |
| --------------------------------- | -------- | -------------------------------------------------------------------------- |
| `PLATFORM_INSTANCE`               | ✅       | Via `datastore_to_platform_map`                                            |
| `TEST_CONNECTION`                 | ✅       | Connectivity, then each capability separately                              |
| `DATA_PROFILING`                  | ✅       | Container + field profiles                                                 |
| `SCHEMA_METADATA`                 | ❌       | Dropped — the warehouse source owns the schema; see the Phase 5 correction |
| `TAGS`                            | ✅       | `emit_tags`, default on                                                    |
| `DESCRIPTIONS`                    | ✅       | Check descriptions → assertion descriptions                                |
| `DELETION_DETECTION`              | ✅       | Stateful ingestion                                                         |
| `LINEAGE_COARSE` / `LINEAGE_FINE` | ❌ v2    | Endpoints exist and are mapped; deferred                                   |
| `OWNERSHIP`                       | ❌ v2    | Deferred                                                                   |
| `USAGE_STATS`                     | ❌ never | Qualytics has no query log; not its job                                    |

Support status: `SupportStatus.ALPHA` (the enum has ALPHA/BETA/GA/UNKNOWN — the docs
template's "Testing" badge does not exist in code).

---

## Testing Strategy

| Layer    | What                                                                                                    | Where                                                                                 |
| -------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Contract | Every `CONSUMED_PATHS` entry exists in the committed spec and supports GET                              | `tests/unit/test_api_paths.py` — **built**                                            |
| Contract | Every spec `RuleType` is mapped or explicitly triaged                                                   | `tests/unit/test_assertion.py` — **built**                                            |
| Unit     | Validators, URN resolution per store type, mappers, error paths, filtering                              | `tests/unit/`                                                                         |
| Golden   | Recorded-fixture ingestion → MCP stream compared to golden JSON                                         | `tests/unit/qualytics/golden/`                                                        |
| Live     | A self-hosted DataHub ← a Qualytics development deployment, incl. the push-integration coexistence case | **Done 2026-10-01**, manual; findings in `_datahub-connector-pr-review-2026-10-06.md` |

**No docker-compose integration test is possible** — see Known Limitations. Say so in the PR
so reviewers don't ask; DataHub's testing standard treats a missing integration test as a
blocker, so this needs pre-empting with the recorded-fixture rationale.

Tests already comply with the standards' anti-pattern list: no default-value tests, no
getter tests, no framework tests, no late imports.

---

## Known Limitations

| Limitation                                                      | Impact                                                                        | Handling                                                                                                         |
| --------------------------------------------------------------- | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| No public Qualytics sandbox                                     | DataHub's usual docker-compose integration test is impossible                 | Sanitized recorded fixtures + golden files; state the rationale in the PR; offer the partner tenant to reviewers |
| UI-based ingestion doesn't support custom sources               | Pre-merge, the connector is CLI-recipe only                                   | Documented; raises the priority of the upstream merge                                                            |
| Source URN reconstruction is inference for some store types     | Assertions could attach to the wrong dataset                                  | Explicit map, platform-name validation, skip-and-warn rather than guess                                          |
| Qualytics is single-tenant, ids are per-deployment sequences    | Two deployments into one DataHub collide                                      | `platform_instance` required whenever more than one deployment feeds a DataHub                                   |
| Per-deployment API root path and version skew                   | Endpoint or enum drift between tenants                                        | Spec-backed `CONSUMED_PATHS` test; unknown enums degrade to custom + warning                                     |
| Profiles are as fresh as the last Qualytics profile run         | Stats tab can look stale                                                      | Documented in `qualytics_post.md`                                                                                |
| Object-store dataset names depend on the customer's `path_spec` | DFS assertions may attach to a path DataHub doesn't use as its table boundary | Reconstruct faithfully, mark not-confident, count separately in the report, recommend the explicit map           |
| Platform-name validation is inert on pip installs               | A typo like `snowflke` passes config and yields invisible assertions          | Upstream packaging bug (below). Validation works on monorepo installs; report it upstream                        |

### Upstream bug: the connector registry is missing from released wheels

`acryl-datahub`'s `setup.py` declares `package_data` for
`datahub.ingestion.autogenerated` (`*.json`) but not for its
`connector_registry` **subpackage**, so `connector_registry/datahub.json` exists in the
monorepo and is absent from every published wheel. `get_known_data_platforms()`
consequently returns `None` for all pip-installed users, silently disabling the
platform-name validation montecarlo's own docstring calls its single source of truth.

Our validator degrades the same documented way (skip, don't fail). Worth reporting
upstream separately from the connector PR — it's a small, self-contained fix that
benefits montecarlo and kafka_connect too, and it is good first-contribution currency
with the maintainers.
| Assertion volume on large tenants | Could overwhelm target DataHub | Server-side filtering, 30-day result window, `max_workers`, patterns |

---

## Implementation Order

**Phase 2 — client + models.** `client.py` (bearer, `page`/`size`, retry/backoff, TLS,
session reuse); `models.py` from the spec with discriminated unions; `test_connection()`
returning a `TestConnectionReport`. Unit tests on `requests_mock`.

**Phase 4 — `urn_resolver.py`.** Port `_build_dataset_urn` per store type. Table-driven
tests across snowflake / bigquery / databricks / postgres / redshift / s3 / iceberg, plus
the unresolvable case. Riskiest unit; do it before any mapper.

**Phase 5 — schema + profile mappers.** `schemaMetadata` incl. nested `parent_field_id`
paths; `datasetProfile` / `datasetFieldProfile`. First golden files.

**Phase 6 — assertion mapper — ✅ DONE.** All 49 rule types, table-driven, with the
spec-enumerated coverage test.

Refinement to the shape described above: every check is emitted as
`AssertionTypeClass.CUSTOM` carrying a `CustomAssertionInfoClass`, rather than as the
FIELD / VOLUME / FRESHNESS / SQL assertion _types_. Both precedents — dbt's
`dbt_tests.py` and `montecarlo/assertion.py` — independently converge on CUSTOM, and it
is the accurate description of an externally defined, externally evaluated check.
Nothing is lost: `CustomAssertionInfoClass` still carries `scope`, `operator`,
`aggregation`, `parameters`, `fields` and `logic`, so the semantic grouping in the table
above survives — it now drives those fields instead of the top-level type.

**Phase 7 — anomalies → `assertionRunEvent` — ✅ DONE**, windowed via
`BaseTimeWindowConfig` (30-day default). Results come from two sources, because neither
alone is sufficient: the check's own `is_passing`/`last_asserted` is the current verdict
_and the only source of passes_ (Qualytics records anomalies, not successes, so
anomalies alone would make every dataset look permanently broken), while anomalies in
the window supply the failure history.

**Wiring — ✅ DONE.** `get_workunits_internal` walks datastores → containers, emitting
profiles, assertions and results per container, with `StaleEntityRemovalHandler`
registered. A recipe now produces real metadata end to end.

### Correction, recorded during the Phase 7 wiring: two meanings of `platform_instance`

Caught by the first end-to-end test. The top-level `platform_instance` identifies the
**Qualytics deployment** — it namespaces assertion URNs so two deployments' check 42 do
not collide. The inference path was also using it as the **warehouse's** platform
instance when building source dataset URNs, which produced
`snowflake,acme.SALES.PUBLIC.ORDERS` — a dataset the customer's Snowflake source never
emitted, so every assertion and profile would have landed on nothing.

One field cannot mean both. The warehouse's instance is now
`default_source_platform_instance` (with `default_source_env` alongside), defaulting to
**none**, which matches the common case of a warehouse ingested without an instance.
Two tests pin the separation. Note montecarlo has the same latent ambiguity — its
inference path uses the top-level instance too — which is worth mentioning in the
upstream PR.

**Phase 9 — docs — ✅ DONE.** `docs/sources/qualytics/` now carries the per-capability
Required Permissions section the standards mandate, a fully commented recipe, the
report-counter reference, and troubleshooting keyed to the failure modes that actually
occur. `tests/unit/test_docs.py` parses the shipped recipe against the real config
class and fails if a capability or an emit toggle is undocumented, so the docs cannot
drift silently.

**Golden files — ✅ DONE.** `tests/unit/qualytics/golden/qualytics_mces_golden.json`: 27 events, 32KB,
three platforms, generated from a deliberately broad synthetic deployment
(`tests/unit/qualytics/fixtures/golden_deployment.json`) covering all three store types, tables/views/
files, mapped and inferred platforms, passing and failing checks, a mapped and an
unmapped rule type, and a never-profiled container. Deterministic across runs.

### Correction, recorded during Phase 9: five toggles that did nothing

The docs drift test found `emit_platform` in the config but not in the recipe, and
checking why showed it was never read. Auditing the rest: **`emit_platform`,
`emit_ownership`, `emit_lineage`, `emit_incidents` and `emit_tags` were all dead** —
`emit_tags` was even probed in `test_connection`, so the connector claimed a TAGS
capability it never exercised.

All five are removed, along with the `TAGS` capability decorator. The standards are
explicit that a half-implemented option is worse than an absent one, and a user setting
`emit_tags: true` and getting no tags has no way to tell whether the feature is broken
or their token is.

Tags in particular need a design decision before returning. `globalTags` is a
full-replacement aspect, so writing Qualytics tags onto a dataset the warehouse source
owns would wipe its other tags — the same clobbering problem that keeps `schemaMetadata`
out. The likely answer is to tag the **assertions**, which this connector does own.

A side effect worth noting: with incidents, structured properties and tags all
unemitted, the push/pull aspect-ownership split now holds with no configuration at all. The
push integration owns them by default because nothing here competes.

**Phase 10 — `/datahub-connector-pr-review` — ✅ DONE.** Findings and their fixes in
`skill_docs/_datahub-connector-pr-review-2026-10-06.md`.

**Phase 11 — live verification — ✅ DONE**, against a Qualytics development deployment
and a self-hosted DataHub, including coexistence with the push integration. Five bugs
found and fixed; see `_datahub-connector-pr-review-2026-10-06.md`.

**Phase 12 — upstream PR.**

Phase 3 is already done (Phase 0 built the config and source skeleton). Phase 8 (lineage,
ownership) drops out of v1.

---

## Approval

- Approved 2026-09-09 via the planning skill's scope questions:
  - **v1 scope**: assertions + results + profiles
  - **Map key**: accept either datastore name or id
  - **Result history**: windowed, 30-day default, via `BaseTimeWindowConfig`
