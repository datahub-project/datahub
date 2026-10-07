# Full Review: Qualytics

**Connector Type:** API (Qualytics REST)
**Base Class:** `StatefulIngestionSourceBase`, `TestableSource`
**Review Date:** 2026-10-06
**Review Mode:** Full Review — re-review before submission to DataHub
**Scope:** `src/datahub/ingestion/source/qualytics/`, `tests/unit/qualytics/`, `docs/sources/qualytics/`

Produced with `/datahub-connector-pr-review`. Four review agents ran in parallel —
silent failures, test quality, type design, simplification/performance — alongside a
manual systematic pass. The `pr-review-toolkit` agent types were not installed, so
general-purpose agents ran the same specialized prompts. Comment resolution does not
apply: there is no upstream PR yet.

**Every finding below was reproduced before it was accepted** — by a test, a script, or
a full `Pipeline` run — and every fix has a regression test that fails when the fix is
reverted. Findings that could not be reproduced were dropped.

The previous review (2026-09-10) and the live verification that followed are
summarised under [History](#history).

---

## Summary

| Category      | Status | Issues                                                       |
| ------------- | ------ | ------------------------------------------------------------ |
| Architecture  | ✅     | 2 blockers fixed (duplicate stale removal, casing processor) |
| Code Quality  | ✅     | 1 blocker, 7 warnings fixed                                  |
| Performance   | ✅     | 1 warning fixed; batching measured and declined              |
| Test Quality  | ✅     | 1 blocker, 5 warnings fixed                                  |
| Documentation | ✅     | Stale comments and descriptions corrected                    |

**Legend:** ✅ Pass | ⚠️ Warnings | ❌ Issues Found

---

## Critical Issues (Blockers) — all fixed

### 1. A transient API error soft-deleted that container's assertions

- **Location:** `source.py` — the datastore, container and per-item parse handlers
- **Standard:** `standards/patterns.md` — API failures are failures
- **Issue:** a 500 on one container's quality-check listing was reported as a warning.
  DataHub's stale-entity handler stands down only when the source reports a _failure_,
  so the next commit soft-deleted every assertion on that container. Reproduced with
  two real `Pipeline` runs against one state file. The same applied to a failed
  container listing and to a datastore, container or check whose payload would not
  parse.
- **Fix:** anything that leaves the run's set of assertions incomplete is now a
  `failure`, and the handler carries the previous state forward. Problems that cannot
  change which assertions exist — profiles, anomaly listings, dataset-existence checks
  — stay warnings, each in its own handler. Upstream Monte Carlo makes the same call
  for the same reason (`montecarlo/source.py`, "Partial-failure guard").
  `test_stateful.py` pins it end to end.

### 2. A `null` the spec allows dropped the whole object

- **Location:** `models.py` — `fields`, `histogram_buckets`, `failed_checks`, `global_tags`
- **Issue:** the spec marks these lists nullable; the models typed them `list[...]`
  with a default, which is optional but still rejects an explicit `null`. A datastore
  with `global_tags: null` was skipped with all its containers; a check with
  `fields: null` lost its assertion. The spec-alignment test checked `required` only.
- **Fix:** models trimmed to the fields the connector reads (an unread field is only
  another way to fail validation — several unread fields were _required_), and the
  read lists are a `NullableList` that reads `null` as empty.
  `test_model_spec_alignment.py` now also asserts every model accepts `null` wherever
  its spec schema allows it, and covers the schemas embedded in anomalies
  (`FailedCheckListing`, `QualityCheckListing`), the path of the original incident.

### 3. The framework's lowercasing processor overrode per-datastore casing

- **Location:** `source.py`, `config.py`
- **Issue:** `AutoLowercaseUrnsProcessor` switches on when the _recipe_ sets
  `convert_urns_to_lowercase`, then lowercases every dataset URN in the stream —
  after the resolver had honoured a `datastore_to_platform_map` entry that turned it
  off, and without touching the column paths of the same dataset. Reproduced through a
  real `Pipeline`; a bare `PipelineContext` does not exercise the processor at all.
- **Fix:** the processor is excluded (`get_excluded_workunit_processors`); the
  resolver owns casing. The config no longer inherits `LowerCaseDatasetUrnConfigMixin`,
  so the field is genuinely optional — "unset" means "follow each platform's own
  source", which previously depended on `model_fields_set` and silently changed
  meaning when a recipe stated the default explicitly.

### 4. Stale-entity removal ran twice

- **Location:** `source.py`
- **Issue:** DataHub 1.7 adds `AutoStaleEntityRemovalProcessor` to every stateful
  source. The source also built its own `StaleEntityRemovalHandler` and appended it.
- **Fix:** the hand-built handler is gone, matching upstream Monte Carlo. The
  `acryl-datahub` floor is now `>=1.7.0`. `test_stateful.py` asserts each stale
  assertion is removed exactly once and that no dataset ever is.

### 5. Tests the standard rejects as trivial

- **Location:** `test_models.py`, `test_assertion.py`, `test_docs.py`, `test_client.py`
- **Standard:** `standards/testing.md` — no trivial tests
- **Issue:** a test that pydantic parses a model nothing used; a test that the
  assertion type is `"qualytics"`; a spec rule-count canary implied by the set-equality
  tests beside it; a test that the recipe's type is `qualytics`; a test of pydantic's
  `ge=1`; and pass-through tests that an input comes back unchanged.
- **Fix:** deleted, including all of `test_models.py` — its coverage survives
  elsewhere (breaking the `schema` alias still fails 12 tests).

---

## Important Issues — fixed

| Finding                                                                                                                                                 | Fix                                                                                             |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Any 404, or a non-object body, from the profile endpoint was counted as "never profiled", so a missing route looked like a tenant with no profiles      | Only Qualytics' own "has not been profiled" 404 returns None; anything else is a failed profile |
| An existence-check failure was cached as "dataset missing", and an outage warned and timed out once per dataset                                         | Reported once, counted as `profiles_existence_check_failed`, not retried for the run            |
| Anything other than an API or validation error inside one container aborted every remaining datastore (`int(inf)` from a `1e400` distinct count did it) | Container boundary catches broadly as a failure; `math.isfinite` before `int()`                 |
| An anomaly-listing failure marked the container "skipped" after its assertions had been emitted                                                         | Own handler, `anomaly_listings_failed`, warning                                                 |
| An unknown rule type warned once per _check_                                                                                                            | Once per rule type; `unmapped_rule_types` is a `LossySet`                                       |
| A mistyped `datastore_to_platform_map` key was silent                                                                                                   | Warned once per unmatched key, counting filtered datastores as matched                          |
| A denied datastore or container of an unknown type still warned                                                                                         | Patterns applied before the type check                                                          |
| `logic` typed `Any` reached a `str` aspect field; a non-string failed only at serialisation                                                             | Coerced, with a `validate()` test                                                               |
| `platform_instance` and `env` carried the mixins' generic descriptions, though here they define assertion identity and a fallback env                   | Redeclared with connector-specific descriptions                                                 |
| `bucket_duration` documented though it does nothing here                                                                                                | Hidden from docs; window fields describe day granularity                                        |
| The ~3 MB spec was cached for the whole run to read one version string                                                                                  | Not cached                                                                                      |
| `PLANNED_PATHS` and ten constants for unbuilt features shipped in `constants.py`                                                                        | Removed; fixture trimmed to the 7 consumed paths                                                |
| Golden test bypassed the processor chain, so it was not what a pipeline writes                                                                          | Uses `get_workunits()`; golden now includes the assertion `status` aspects (56 events)          |
| Contract tests skipped silently if the fixture was missing, and their paths broke on the port                                                           | Fail instead of skip; fixtures beside the tests; docs located by search                         |
| Comments that were stale or narrated bug history                                                                                                        | Corrected or removed                                                                            |

---

## Deliberate deviations — assessed

- **MCPs, not SDK V2 entities.** Justified. The installed SDK has no Assertion entity,
  `sdk.Dataset` has no profile setter, and its assertion client writes through the
  DataHub client directly, bypassing workunits, stateful ingestion and file sinks.
  The profiles also attach to datasets other sources own; constructing an SDK
  `Dataset` would write aspects onto someone else's entity. `profile.py` documents
  this.
- **No `schemaMetadata`.** Justified: the warehouse source owns the schema, and
  Qualytics' view of it is a subset. The golden checklist's empty schema row is by
  design.
- **No docker integration test.** Justified: Qualytics has no public sandbox. The
  substitute is a replayed synthetic deployment (the shape of upstream Monte Carlo's
  test) plus contract tests against a slice of a real deployment's OpenAPI spec —
  paths, query parameters including enums, and model required/nullable alignment —
  and the live verification below.
- **`SupportStatus.BETA`.** A judgement for DataHub's maintainers. Their definitions
  tie BETA and GA to sources "maintained by the DataHub Ingestion team"; vendor-built
  connectors such as Monte Carlo ship as ALPHA. Qualytics maintains this connector.
  Raise it explicitly in the PR rather than leave it to be found.

---

## Suggestions (not actioned)

- **Skip the anomaly listing for a container whose checks all report `anomaly_count:
0`.** Saves one call per clean container without buffering. Needs `anomaly_count`
  optional so an older deployment that omits it does not lose history.
- **Keyword-only URN arguments on the mapper entry points.** `check_state_workunits`
  takes two adjacent `str` URNs that mypy would accept swapped.
- **Reject contradictory config:** `verify_ssl: false` with `ca_cert_path` still
  verifies; `emit_assertion_results` without `emit_assertions` emits nothing (now
  documented).
- **Map entries do not inherit the recipe's `env`** (they default to PROD, as in Monte
  Carlo). Documented; changing it would move existing URNs.
- **Validate the golden fixture's payloads against the spec.** They are invented, and
  every type is missing some required spec field the models do not read.
- **Validate parameters from recorded traffic** rather than hand-picked calls, which
  would also cover `$ref` enums.
- **`capture_openapi.py` assumes the `/api` root**; `client.detect_api_root_path()`
  already solves it.

---

## Positive Observations

- **Spec-as-contract testing built from real incidents** — paths, query parameters and
  model strictness are all checked against a captured spec slice, and each check exists
  because a mock-based suite once passed while a live deployment failed.
- **Every `report.warning`/`failure` call obeys the `LiteralString` rule**, with
  dynamic values in `context` and `exc=e` in every handler.
- **Never owns what it enriches:** `is_primary_source=False` on profiles, profiles
  only for datasets that already exist, the failure-before-delete rule, and a
  regression test for each.
- **`mypy --strict` clean with no `cast()`**; the one `# type: ignore` is specific and
  documented, and five upstream sources need the same one.
- **Fully streaming** — paging, parsing and emission are generators throughout; the
  only collections are per container.
- **Actionable errors** that name the object, the reason and the fix, including the
  `/api` root-path diagnosis in `test_connection`.

---

## Checklist Results

### Architecture

- [x] Correct base class (`StatefulIngestionSourceBase`, `TestableSource`)
- [x] SDK V2 usage — deviation assessed above
- [x] Proper config structure — separate `config.py`, `QualyticsPlatformDetail` mirrors Monte Carlo
- [x] File organization per `standards/patterns.md`

### Code Quality

- [x] Type hints on all public methods
- [x] No `type: ignore` without justification
- [x] Uses `isinstance()` over `cast()`
- [x] Proper error handling

### Performance

- [x] Uses generators (`yield`) for workunit emission
- [x] N+1: ~3.2 calls per container, measured; batching saves ~a fifth, declined
- [x] Pagination implemented for every list endpoint
- [x] HTTP session reuse, with retries honouring `Retry-After`

### Testing

- [x] Unit tests exist and are meaningful — 247 tests, 97% coverage
- [x] Golden file through the full processor chain
- [x] Golden file >5 KB, >20 events — 56 events
- [x] Tests are non-trivial — trivial ones removed

### Documentation

- [x] Config options documented, with connector-specific descriptions
- [x] Annotated example recipe
- [x] Known limitations, failure semantics and troubleshooting documented

---

## Quality Score

| Aspect               | Score    | Notes                                                |
| -------------------- | -------- | ---------------------------------------------------- |
| Standards Compliance | 9/10     | BETA status is the open question                     |
| Test Coverage        | 9/10     | 97%; golden fixture is synthetic, not spec-validated |
| Code Quality         | 9/10     |                                                      |
| Documentation        | 9/10     |                                                      |
| **Overall**          | **9/10** |                                                      |

---

## Verdict

**APPROVED for submission** — all blockers and warnings fixed, each with a regression
test.

### Before the upstream PR

1. Raise the support status with DataHub's maintainers (BETA requested; their
   definitions point to ALPHA for vendor-maintained sources).
2. File the `connector_registry` packaging bug upstream as its own small PR: the
   registry is absent from released `acryl-datahub` wheels (still true in 1.7.0.10),
   which silently disables platform-name validation for every pip user.
3. Port notes: add `__init__.py` to `tests/unit/qualytics/`; drop the local
   `tests/conftest.py`, whose `pytest_plugins` line upstream's root conftest already
   provides; register the platform in `data-platforms.yaml` with the logo.
4. Offer DataHub's reviewers a partner Qualytics tenant, so they can run the connector
   against real data.

---

## History

### Review of 2026-09-10 — 9 blockers, all fixed

1. Every quality-check and anomaly listing 422'd on `archived=False` — guarded since by
   `test_api_params.py`, which checks outgoing parameters against the spec.
2. Profiles were primary-source, so stale removal could soft-delete the customer's
   warehouse datasets.
3. A malformed page envelope produced a clean, empty, "successful" run.
4. One malformed record aborted the whole run.
5. A paging failure escaped its handler and aborted the run.
6. A rejected token was swallowed by the version probe.
7. A `max_workers` toggle did nothing — the sixth dead toggle removed.
8. Unvalidated values fed straight into URNs (`default_source_env`, `ui_base_url`).
9. Late imports and vacuous tests.

Accepted at the time and since closed: list-valued parameters are capped at 100 items;
the three mappers share one entry-point shape; the per-container call count was
measured and kept.

### Live verification, 2026-10-01

Against a Qualytics development deployment and a self-hosted DataHub 1.7, including
the Qualytics push integration writing to the same DataHub. It found five bugs no mock
could, all fixed with regression tests: anomaly-embedded quality-check fields omit
`id`, which the model required; unprofiled containers answer 404, not an empty body;
Snowflake dataset names, and separately column paths, were not lowercased as
DataHub's Snowflake source lowercases them; and a profile written for a table DataHub
had not ingested created a stub dataset. It also confirmed the profile fix from the
2026-09-10 review end to end: narrowing scope soft-deleted exactly the dropped
container's assertions and no dataset. Timings: 229 containers, 7,018 checks and 6,980
anomalies in 7m27s, ~3.2 requests per container, no rate limiting, 351 MiB peak.

---

_Review generated by DataHub Connector PR Review Skill_
