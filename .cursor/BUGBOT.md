# DataHub Bugbot rules

Project-specific review guidance for Cursor Bugbot. Derived from AGENTS.md /
CLAUDE.md conventions, gold High/Critical misses, and historical human review
themes. Prefer correctness, security, and broken follow-through over style.
Cap pure style/DX comments; prefer dropping unverified Highs over speculative blockers.
Sharpen triggers with concrete bad/good snippets — vague themes often fail to fire.

## General

If a change alters a public API, GraphQL schema, PDL aspect, CLI flag, or default
config, then:

- Flag missing updates to callers, tests, smoke tests, Playwright fixtures, and
  docs (especially `docs/how/updating-datahub.md` for breaking changes).
- Title: "Follow-through gap"
- Prefer High when tests or defaults would break at runtime.

If a change deletes or renames a PDL / GraphQL / config field that may already
exist in stored metadata, then:

- Flag deletion without deprecation or migration / dual-read.
- Body: "Deprecate instead of deleting; add migration if renaming stored aspects."
- Label: compatibility
- Do not emit a second comment for the same rename under a separate "GraphQL/aspect
  fields renamed" heading — this rule already covers dual-read / backfill.

If a Java/TS constructor, fixture, CLI flag, or `test.use` signature changes, then:

- Flag unupdated callers and tests as High — stale signatures often fail compile
  or CI only after merge.

If a feature flag or behavior toggle is introduced, then:

- Prefer Spring `application.yaml` / typed config over raw environment variables
  for GMS/backend flags.
- Flag new `System.getenv` / process-env feature switches in Java services.

If `FeatureFlags.java` gains a field, then:

- Flag missing `scripts/dev/datahub-dev.sh sync-flags` / flag-classification refresh.

If validation of a **persisted aspect payload** is added only in a GraphQL
resolver, Rest controller, or OpenAPI handler, then:

- Flag it. Aspect payload validation must live in an `AspectPayloadValidator`
  registered in `SpringStandardPluginConfiguration` so GraphQL, OpenAPI, and
  Rest.li are all protected.
- Do not flag ordinary request/DTO validation that is not an aspect write
  (authz checks, query-arg bounds, transport-only shapes).

If resolvers throw on auth failure, then:

- Prefer naming the missing privilege in the error (Medium–High for admin mutations).

## Config property classification

If a PR adds a new `@ConfigurationProperties` field, env-backed setting, or other
config property that can surface via system info (often under `metadata-service/`
or `metadata-io/`), then:

- Flag missing classification in
  `PropertiesCollectorConfigurationTest` (sensitive vs non-sensitive lists /
  templates).
- High security when a secret-like key would be visible in system-info APIs.
- Classification lists do **not** drive runtime redaction. For secrets, also
  verify `PropertiesCollector` redacts the key (name matches an existing
  `SENSITIVE_PATTERNS` keyword, or extend those patterns) — do not stop at the
  test list alone.
- Title: "Classify new config property"

## Assertion operators

If `AssertionStdOperator` (PDL) or operator evaluation/display semantics change,
then:

- Flag missing sync across executor (`types.py`, evaluator), frontend labels /
  builder / result messages, and focused tests.
- Title: "Assertion operator follow-through"

## Elasticsearch / OpenSearch

If a query filters or aggregates on a URN / ID field using the analyzed mapping
(e.g. `entityUrn` without `.keyword`), then:

- Blocking. Analyzed URN fields lose exact full-URN semantics (`:` tokenization);
  prefixes/terms may match nothing or match unintended tokens.
- Bad: `{"prefix": {"entityUrn": "urn:li:..."}}` or `term` on analyzed `entityUrn`
- Good: `entityUrn.keyword` **or** a field that is already mapped as `keyword`
  (do not append `.keyword` to an already-keyword field).
- Title: "Use keyword mapping for URN/ID filters"
- Label: correctness

If a query scans a broad event/index and filters hits in application code, then:

- High only when an equivalent indexed keyword/exact field exists to push the
  filter into ES (prefix/term on that field). Do not demand ES pushdown for values
  that exist only in an unindexed payload or need app-level decoding.

## Time units

If code compares timestamps, TTLs, expire/purge thresholds, or durations across
APIs, then:

- Check ms vs seconds (`reportedAt`, Kafka timestamps, sweeper thresholds).
- Bad: `time.time()` (seconds) used in a `<` / `>` expire check against ms
  `reportedAt` (comparison is invalid / always wrong for the intended window).
- Flag confirmed unit mismatches as High/Critical — do not assume every
  seconds-vs-ms pairing is false without checking operator and operand order.

## Auth between services

If code forwards credentials between GMS, frontend, MCP, or integrations, then:

- Verify the receiver's accepted scheme (Bearer vs Basic vs session).
- Example: GMS `Basic` toward MCP `/public/mcp` that only accepts Bearer → High.
- Flag scheme mismatches as High security/correctness.

## Concurrency / overwrite / write-path parity

If a tool or API replaces a whole collection/aspect without merging or checking
existing user edits, then:

- Flag last-write-wins / silent overwrite. Prefer a code guard, not prompt text.
- Title: "Destructive full replace"
- Prefer High when user-authored state can be lost.

If agent/automation code writes a different field than the UI for the same user
edit surface, then:

- High inconsistency / data-loss risk. Align storage paths.

If one thread writes multi-field progress/health state read by another without
synchronization, then:

- Flag race; use a lock or atomic snapshot.

## Batch job failure scope

If a backfill, embedding loader, or bulk processor aborts the entire run on
non-retryable errors, then:

- Distinguish systemic failures (auth, wrong model/dimensions, bad credentials)
  from per-item failures (oversized/poison document).
- Per-item: record + continue. Systemic: fail fast.
- Token/credential **acquisition** failures are systemic: fail the run (or gate
  once at run start). Do not put acquisition in a per-item catch/continue path
  that retries tokens for every item or normalizes an auth outage as item
  failures. Remote-call exceptions for individual docs stay per-item.

## System upgrade / bootstrap steps

If an `UpgradeStep` or bootstrap job deletes search documents or mutates
cluster state in a non-idempotent way, then:

- Flag missing completion marker so the step becomes skippable after success.
- High when deletes are at-most-once and the step would re-run unsafely.
- Do not require `skip()` after first success for intentional per-deploy
  reconciliation steps that deliberately re-run.

If a PR touches `CleanIndicesStep` (runtime id `CleanUpIndicesStep`),
`IncrementalReindexCatchUpStep`, or ZDU step ordering, then:

- High / Critical when cleanup can delete ES backing indices still needed by
  incomplete catch-up (alias-less ≠ orphan during ZDU).

If a PR bumps an existing bootstrap MCP file version (templates, defaults that
write org/user state with UPSERT/overwrite semantics), then:

- High overwrite risk. Prefer a new bootstrap file or idempotent/migration guard.
- Do not flag CREATE-only bootstrap MCPs (e.g. templates that reject when the
  entity already exists) as overwrite risks solely due to a version bump.

## User status / eligibility

If a PR adds or changes login, auth eligibility, or "is user active" checks, then:

- High. Use `corpUserStatus` (ACTIVE/…); do not read deprecated
  `corpUserInfo.active`.

## Masked secrets on save

If a PR adds/edits connection or integration save flows (GraphQL mutation + form
state), then:

- High when masked/obfuscated secret fields from a read query are sent unchanged
  in an upsert without an "unchanged secret" sentinel or server-side merge.
- Bad: persisting display values like `4a****556` as the real secret.

## Kafka consumer failure paths

If a PR handles aspect/MCL **producer** emission failures, then:

- Prefer reject/propagate so writers know the write failed; do not treat an
  unqualified drop as success.

If a PR handles Kafka **consumer** error handling for aspect/MCL records, then:

- High when `RecordTooLargeException` / oversized aspects crash the consumer
  instead of DLQ, quarantine, or validation reject.
- Per-item poison records must not crash-loop the pod.

## Multi-surface fixes

If a bug fix touches identifier quoting, casing, escaping, or auth scheme in
one layer (executor / UI / GMS) only, then:

- First check whether another emitter duplicates the same transformation or
  scheme. Flag missing twin fixes only when a peer path shares the buggy
  behavior — do not speculate about unrelated emitters.

## Competing key / side-index schemes

If a PR introduces a new platform-resource or side-index key format, then:

- Flag leftover competing ID classes, resource_types, or tests still using the
  old scheme.

## Defaults-unchanged claims

If a PR claims unset config preserves prior behavior, then:

- Diff default templates/copy/assets; flag default-path visual or behavioral
  changes without an explicit override.

## SDK / API destructive defaults

If a public method deletes or replaces subscriptions/policies and takes an
optional actor/scope, then:

- High when omitted scope means "everyone". Require identity or a named
  destructive API.

## Default routes vs flags/privileges

If default nav/redirect targets change, then:

- Flag targets that can be feature-flagged off or privilege-gated (blank landing).

## Packaging / logging

If a published package (`setup.py` / `pyproject.toml` / plugin wheel) introduces
an editable (`-e`) or path dependency on local `metadata-ingestion`, then:

- Flag it. Prefer a pinned released version.
- Label: packaging
- Do not flag first-party image builds under `docker/datahub-ingestion` or
  `docker/datahub-actions` that intentionally COPY + editable-install the
  checkout (those stamp `RELEASE_VERSION` at build time).

If Logback XML is added/changed under `metadata-jobs/*/src/main/resources`, then:

- Medium note only when runtime clearly loads an external/helm Logback config
  that ignores the jar resource. Do not blanket-reject every
  `src/main/resources/logback.xml` change — MCE/MAE images often ship and use
  the jar's Logback file.

## Search pagination

If new search/ops code deep-pages Elasticsearch with `from`/`size` (especially
beyond shallow UI pages), then:

- Flag. Prefer `scroll` / `search_after` / existing `scrollAcrossEntities` helpers.

## Error propagation

If a user-visible API, write path, or authz decision swallows failures as
best-effort/logged-only (catch + log / return null / empty success), then:

- Flag when the caller cannot see or handle the failure.
- Prefer propagate, or explicitly document intentional fire-and-forget.
- Do not flag metrics, cache warmers, or secondary enrichers that already
  document best-effort behavior.

## Retries

If production HTTP/Kafka shared clients add reconnect/retry loops, then:

- Flag hard-coded retry counts/delays; prefer config.
- Document which failures are retried and terminal behavior after exhaustion.
- Skip tests, one-off scripts, and short fixed retries in local tooling.

## MCL / index batching

If a PR parallelizes MCL or search-index updates across entities that share
aspects (or otherwise batches conflicting default-aspect writes), then:

- Flag conflict/deadlock scenarios across batches.
- Require an explicit retry/idempotency story.

## Null / empty sentinels

If UI/API uses empty string as a missing URN/id sentinel, then:

- Medium by default (prefer null/undefined/Optional.empty; empty string hides
  bugs). Raise to High only when a demonstrated contract breaks (write/read path
  treats `""` as a real URN or drops required identity).

If new code reads DB/search rows and dereferences URN/id fields without a null
guard on a path that historically sees corrupt/partial rows, then:

- Medium. Flag NPE risk; do not demand guards on every field access.

## Frontend

If new code calls `crypto.randomUUID()` directly in browser code, then:

- High: fails outside secure contexts. Use the `uuid` package (`import { v4 as
uuidv4 } from 'uuid'`), matching existing frontend usage.

If styled UI introduces hardcoded hex colors, `REDESIGN_COLORS`, `ANTD_GRAY`, or
raw alchemy palette imports, then:

- Non-blocking note: use semantic theme tokens from `colorThemes/types.ts`.

## Confidentiality

Never suggest or invent customer-identifiable names, hostnames, account IDs, or
ticket IDs in review comments. Prefer generic placeholders.
