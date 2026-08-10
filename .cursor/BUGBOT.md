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

If GraphQL/aspect fields are renamed, then:
- Flag missing dual-read / mapper backfill for historical rows still on the old shape.

If a Java/TS constructor, fixture, CLI flag, or `test.use` signature changes, then:
- Flag unupdated callers and tests as High — stale signatures often fail compile
  or CI only after merge.

If a feature flag or behavior toggle is introduced, then:
- Prefer Spring `application.yaml` / typed config over raw environment variables
  for GMS/backend flags.
- Flag new `System.getenv` / process-env feature switches in Java services.

If `FeatureFlags.java` gains a field, then:
- Flag missing `scripts/dev/datahub-dev.sh sync-flags` / flag-classification refresh.

If validation is added only in a GraphQL resolver, Rest controller, or OpenAPI
handler, then:
- Flag it. Validation must live in an `AspectPayloadValidator` registered in
  `SpringStandardPluginConfiguration` so all APIs are protected.

If resolvers throw on auth failure, then:
- Prefer naming the missing privilege in the error (Medium–High for admin mutations).

## Assertion operators

If `AssertionStdOperator` (PDL) or operator evaluation/display semantics change,
then:
- Flag missing sync across executor (`types.py`, evaluator), frontend labels /
  builder / result messages, and focused tests.
- Title: "Assertion operator follow-through"

## Elasticsearch / OpenSearch

If a query filters or aggregates on a URN / ID field using the analyzed mapping
(e.g. `entityUrn` without `.keyword`), then:
- Blocking. URNs tokenize on `:`; prefixes/terms silently match nothing in prod.
- Bad: `{"prefix": {"entityUrn": "urn:li:..."}}` or `term` on `entityUrn`
- Good: `entityUrn.keyword` (or an explicitly keyword-mapped field)
- Title: "Use .keyword for URN/ID filters"
- Label: correctness

If a query scans a broad event/index and comments claim client-side filtering,
then:
- High. Push the filter into ES (prefix/term on `.keyword`) instead of pulling
  unmatched hits and filtering in Python/Java.

## Time units

If code compares timestamps, TTLs, expire/purge thresholds, or durations across
APIs, then:
- Check ms vs seconds (`reportedAt`, Kafka timestamps, sweeper thresholds).
- Bad: `time.time()` / seconds threshold compared to ms `reportedAt` (always false).
- Flag unit mismatches as High/Critical.

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
- Token/credential acquisition must be inside the same guarded normalization
  path as the remote call (not before `try`).

## System upgrade / bootstrap steps

If an `UpgradeStep` or bootstrap job deletes search documents or mutates
cluster state, then:
- Flag missing completion marker so the step becomes skippable after success.
- High when deletes are at-most-once / non-idempotent and the step would re-run.

## Multi-surface fixes

If a bugfix touches identifier quoting, casing, escaping, or auth scheme in
one layer (executor / UI / GMS) only, then:
- Flag missing twin fixes in other emitters of the same identifiers
  (ingestion, parsers, authoring UI).

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


## Frontend

If new code calls `crypto.randomUUID()` directly in browser code, then:
- High: fails outside secure contexts. Use `mintRequestId` / agentChatUtils.

If styled UI introduces hardcoded hex colors, `REDESIGN_COLORS`, `ANTD_GRAY`, or
raw alchemy palette imports, then:
- Non-blocking note: use semantic theme tokens from `colorThemes/types.ts`.

## Deprecated test stacks

If a PR adds or extends tests under `smoke-test/tests/cypress/`, then:
- Flag: Cypress is deprecated; new UI automation must use Playwright.

## Confidentiality

Never suggest or invent customer-identifiable names, hostnames, account IDs, or
ticket IDs in review comments. Prefer generic placeholders.
