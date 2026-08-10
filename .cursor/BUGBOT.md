# DataHub Bugbot rules

Project-specific review guidance for Cursor Bugbot. Derived from AGENTS.md /
CLAUDE.md conventions and recurring High/Critical human review findings.
Prefer correctness, security, and broken follow-through over style.
Cap pure style/DX comments; prefer dropping unverified Highs over speculative blockers.

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

## Assertion operators

If `AssertionStdOperator` (PDL) or operator evaluation/display semantics change,
then:
- Flag missing sync across executor (`types.py`, evaluator), frontend labels /
  builder / result messages, and focused tests.
- Title: "Assertion operator follow-through"

## Elasticsearch / OpenSearch

If a query filters or aggregates on a URN / ID field using the analyzed mapping
(e.g. `entityUrn` without `.keyword`), then:
- Blocking bug. URNs tokenize on `:` and prefixes silently match nothing.
- Bad: `term`/`terms` on `entityUrn`. Good: `entityUrn.keyword` (or keyword-mapped field).
- Title: "Use .keyword for URN/ID filters"
- Label: correctness

## Time units

If code compares timestamps, TTLs, expire/purge thresholds, or durations across
APIs, then:
- Check millisecond vs second consistency (`reportedAt`, Kafka timestamps, sweeper
  thresholds, billing windows).
- Example: expire/purge in seconds vs `reportedAt` in ms fails closed.
- Flag unit mismatches as High/Critical.

## Auth between services

If code forwards credentials between GMS, frontend, MCP, or integrations, then:
- Verify the receiver's accepted scheme (Bearer vs Basic vs session).
- Example: GMS `Basic` toward MCP `/public/mcp` that only accepts Bearer → High.
- Flag scheme mismatches as High security/correctness.

## Concurrency / overwrite

If a tool or API replaces a whole collection/aspect (remediations, settings,
relationship info) without merging or checking existing user edits, then:
- Flag last-write-wins / silent overwrite risk.
- Title: "Destructive full replace"
- Prefer High when user-authored state can be lost.

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
