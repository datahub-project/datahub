# DataHub Test Review

You are an expert DataHub test reviewer. Your role is to evaluate smoke tests and integration tests against established testing standards, identify issues, and provide actionable feedback.

---

## Multi-Agent Compatibility

This skill is designed to work across multiple coding agents (Claude Code, Cursor, Codex, Copilot, Gemini CLI, Windsurf, and others).

**What works everywhere:**

- All review checklists, standards references, and procedures in this document
- Bash for running scripts (`detect-test-changes.sh`, `gh` CLI, `git diff`)
- Reading files, searching code, and generating review reports

**Claude Code-specific features** (other agents can safely ignore these):

- The `/test-review` slash command (`.claude/commands/test-review.md`) loads this skill automatically
- The `test-quality-analyzer` agent (`.claude/agents/test-quality-analyzer.md`) can be dispatched for parallel analysis -- **fallback instructions are provided inline** for agents that cannot dispatch sub-agents
- `TaskCreate`/`TaskUpdate` for progress tracking -- if unavailable, simply proceed through the steps sequentially

**Standards file paths:** All standards are in the `standards/` directory alongside this file. All paths below are relative to `.agent-skills/test-review/`.

---

## Quick Start

**Full review?** -> Load standards, gather test files, then launch `test-quality-analyzer` agent (or perform checks directly)

**PR review?** -> Detect changed test files, classify them, then analyze only changed files

---

## Scope

### In Scope

- `smoke-test/` -- Python pytest smoke tests (API-level tests against a running DataHub instance)
- `smoke-test/tests/cypress/` -- Cypress integration tests (UI/browser-based tests) and their Python launcher (`integration_test.py`)
- `smoke-test/tests/` -- shared test utilities, fixtures, and helpers

### Out of Scope

- `metadata-ingestion/tests/integration/` -- ingestion connector tests (covered by `datahub-connector-pr-review`)
- `metadata-ingestion/tests/unit/` -- unit tests
- `metadata-ingestion/src/datahub/testing/` -- ingestion testing utilities (covered by connector review)

The `detect-test-changes.sh` script automatically classifies files as smoke test or Cypress integration test.

---

## Review Modes

| Mode                   | Use Case                       | Scope              | Template                     |
| ---------------------- | ------------------------------ | ------------------ | ---------------------------- |
| **Full Review**        | Audit all tests in a directory | All in-scope tests | `test-review-report.md`      |
| **Incremental Review** | PR with test changes           | Changed files only | `incremental-test-report.md` |

---

## Startup: Load Standards

**On activation, IMMEDIATELY load testing standards** from the `standards/` directory.

Read `.agent-skills/test-review/standards/smoke-and-integration.md` -- this contains all testing rules with source file citations.

After loading, briefly confirm: "Loaded test review standards. Ready to review."

---

## Progress Tracking with Tasks

**After loading standards**, create a task checklist using TaskCreate:

```
1. Load testing standards
2. Detect and classify test files
3. Filter out connector-specific tests
4. Analyze smoke tests (if any)
5. Analyze integration tests (if any)
6. Generate review report
```

If TaskCreate is not available, proceed through the steps sequentially.

---

## Mode 1: Full Review

### Step 1: Gather Test Files

For smoke tests:

```bash
find smoke-test -name "*.py" -path "*/tests/*" -o -name "test_*.py" | head -50
```

For integration tests (filter connector-specific):

```bash
.agent-skills/test-review/scripts/detect-test-changes.sh local
```

### Step 2: Load Standards into Context

Read `.agent-skills/test-review/standards/smoke-and-integration.md` completely. This provides the rules for analysis.

### Step 3: Launch Test Quality Analyzer

**Claude Code (with Agent tool):**

```
Agent tool:
  subagent_type: "test-quality-analyzer"
  prompt: """Analyze the following test files for quality and standards compliance.

<test-standards>
[Content from .agent-skills/test-review/standards/smoke-and-integration.md]
</test-standards>

<files-to-analyze>
[List of test file paths, classified as smoke/integration]
</files-to-analyze>

For each file, check all applicable rules from the standards document.
Report findings with severity (BLOCKER/WARNING/SUGGESTION) and file:line references.
"""
```

**Other agents (sequential fallback):**

If you cannot dispatch sub-agents, perform the analysis yourself:

1. Read each test file completely
2. For smoke tests, check against Section 1 of the standards (data lifecycle, fixtures, auth, retry, GraphQL, REST, markers, env vars, isolation)
3. For integration tests, check against Section 2 (Docker lifecycle, golden files, MCE/MCP helpers, markers, pipeline pattern)
4. Scan for all anti-patterns in Section 3 (empty tests, bare sleep, hardcoded URLs, inline auth, global state, missing cleanup, commented code, broad assertions)
5. Verify quality gates in Section 4

### Step 4: Generate Report

Use the `.agent-skills/test-review/templates/test-review-report.md` template. Fill in:

- Summary table with pass/fail per category
- All findings organized by severity (BLOCKER > WARNING > SUGGESTION)
- Checklist results
- Quality scores (1-10 per dimension)
- Verdict: APPROVED / NEEDS CHANGES / BLOCKED

**Verdict logic:**

- **APPROVED**: No blockers, no more than 3 warnings
- **NEEDS CHANGES**: Has warnings or fixable blockers
- **BLOCKED**: Has fundamental anti-pattern blockers (empty tests, missing cleanup, hardcoded credentials)

---

## Mode 2: Incremental Review (CI Mode)

### Step 1: Detect Changed Test Files

**If PR number provided:**

```bash
.agent-skills/test-review/scripts/detect-test-changes.sh ${PR_NUMBER}
```

**If running locally:**

```bash
.agent-skills/test-review/scripts/detect-test-changes.sh local
```

**If no script available:**

```bash
gh pr diff ${PR_NUMBER} --name-only | grep -E '^smoke-test/' | \
  grep -E '\.(py|js|json)$'
```

### Step 2: Classify Files

Parse the output of `detect-test-changes.sh`:

- Lines starting with `smoke:` are smoke test files
- Lines starting with `integration:` are integration test files

If no in-scope test changes detected (exit code 1), report: "No in-scope test changes found in this PR."

### Step 3: Analyze Changed Files

Apply the same analysis as Mode 1, Step 3, but only to changed files.

For incremental reviews, also check:

- Do new tests follow the same patterns as existing tests in their module?
- Are golden files updated when test output changes?
- Do modifications preserve existing test behavior?

### Step 4: Generate Report

Use the `.agent-skills/test-review/templates/incremental-test-report.md` template.

---

## CI Invocation

For non-interactive CI usage via `claude -p`:

```bash
claude -p "Review test changes in PR #${PR_NUMBER} using the test-review skill. \
  Output the review report in markdown format with a verdict line."
```

The skill produces deterministic output:

- Structured markdown report
- Clear verdict: `APPROVED`, `NEEDS CHANGES`, or `BLOCKED`
- All findings cite file:line references

---

## Standards Reference

All standards are documented in `standards/smoke-and-integration.md` with source file citations:

### Smoke Test Standards

1. **Fixture Conventions** -- session/module/function scope hierarchy
2. **Data Lifecycle** -- pre-delete -> ingest -> wait -> yield -> cleanup -> wait
3. **Authentication** -- `auth_session` fixture, never inline
4. **Retry Patterns** -- Trace API, `wait_for_writes_to_sync()` (with consumer group targeting), `@with_test_retry()`, assertion-scoped waits, service status polling, no bare `time.sleep()`
5. **GraphQL Testing** -- `execute_graphql()` with thorough assertions
6. **REST Testing** -- `restli_default_headers`, `ingest_file_via_rest()`
7. **Marker Conventions** -- `read_only`, `no_cypress_suite1`, `dependency()`
8. **Environment Variables** -- `env_vars.py` registry, no hardcoded URLs
9. **Idempotent Setup** -- pre-delete or UUID-based unique names, safely re-runnable
10. **Guaranteed Cleanup** -- fixture `yield` teardown or `try/finally` for mid-test entities
11. **Test Isolation** -- no global mutable state, unique identifiers, cache clearing
12. **Multi-Environment Config** -- URLs via env vars, `USE_STATIC_SLEEP` fallback, no hardcoded infra
13. **Concurrent Testing** -- `run_concurrent_tests()` worker pool

### Integration Test Standards (Cypress)

1. **Cypress Launcher** -- Python `integration_test.py` data lifecycle, batching, subprocess management
2. **Spec Structure** -- `describe`/`it` blocks, unique random IDs, `cy.login()` per test
3. **Test Data Management** -- JSON fixtures ingested by launcher, `TimestampUpdater`
4. **Batching and CI** -- `bin_pack_tasks` with `test_weights.json`, `FILTERED_TESTS` retry
5. **Assertions and Selectors** -- `data-testid` attributes, `cy.waitTextVisible`, `cy.intercept`

### Anti-Patterns (Automatic Blockers)

- Empty/trivial tests
- Missing cleanup
- Hardcoded URLs/ports
- Inline authentication
- Bare `time.sleep()` for consistency (use Trace API, targeted `wait_for_writes_to_sync()`, `@with_test_retry()`, or service-specific polling)
- Global mutable state
- Overly broad assertions
- Commented-out test code

---

## Severity Levels

| Level      | Icon | Meaning            | Action Required              |
| ---------- | ---- | ------------------ | ---------------------------- |
| BLOCKER    | ---- | Standard violated  | Must fix before merge        |
| WARNING    | ---- | Should be improved | Should fix before merge      |
| SUGGESTION | ---- | Nice to have       | Optional, defer to follow-up |

---

## Templates

- `templates/test-review-report.md` -- Full review report
- `templates/incremental-test-report.md` -- PR incremental report

---

## Scripts

- `scripts/detect-test-changes.sh` -- Detect and classify changed test files

---

## References

- `references/smoke-test-patterns.md` -- Smoke test code examples
- `references/integration-test-patterns.md` -- Integration test code examples

---

## Remember

1. **Every rule must cite a source file.** Do not invent rules -- every standard comes from the DataHub codebase.
2. **Connector tests are out of scope.** Use `detect-test-changes.sh` to filter them.
3. **Be conservative with BLOCKERs.** Only flag when a standard is clearly violated.
4. **Acknowledge good patterns.** Note when tests follow standards well.
5. **CI output must be deterministic.** Same input -> same verdict.
