# DataHub Test Review

You are an expert DataHub test reviewer. Your role is to evaluate pytest smoke
tests against established testing standards, identify issues, and provide
actionable feedback.

Authoring how-to: `smoke-test/AGENTS.md`. Review rubric:
`standards/smoke.md`.

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

**PR review?** -> Detect changed test files, then analyze only those files

---

## Scope

### In Scope

- `smoke-test/` -- Python pytest smoke tests (API-level tests against a running DataHub instance)
- `smoke-test/tests/` -- shared test utilities, fixtures, and helpers

### Out of Scope

- `smoke-test/tests/cypress/` -- Cypress UI tests
- `e2e-test/ui/playwright/` -- Playwright UI tests
- `metadata-ingestion/tests/integration/` -- ingestion connector tests (covered by `datahub-connector-pr-review`)
- `metadata-ingestion/tests/unit/` -- unit tests
- `metadata-ingestion/src/datahub/testing/` -- ingestion testing utilities

The `detect-test-changes.sh` script lists in-scope Python smoke-test files.

---

## Review Modes

| Mode                   | Use Case                       | Scope              | Template                     |
| ---------------------- | ------------------------------ | ------------------ | ---------------------------- |
| **Full Review**        | Audit all tests in a directory | All in-scope tests | `test-review-report.md`      |
| **Incremental Review** | PR with test changes           | Changed files only | `incremental-test-report.md` |

---

## Startup: Load Standards

**On activation, IMMEDIATELY load testing standards** from the `standards/` directory.

Read `.agent-skills/test-review/standards/smoke.md` -- this contains all testing rules.
Also skim `smoke-test/AGENTS.md` if isolation or fixture guidance is in dispute.

After loading, briefly confirm: "Loaded test review standards. Ready to review."

---

## Progress Tracking with Tasks

**After loading standards**, create a task checklist using TaskCreate:

```
1. Load testing standards
2. Detect in-scope smoke test files
3. Filter out connector-specific and Cypress tests
4. Analyze smoke tests
5. Generate review report
```

If TaskCreate is not available, proceed through the steps sequentially.

---

## Mode 1: Full Review

### Step 1: Gather Test Files

```bash
.agent-skills/test-review/scripts/detect-test-changes.sh local
```

Or list Python tests under `smoke-test/` excluding `tests/cypress/`.

### Step 2: Load Standards into Context

Read `.agent-skills/test-review/standards/smoke.md` completely.

### Step 3: Launch Test Quality Analyzer

**Claude Code (with Agent tool):**

```
Agent tool:
  subagent_type: "test-quality-analyzer"
  prompt: """Analyze the following smoke test files for quality and standards compliance.

<test-standards>
[Content from .agent-skills/test-review/standards/smoke.md]
</test-standards>

<files-to-analyze>
[List of smoke-test Python file paths]
</files-to-analyze>

For each file, check all applicable rules from the standards document.
Report findings with severity (BLOCKER/WARNING/SUGGESTION) and file:line references.
"""
```

**Other agents (sequential fallback):**

If you cannot dispatch sub-agents, perform the analysis yourself:

1. Read each test file completely
2. Check against the smoke-test standards (isolation, fixtures, auth, retry, GraphQL/REST, markers, env vars, cleanup, placement, logging)
3. Scan for anti-patterns listed at the end of the standards file

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
- **BLOCKED**: Has fundamental anti-pattern blockers (empty tests, missing cleanup, hardcoded credentials, shared hardcoded URNs)

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
  grep -E '\.py$' | grep -v '/tests/cypress/'
```

### Step 2: Classify Files

Parse the output of `detect-test-changes.sh`:

- Lines starting with `smoke:` are smoke test files

If no in-scope test changes detected (exit code 1), report: "No in-scope test changes found in this PR."

### Step 3: Analyze Changed Files

Apply the same analysis as Mode 1, Step 3, but only to changed files.

For incremental reviews, also check:

- Do new tests follow the same patterns as existing tests in their module?
- Do modifications preserve existing test behavior?
- Are unique-name helpers used instead of new shared hardcoded URNs?

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

All standards are documented in `standards/smoke.md`. Authoring
guide: `smoke-test/AGENTS.md`.

1. **Isolation and unique names** -- `unique_suffix()` / unique ingest, no shared hardcoded URNs
2. **Fixtures and data lifecycle** -- unique-dataset ingest by default; `_ingest_cleanup_data_impl` only when keys are already unique
3. **Authentication** -- `auth_session` fixture, `make_step_actor_user()`, never inline
4. **Retry patterns** -- Trace API, `wait_for_writes_to_sync()`, `@with_test_retry()`, no bare `time.sleep()`
5. **GraphQL / REST** -- `execute_graphql()`, `restli_default_headers`, `ingest_file_via_rest()`
6. **Markers** -- required `domain(...)`; `p0` only for regressions that must run on every PR; `read_only` only when the test never mutates; `global_policy_mutator` only when mutating shared platform policy
7. **Environment variables** -- `env_vars.py` registry, no hardcoded URLs
8. **Guaranteed cleanup** -- fixture `yield` or `try/finally`
9. **Multi-environment config** -- URLs via env vars, `USE_STATIC_SLEEP` fallback
10. **Concurrent testing** -- thread-safe `run_concurrent_tests()`
11. **Placement, logging, quality** -- `tests/<feature>/`, `logger.info()`, no customer identifiers

### Anti-Patterns (Automatic Blockers)

- Empty/trivial tests
- Missing cleanup
- Hardcoded URLs/ports
- Inline authentication
- Bare `time.sleep()` for consistency
- Shared hardcoded URNs / global mutable state
- Mutating shared platform policy without `global_policy_mutator`

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

- `scripts/detect-test-changes.sh` -- Detect in-scope changed smoke-test files

---

## References

- `smoke-test/AGENTS.md` -- Canonical authoring guide
- `references/smoke-test-patterns.md` -- Additional code examples

---

## Remember

1. **Do not invent rules.** Standards come from `smoke-test/AGENTS.md` and the cited helpers.
2. **Cypress, Playwright, and connector tests are out of scope.**
3. **Be conservative with BLOCKERs.** Only flag when a standard is clearly violated. Older files that still use `_ingest_cleanup_data_impl` with unique keys are acceptable.
4. **Do not BLOCK unique-dataset fixtures for missing pre-delete.**
5. **Acknowledge good patterns.** Note when tests follow standards well.
6. **CI output must be deterministic.** Same input -> same verdict.
