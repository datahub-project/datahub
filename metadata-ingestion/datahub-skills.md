# Building Connectors with datahub-skills

datahub-skills is a [Claude Code](https://claude.ai/code) plugin that accelerates connector development with specialized skills for planning, standards loading, and code review. It is designed to be used alongside the [adding a source guide](./adding-source.md) — the skills handle research, scaffolding, and review; you handle the code.

:::note

This guide assumes you have already set up your local development environment per the [developing guide](./developing.md).

:::

## Installing datahub-skills

**Option 1 — install script (recommended):**

```bash
curl -fsSL https://raw.githubusercontent.com/acryldata/datahub-skills/main/install.sh | bash
```

**Option 2 — npx:**

```bash
npx datahub-skills install
```

After installation, confirm the plugin is registered:

```bash
cat ~/.claude/plugins/installed_plugins.json
```

```json
{
  "version": 2,
  "plugins": {
    "datahub-skills@datahub-skills": [
      {
        "scope": "user",
        "installPath": "/Users/you/.claude/plugins/cache/datahub-skills/datahub-skills/1.2.0",
        "version": "1.2.0"
      }
    ]
  }
}
```

## Available Skills

| Skill            | Command                                       | When to use                                                                |
| ---------------- | --------------------------------------------- | -------------------------------------------------------------------------- |
| Load standards   | `/datahub-skills:load-standards`              | Start of every session — loads golden connector standards into context     |
| Plan connector   | `/datahub-skills:datahub-connector-planning`  | Before writing any code — research, entity mapping, architecture decisions |
| Review connector | `/datahub-skills:datahub-connector-pr-review` | Before opening a PR — automated review against standards                   |

## Workflow

### 1. Load standards

Run this at the start of every Claude Code session. It loads the golden connector standards into context so all subsequent skills and code generation follow established patterns.

```
/datahub-skills:load-standards
```

Expected output:

```
Loaded connector standards. Ready to review.

Standards loaded:
- main.md — base classes, SDK V2, config patterns
- patterns.md — file organization, error handling, warning/failure reporting
- testing.md — test requirements, golden files, anti-patterns
- code_style.md — type safety, naming conventions, mypy compliance
- containers.md — container hierarchy design
- lineage.md — SqlParsingAggregator, view lineage, query log lineage
...
```

:::note

Standards are loaded into your current Claude Code context window. If you start a new session, run `/load-standards` again before continuing.

:::

### 2. Plan the connector

Run the planning skill before writing any implementation code. It researches the source system, asks you a set of scoping questions, then generates a `_PLANNING.md` document that serves as the implementation blueprint.

```
/datahub-skills:datahub-connector-planning build a connector for [source name]
```

The skill runs in four stages:

1. **Classify** — identifies the source category (SQL database, BI tool, orchestration, etc.) and asks you to confirm
2. **Research** — gathers API docs, Python client libraries, similar existing connectors, Docker images for testing
3. **Scope** — asks you three questions about test environment, permissions, and which features to prioritize
4. **Document** — generates `_PLANNING.md` at the repo root with entity mapping, architecture decisions, and an ordered implementation plan

After the document is generated, you will see a summary and a prompt:

```
## Planning Document Created

Location: `_PLANNING.md`

### Key Decisions:
- Base class: StatefulIngestionSourceBase — no SQLAlchemy dialect exists
- Entity mapping: Pipeline → DataFlow, Table → DataJob, outlet lineage to destination Datasets
- Lineage approach: outlet URNs from state files; inlet URNs user-configured
- Test strategy: filesystem fixtures (no live service required)

Do you approve proceeding to implementation?
```

Reply `approved` to proceed, or describe changes and the skill will revise the document before continuing.

### 3. Build

This is your implementation step. Use the plan in `_PLANNING.md` as the guide and refer to `CLAUDE.md` for build, lint, and test commands.

Key commands during development:

```bash
# Lint and format Python code
./gradlew :metadata-ingestion:lintFix

# Run unit tests
./gradlew :metadata-ingestion:testQuick

# Run a single test file
pytest tests/unit/myconnector/test_myconnector_source.py -v
```

See [CLAUDE.md](../CLAUDE.md) and [developing.md](./developing.md) for the complete command reference.

### 4. Review the connector

Run the review skill before opening a PR. It performs a full standards compliance review — architecture, code quality, type safety, error handling, test coverage, and documentation — and produces a verdict with prioritized findings.

```
/datahub-skills:datahub-connector-pr-review
```

The skill outputs a structured report:

```
## Connector Review Report

Verdict: NEEDS CHANGES — 3 blockers require fixes before merge.

| Category          | Status         |
| ----------------- | -------------- |
| Architecture      | ✅ PASS        |
| Code Organization | ✅ PASS        |
| Error Handling    | 🔴 NEEDS FIXES |
| Type Safety       | 🟡 NEEDS FIXES |
| Test Quality      | 🔴 NEEDS FIXES |
| Performance       | ✅ PASS        |

### 🔴 BLOCKERS (Must Fix)

B1 — Silent exception swallow in state.json parsing
  dlt_client.py:269 — except Exception: pass with no logging...
```

Severity levels:

| Level             | Meaning                                               | Action                |
| ----------------- | ----------------------------------------------------- | --------------------- |
| 🔴 **BLOCKER**    | Violates standards or will cause issues in production | Must fix before merge |
| 🟡 **WARNING**    | Significant issue that should be addressed            | Should fix            |
| ℹ️ **SUGGESTION** | Would improve quality                                 | Optional              |

After fixing blockers, re-run the review skill. It resumes with full context from the previous session:

```
/datahub-skills:datahub-connector-pr-review
```

## The `skill_docs/` convention

Commit the skill-generated artifacts alongside your connector code. This gives PR reviewers full context on design decisions and known issues, and creates a paper trail for follow-up work.

Place them in `src/datahub/ingestion/source/<connector>/skill_docs/`:

```
src/datahub/ingestion/source/myconnector/
├── __init__.py
├── config.py
├── myconnector.py
├── myconnector_client.py
├── myconnector_report.py
└── skill_docs/
    ├── _PLANNING.md                              # /datahub-connector-planning output
    └── _datahub-connector-pr-review-YYYY-MM-DD.md  # /datahub-connector-pr-review output
```

## Tips

- Always run `/load-standards` at the start of a new Claude Code session — standards are not persisted across sessions
- The planning skill will ask you scoping questions before generating `_PLANNING.md`; answer them rather than skipping, as they determine the architecture decisions in the plan
- The review skill catches issues the checklist cannot — error handling gaps, trivial tests, type safety violations — run it even if your code looks clean
- Commit `skill_docs/` alongside the connector code; PR reviewers use it to understand design decisions without re-reading the full diff
- If a review session is interrupted, re-invoke the skill and it will resume from where it left off
