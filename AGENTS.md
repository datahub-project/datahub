# DataHub — Agent Development Guide

This is the canonical reference for working with the DataHub codebase. It applies to all coding
agents (Claude Code, Cursor, Codex CLI, Devin, etc.) and human developers alike.

## Code Navigation (LSP)

Prefer LSP tools over Grep for code navigation tasks:

- Use `goToDefinition` to find where something is defined
- Use `findReferences` to find all call sites
- Use `workspaceSymbol` to find symbols by name
- Use diagnostics after any edit to catch type errors immediately

See [docs/lsp-setup.md](docs/lsp-setup.md) for installation and configuration.

## Essential Commands

**Build and test:**

```bash
./gradlew build           # Build entire project
./gradlew check           # Run all tests (Python/JS lint lives in lint-jobs.yml / :module:lint)
./gradlew lintCheck       # Run all lint checks (Python, JS, GraphQL, markdown, Java)
./gradlew lintFix         # Auto-fix all lint issues
./gradlew format          # Format all code (Java, Markdown, GraphQL, YAML)

# Note that each directory typically has a build.gradle file, but the available tasks follow similar conventions.

# Java code.
./gradlew spotlessApply   # Java code formatting

# Python code.
./gradlew :metadata-ingestion:testQuick     # Fast Python unit tests
./gradlew :metadata-ingestion:lint          # Python linting (ruff, mypy)
./gradlew :metadata-ingestion:lintFix       # Python linting auto-fix (ruff only)

# Markdown, GraphQL, YAML formatting
./gradlew :datahub-web-react:mdPrettierWrite        # Format markdown files
./gradlew :datahub-web-react:graphqlPrettierWrite   # Format GraphQL schemas
./gradlew :datahub-web-react:githubActionsPrettierWrite # Format GitHub Actions
```

**Verifying Python code changes:**

- `./gradlew :metadata-ingestion:lintFix` auto-fixes what ruff can (`check --fix` + `format`). It is not validation.
- `./gradlew :metadata-ingestion:lint` validates: `ruff check`, `ruff format --check` and mypy, as CI runs them. Run it before calling a change done; `lintFix` alone catches neither type errors nor what ruff cannot fix.
- Not `python3 -m py_compile`: it misses style issues and type errors.
- Not `ruff` or `mypy` directly: the Gradle tasks use the project's venv and config.
- For smoke-test changes, the lintFix command will also check those files

## Code Formatting and Linting

Format and lint through the Gradle tasks below, not by calling prettier or eslint via npm, yarn or npx: the tasks carry the project's config and match CI.

### Available Formatting Tasks

**Format everything:**

```bash
./gradlew lintCheck           # Run all lint checks (Python, JS, GraphQL, markdown, Java)
./gradlew lintFix             # Auto-fix all lint issues
./gradlew format              # Format all code (Java, Markdown, GraphQL, YAML)
./gradlew formatChanged       # Format only changed files (faster)
```

**Format specific file types:**

```bash
# Markdown files
./gradlew :datahub-web-react:mdPrettierWrite        # Format all markdown
./gradlew :datahub-web-react:mdPrettierCheck        # Check markdown formatting

# GraphQL schemas
./gradlew :datahub-web-react:graphqlPrettierWrite   # Format GraphQL files
./gradlew :datahub-web-react:graphqlPrettierCheck   # Check GraphQL formatting

# GitHub Actions YAML
./gradlew :datahub-web-react:githubActionsPrettierWrite   # Format workflow files
./gradlew :datahub-web-react:githubActionsPrettierCheck   # Check workflow files

# Java code
./gradlew spotlessApply       # Format Java code

# Python code
./gradlew :metadata-ingestion:lintFix      # Auto-fix Python (ruff)
./gradlew :metadata-ingestion:lint         # Validate Python (ruff + mypy)
```

### When CI Formatting Checks Fail

If you see CI failures like:

- `markdown_format / markdown_format_check (pull_request)` - Use `./gradlew :datahub-web-react:mdPrettierWrite`
- `graphql_prettier_check` - Use `./gradlew :datahub-web-react:graphqlPrettierWrite`
- `spotless-check` - Use `./gradlew spotlessApply`
- Python linting failures - Use `./gradlew :metadata-ingestion:lintFix`, then `./gradlew :metadata-ingestion:lint` to confirm (mypy errors need a manual fix)

**Never do this:**

```bash
npx prettier --write "docs/**/*.md"    # WRONG - bypasses Gradle
yarn prettier --write                   # WRONG - bypasses Gradle
npm run format                          # WRONG - bypasses Gradle
```

**Always do this:**

```bash
./gradlew :datahub-web-react:mdPrettierWrite      # CORRECT - uses Gradle
./gradlew format                                   # CORRECT - formats everything
```

### Why Use Gradle Tasks?

1. **Consistent configuration**: Gradle tasks use the project's Prettier config
2. **Pre-commit hook integration**: Gradle tasks match what CI runs
3. **Dependency management**: Ensures correct tool versions
4. **Cross-platform**: Works reliably across all environments

**Java SDK v2 integration tests:**

See [metadata-integration/java/datahub-client/CLAUDE.md](metadata-integration/java/datahub-client/CLAUDE.md) for detailed integration test documentation.

## Architecture Overview

DataHub is a **schema-first, event-driven metadata platform** with three core layers:

### Core Services

- **GMS (Generalized Metadata Service)**: Java/Spring backend handling metadata storage and REST/GraphQL APIs
- **Frontend**: React/TypeScript application consuming GraphQL APIs
- **Ingestion Framework**: Python CLI and connectors for extracting metadata from data sources
- **Event Streaming**: Kafka-based real-time metadata change propagation

### Key Modules

- `metadata-models/`: Avro/PDL schemas defining the metadata model
- `metadata-service/`: Backend services, APIs, and business logic
- `datahub-web-react/`: Frontend React application
- `metadata-ingestion/`: Python ingestion framework and CLI
- `datahub-graphql-core/`: GraphQL schema and resolvers

Most of the non-frontend modules are written in Java. The modules written in Python are:

- `metadata-ingestion/`
- `datahub-actions/`
- `metadata-ingestion-modules/airflow-plugin/`
- `metadata-ingestion-modules/gx-plugin/`
- `metadata-ingestion-modules/dagster-plugin/`
- `metadata-ingestion-modules/prefect-plugin/`

Each Python module has a gradle setup similar to `metadata-ingestion/` (documented above)

### Metadata Model Concepts

- **Entities**: Core objects (Dataset, Dashboard, Chart, CorpUser, etc.)
- **Aspects**: Metadata facets (Ownership, Schema, Documentation, etc.)
- **URNs**: Unique identifiers (`urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)`)
- **MCE/MCL**: Metadata Change Events/Logs for updates
- **Entity Registry**: YAML config defining entity-aspect relationships (`metadata-models/src/main/resources/entity-registry.yml`)

### Validation Architecture

Validation must work across all APIs (GraphQL, OpenAPI, RestLI).

- **Never add validation in API-specific layers** (GraphQL resolvers, REST controllers) - this only protects one API
- **Always implement AspectPayloadValidators** in `metadata-io/src/main/java/com/linkedin/metadata/aspect/validation/`
- **Register as Spring beans** in `SpringStandardPluginConfiguration.java`
- **Follow existing patterns**: See `SystemPolicyValidator.java` and `PolicyFieldTypeValidator.java` as examples

### Authorization Architecture

A metadata policy has three parts, and policies are OR'd. One matching policy grants the
operation. See [Policies](docs/authorization/policies.md).

- **Resource filter** (`resources.filter`): which assets. A tag or domain criterion selects assets
  that have that tag or domain. It is not a list of sub-resources the actor may edit.
- **Privilege**: the operation. An entity update is **Edit Entity**, or **Manage Documents** on
  documents. **Edit Entity Properties** covers every structured property. **Edit Entity Tags**
  covers tags. A column is authorized on the parent dataset or on the schema field itself with
  **Edit Entity** or **Edit Dataset Column Properties**. Create uses **Create Entity** when the
  asset does not exist yet. Delete uses **Delete Entity**.
- **Sub-resource list** (`privilegeConstraints`): which tags or structured properties that policy
  may change when the action names them. No list means the privilege applies in full. An action
  that names no sub-resource is not filtered by the list. A list of tag URNs allows only those
  tags. A list of property URNs allows only those properties. `TYPE` equals `structuredProperty`
  matches every structured property. A second policy with no list still grants in full.

The validator sees the real diff and is the precise check. The ingest gate and GraphQL resolvers
are coarse: they accept the same privilege set and must not deny a write the validator would
allow. For aspects that carry tags or structured properties they pass a wildcard sub-resource so a
list is decided by the validator, and both pick create, update or delete with
`AuthUtil.subResourceWriteOperation`. A form prompt checks that the answer matches the prompt. An
assigned user can answer without the property privilege: the value is written as the system actor
and the audit stamp stays the caller. A user who is not assigned is authorized as the caller.
Authorization failures are authorization results (403), not validation errors.

When adding an entity or API:

- Enforce authorization across GraphQL, OpenAPI, and Rest.li
- Keep basic entity CRUD permissions alongside any higher-level, entity-specific permissions
- Use `AuthorizationUtils` for GraphQL and `AuthUtil.isAPIAuthorized*` for REST APIs
- Put shared aspect rules in an `AbstractAspectAuthorizationValidator`
- Apply view-based access controls by default; only set `viewUnrestricted: true` for intentionally public entities
- Add allowed and denied access tests

## Development Flow

1. **Schema changes** in `metadata-models/` trigger code generation across all languages
2. **Backend changes** in `metadata-service/` and other Java modules expose new REST/GraphQL APIs
3. **Frontend changes** in `datahub-web-react/` consume GraphQL APIs
4. **Ingestion changes** in `metadata-ingestion/` emit metadata to backend APIs

## Working on Docs

The docs site is a **Docusaurus 2** app in `docs-website/`. It runs on **port 3001** (not 3000, to avoid
conflicting with the frontend dev server).

### Quick start

```bash
scripts/dev/datahub-dev.sh docs            # fast start (assumes prior build)
scripts/dev/datahub-dev.sh docs --build    # full rebuild (runs docGen + yarnGenerate first)
```

Or via Gradle directly: `./gradlew :docs-website:yarnStart` (always does a full build).

### How the docs site is assembled

The final site is assembled at build time into `docs-website/genDocs/` (gitignored) —
see `docs-website/AGENTS.md` for the full pipeline.

### Where docs live

| Path                                           | What to edit                                    | Detail guide                                |
| ---------------------------------------------- | ----------------------------------------------- | ------------------------------------------- |
| `docs/`                                        | Hand-authored feature guides, API docs, how-tos | _(this section)_                            |
| `metadata-ingestion/docs/sources/<connector>/` | Connector docs (`*_pre.md`, `*_post.md`, etc.)  | `metadata-ingestion/docs/sources/AGENTS.md` |
| `metadata-models/docs/entities/`               | Entity descriptions (input to `modelDocGen`)    | `metadata-models/docs/AGENTS.md`            |
| `docs-website/src/pages/`                      | Custom React pages (e.g. `/integrations`)       | `docs-website/AGENTS.md`                    |
| `docs-website/src/learn/`                      | Blog / learning articles (served at `/learn`)   | `docs-website/AGENTS.md`                    |
| `docs-website/sidebars.js`                     | Sidebar navigation tree                         | `docs-website/AGENTS.md`                    |
| `docs-website/static/`                         | Images, logos, static assets                    | `docs-website/AGENTS.md`                    |
| `docs/generated/`                              | **Never edit** — auto-generated                 |                                             |
| `docs-website/genDocs/`                        | **Never edit** — assembled output               |                                             |

Adding a hand-authored doc or a DataHub Cloud release note: the file alone is not enough, it
must also be registered in `docs-website/sidebars.js` (the build only warns). Steps:
`docs-website/AGENTS.md`.

## Code Standards

### General Principles

- This is production code - maintain high quality
- Follow existing patterns within each module
- Generate appropriate unit tests
- Use type annotations everywhere (Python/TypeScript)

### Language-Specific

- **Java**: Use Spotless formatting, Spring Boot patterns, TestNG/JUnit Jupiter for tests
- **Python**: Use ruff for linting/formatting, pytest for testing, pydantic for configs
  - **Type Safety**: Everything must have type annotations, avoid `Any` type, use specific types (`Dict[str, int]`, `TypedDict`)
  - **Data Structures**: Prefer dataclasses/pydantic for internal data, return dataclasses over tuples
  - **Code Quality**: Avoid global state, use named arguments, don't re-export in `__init__.py`, refactor repetitive code
  - **Error Handling**: Robust error handling with layers of protection for known failure points
  - **Security**: Never write secrets into the process environment to feed a third-party SDK; inject them programmatically. Rationale and the canonical pattern: `metadata-ingestion/src/datahub/ingestion/source/AGENTS.md`
  - **Ingestion**: Module guide `metadata-ingestion/AGENTS.md`; connector conventions (layout, config, reporting, lineage, stale-entity safety, PR scope) in `metadata-ingestion/src/datahub/ingestion/source/AGENTS.md`
  - **Smoke tests**: Authoring conventions live in [`smoke-test/AGENTS.md`](smoke-test/AGENTS.md)
- **TypeScript**: Use Prettier formatting, strict types (no `any`), React Testing Library
  - **Frontend**: Component conventions, theming/color tokens, file layout, and the frontend CI checklist live in `datahub-web-react/AGENTS.md`

### Code Comments

Only add comments that provide real value beyond what the code already expresses.

**Do NOT** add comments for:

- Obvious operations (`# Get user by ID`, `// Create connection`)
- What the code does when it's self-evident (`# Loop through items`, `// Set variable to true`)
- Restating parameter names or return types already in signatures
- Basic language constructs (`# Import modules`, `// End of function`)

**DO** add comments for:

- **Why** something is done, especially non-obvious business logic or workarounds
- **Context** about external constraints, API quirks, or domain knowledge
- **Warnings** about gotchas, performance implications, or side effects
- **References** to tickets, RFCs, or external documentation that explain decisions
- **Complex algorithms** or mathematical formulas that aren't immediately clear
- **Temporary solutions** with TODOs and context for future improvements

Examples:

```python
# Good: Explains WHY and provides context
# Use a 30-second timeout because Snowflake's query API can hang indefinitely
# on large result sets. See issue #12345.
connection_timeout = 30

# Bad: Restates what's obvious from code
# Set connection timeout to 30 seconds
connection_timeout = 30
```

### Testing Strategy

- Python: Tests go in the `tests/` directory alongside `src/`, use `assert` statements. Ingestion test conventions: `metadata-ingestion/tests/AGENTS.md`
- Java: Tests alongside source in `src/test/`
- Frontend: Tests in `__tests__/` or `.test.tsx` files
- Smoke tests go in the `smoke-test/` directory. How to write them: [`smoke-test/AGENTS.md`](smoke-test/AGENTS.md). How to run them: [`smoke-test/README.md`](smoke-test/README.md)

#### Testing Principles: Focus on Value Over Coverage

Quality over quantity. Avoid AI-generated test anti-patterns that create maintenance burden without providing real value.

**Focus on behavior, not implementation**:

- Test what the code does (business logic, edge cases that occur in production)
- Don't test how it does it (implementation details, private fields via reflection)
- Don't test third-party libraries work correctly (Spring, Micrometer, Kafka clients, etc.)
- Don't test Java/Python language features (`synchronized` methods are thread-safe, `@Nonnull` parameters reject nulls)

**Avoid these specific anti-patterns**:

- Testing null inputs on `@Nonnull`/`@NonNull` annotated parameters
- Verifying exact error message wording (creates brittleness during refactoring)
- Testing every possible input variation (case sensitivity x whitespace x special chars = maintenance nightmare)
- Using reflection to verify private implementation details
- Redundant concurrency testing on `synchronized` methods
- Testing obvious getter/setter behavior without business logic
- Testing Lombok-generated code (`@Data`, `@Builder`, `@Value` classes) - you're testing Lombok's code generator, not your logic
- Testing that annotations exist on classes - if required annotations are missing, the framework/compiler will fail at startup, not in your tests

**Appropriate test scope**:

- **Simple utilities** (enums, string parsing, formatters): ~50-100 lines of focused tests
  - Happy path for each method
  - One example of invalid input per method
  - Edge cases likely to occur in production
- **Complex business logic**: Test proportional to risk and complexity
  - Integration points and system boundaries
  - Security-critical operations
  - Error handling for realistic failure scenarios
- **Warning sign**: If tests are 5x+ the size of implementation, reconsider scope

**Examples of low-value tests to avoid**:

```java
// BAD: Testing @Nonnull contract (framework's job)
@Test
public void testNullParameterThrowsException() {
    assertThrows(NullPointerException.class,
        () -> service.process(null)); // parameter is @Nonnull
}

// BAD: Testing Lombok-generated code
@Test
public void testBuilderSetsAllFields() {
    MyConfig config = MyConfig.builder()
        .field1("value1")
        .field2("value2")
        .build();
    assertEquals(config.getField1(), "value1");
    assertEquals(config.getField2(), "value2");
}

// BAD: Testing that annotations exist
@Test
public void testConfigurationAnnotations() {
    assertNotNull(MyConfig.class.getAnnotation(Configuration.class));
    assertNotNull(MyConfig.class.getAnnotation(ComponentScan.class));
}
// If @Configuration is missing, Spring won't load the context - you don't need a test for this

// BAD: Exact error message (brittle)
assertEquals(exception.getMessage(),
    "Unsupported database type 'oracle'. Only PostgreSQL and MySQL variants are supported.");

// BAD: Redundant variations
assertEquals(DatabaseType.fromString("postgresql"), DatabaseType.POSTGRES);
assertEquals(DatabaseType.fromString("PostgreSQL"), DatabaseType.POSTGRES);
assertEquals(DatabaseType.fromString("POSTGRESQL"), DatabaseType.POSTGRES);
assertEquals(DatabaseType.fromString("  postgresql  "), DatabaseType.POSTGRES);
// ... 10 more case/whitespace variations

// GOOD: Focused behavioral test
@Test
public void testFromString_ValidInputsCaseInsensitive() {
    assertEquals(DatabaseType.fromString("postgresql"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("POSTGRESQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("  postgresql  "), DatabaseType.POSTGRES);
}

@Test
public void testFromString_InvalidInputThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> DatabaseType.fromString("oracle"));
}

// GOOD: Testing YOUR custom validation logic on a Lombok class
@Test
public void testCustomValidation() {
    assertThrows(IllegalArgumentException.class,
        () -> MyConfig.builder().field1("invalid").build().validate());
}
```

**When in doubt**: Ask "Does this test protect against a realistic regression?" If not, skip it.

#### Security Testing: Configuration Property Classification

**Critical test**: `metadata-io/src/test/java/com/linkedin/metadata/system_info/collectors/PropertiesCollectorConfigurationTest.java`

This test prevents sensitive data leaks by requiring explicit classification of all configuration properties as either sensitive (redacted) or non-sensitive (visible in system info).

**When adding new configuration properties**: The test will fail with clear instructions on which classification list to add your property to. Refer to the test file's comprehensive documentation for template syntax and examples.

This is a mandatory security guardrail - never disable or skip this test.

### Commits

- Follow Conventional Commits format for commit messages
- Breaking Changes: Always update `docs/how/breaking-changes.md` for breaking changes. Write entries for non-technical audiences, reference the PR number, and focus on what users need to change rather than internal implementation details
- **Never bypass git hook failures with `--no-verify`** (or any equivalent skip flag) on commit or push. A failing hook is a signal that something needs attention — stop, report the failure to the user, and confirm how to proceed. Only use `--no-verify` if the user explicitly tells you to for that specific action.

### Pull Requests

When creating PRs, follow the template in `.github/pull_request_template.md`:

**PR Title Format** (from [Contributing Guide](docs/CONTRIBUTING.md#pr-title-format)):

```
<type>[optional scope]: <description>
```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `perf`, `style`, `build`, `ci`, `chore`

Example: `feat(parser): add ability to parse arrays`

**Checklist** (verify before submitting):

- [ ] PR conforms to the Contributing Guideline (especially PR Title Format)
- [ ] Links to related issues (if applicable)
- [ ] Tests added/updated (if applicable)
- [ ] Docs added/updated (if applicable)
- [ ] Breaking changes documented in `docs/how/breaking-changes.md`

### Confidentiality in Committed Code

DataHub is a **public repository**. Never put customer-identifiable or
environment-specific details into committed code, tests, docs, comments, commit
messages, or PRs:

- No real database / schema / table / view / column names, and no usernames,
  customer names, host names, account IDs, or URLs from customer environments.
- No Linear/Jira ticket IDs or links.
- When reproducing a customer issue in a test, use generic placeholder names
  (e.g. `my_db.my_schema.events`, `col_a`) that preserve the structural pattern
  being tested, not the customer's actual identifiers.
- Vendor/system built-ins (e.g. a platform's standard system tables) are fine,
  but prefer generic names when in doubt.

## Starting / Operating DataHub

Use `scripts/dev/datahub-dev.sh` for **ALL** environment operations.
**Do NOT use `./gradlew quickstartDebug` directly** — always use the wrapper script.

### `datahub-dev` CLI Tool

A stdlib-only Python CLI for agent-driven development. No venv needed — runs with system `python3`.

**Always use the shell wrapper as the entry point:**

```bash
scripts/dev/datahub-dev.sh <command>
```

Run `scripts/dev/datahub-dev.sh --help` to see all available subcommands (`start`, `stop`, `suspend`,
`setup`, `frontend`, `docs`, `status`, `wait`, `rebuild`, `test`, `flag list/get`, `env`,
`sync-flags`, `reset`, `nuke`, `instances list/clean`, `shell-env`).

### Local mode, remote runners, recovery

Docker-free `start --local`, remote runner plugins, recovery escalation (`stop` → `reset` →
`nuke --keep-data` → `nuke`) and `AGENT_MODE=1` structured test output: `scripts/dev/AGENTS.md`.

### End-to-End Workflow

0. **Setup** (once): `scripts/dev/datahub-dev.sh setup` — installs Python dev environment (provides `datahub` CLI). For frontend work, also run `scripts/dev/datahub-dev.sh setup frontend`.
1. **Start**: `scripts/dev/datahub-dev.sh start`
2. **Code**: Make changes to Java/Python/frontend code
3. **Rebuild**: `scripts/dev/datahub-dev.sh rebuild --wait`
4. **Test**: `scripts/dev/datahub-dev.sh test <test-path>`
5. **Iterate**: Repeat steps 2–4

**Frontend hot-reload:** Run `scripts/dev/datahub-dev.sh frontend` to start the React dev server with hot-reload (instead of rebuilding the frontend container).

### Module-to-Container Mapping

| Source directory                   | Container                                     |
| ---------------------------------- | --------------------------------------------- |
| `metadata-service/`                | `datahub-gms`                                 |
| `datahub-graphql-core/`            | `datahub-gms`                                 |
| `metadata-io/`                     | `datahub-gms`                                 |
| `datahub-frontend/`                | `datahub-frontend-react`                      |
| `packages/datahub-ai-service/`     | `datahub-ai-service`                          |
| `packages/datahub-core-mcp-tools/` | `datahub-ai-service`                          |
| `metadata-jobs/mce-consumer-job/`  | `datahub-mce-consumer`                        |
| `metadata-jobs/mae-consumer-job/`  | `datahub-mae-consumer`                        |
| `metadata-models/`                 | All (triggers full rebuild + code generation) |

### Environment Variables

Set any env var for DataHub containers via `env set` + `env restart`:

```bash
scripts/dev/datahub-dev.sh env set KEY=VALUE
scripts/dev/datahub-dev.sh env restart       # required — changes take effect on restart
scripts/dev/datahub-dev.sh env list           # show current vars and pending_restart status
```

**Do NOT** manually edit `.env` files, use `docker compose -e`, or `export` — always use the wrapper.

**GMS primary storage read pool** (optional, entity aspect DAO only): `EBEAN_READ_POOL_ENABLED` /
`CASSANDRA_READ_POOL_ENABLED` route non-locking reads to a second pool; writes and `forUpdate`
reads stay on PRIMARY. See [docs/deploy/primary-storage-read-pool.md](docs/deploy/primary-storage-read-pool.md).
`DATAHUB_READ_ONLY=true` is separate — it disables writes and does not register the read pool.

### Feature Flag Lifecycle

**All flag changes require a container restart.** Use `env set` + `env restart`:

```bash
scripts/dev/datahub-dev.sh env set SHOW_BROWSE_V2=true
scripts/dev/datahub-dev.sh env restart
```

`flag list` and `flag get` are read-only inspection tools — they show the current live values from
the running server but do not change anything.

The flag manifest at `scripts/generated/flag-classification.json` is **auto-generated**
(gitignored). Run `scripts/dev/datahub-dev.sh sync-flags` after adding fields to `FeatureFlags.java`
or after a fresh clone.

### Stopping DataHub

`scripts/dev/datahub-dev.sh stop` shuts down all containers without restarting.

When starting, `datahub-dev start` automatically detects and stops conflicting DataHub instances
from other worktrees/compose projects that occupy the same ports.

## DataHub CLI against any instance

Init, connection profiles, GraphQL discovery and recipes: `.claude/skills/datahub-cli/SKILL.md`,
or `datahub init --agent-context` and `datahub graphql --agent-context` from the CLI itself.

### DataHub CLI connection profiles

- Never run `datahub init` without `--profile <name>`. Name the profile after the instance (`acme`, `acme-staging`).
- Never run `datahub profile use`; it changes the default for the user's other shells.
- Pass `--profile <name>` on every command, unless the session already sets `DATAHUB_PROFILE`.
- Before any write (`put`, `delete`, `ingest`, upserts), run `datahub profile current --format json` and confirm the server is the one the user asked for.
- If a token has expired, re-run `datahub init --profile <same name>` with the same host. Do not create a new profile.

## Key Documentation

**Essential reading:**

- `docs/architecture/architecture.md` - System architecture overview
- `docs/modeling/metadata-model.md` - How metadata is modeled
- `docs/what-is-datahub/datahub-concepts.md` - Core concepts (URNs, entities, etc.)

**External docs:**

- https://docs.datahub.com/docs/developers - Official developer guide
- https://demo.datahub.com/ - Live demo environment

## Playwright UI E2E Tests

Full reference: [`e2e-test/ui/playwright/README.md`](e2e-test/ui/playwright/README.md) —
including test-data seeding via `test.use({ featureName: ... })`.

## Python Virtual Environments

Gradle tasks manage all venvs automatically. Never create, activate, or pip-install into them manually. When running smoke tests outside Gradle: `smoke-test/venv/bin/python -m pytest ...`

## Maintaining AGENTS.md files

Root holds cross-project rules; module-specific rules live in the nearest `AGENTS.md`
(nearest file wins). Where an `AGENTS.md` exists, its `CLAUDE.md` is a one-line `@AGENTS.md` shim.

- While developing or reviewing a PR, when you make or receive an architectural or
  strategic decision that is not derivable from the code (a pattern to follow, a trap, a
  framework-over-connector call), ask the author or reviewer whether to persist it.
  Persist it as a check (ruff rule, unit test, lint script) when possible; otherwise as
  one bullet of at most two lines in the nearest `AGENTS.md`, linking the canonical code
  example.
- Remove a bullet when tooling now enforces it, or the code it points at is gone. A style
  opinion the code does not agree on is not written down until the team decides.
- One owner per fact: signpost root `AGENTS.md`, a module `developing.md` or a dev guide;
  never copy. Budgets: 200 lines per `AGENTS.md`, 120 per `BUGBOT.md`. To add, cut.
- Rules a reviewer should enforce get a mirrored "If ... then" entry in the module's
  `.cursor/BUGBOT.md`.
- `./gradlew agentDocsLint` (part of `lintCheck`) fails on an exceeded budget or a
  backticked path or relative link that no longer exists. Known findings are tolerated via
  `.github/scripts/check_agent_docs_baseline.txt`, which only shrinks.

## Important Notes

- Entity Registry is defined in YAML, not code (`entity-registry.yml`)
- All metadata changes flow through the event streaming system
- GraphQL schema is generated from backend GMS APIs
