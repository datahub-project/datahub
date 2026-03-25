# DataHub — Agent Development Guide

This is the canonical reference for working with the DataHub codebase. It applies to all coding
agents (Claude Code, Cursor, Codex CLI, Devin, etc.) and human developers alike.

## Essential Commands

**Build and test:**

```bash
./gradlew build           # Build entire project
./gradlew check           # Run all tests and linting
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

**IMPORTANT: Verifying Python code changes:**

- **ALWAYS use `./gradlew :metadata-ingestion:lintFix`** to verify Python code changes
- **NEVER use `python3 -m py_compile`** - it doesn't catch style issues or type errors
- **NEVER use `ruff` or `mypy` commands directly** - use the Gradle task instead
- lintFix runs ruff formatting and fixing automatically, ensuring code quality
- For smoke-test changes, the lintFix command will also check those files

## Code Formatting and Linting

**CRITICAL: Always use Gradle tasks for formatting and linting. Never use npm/yarn/npx commands directly.**

### Available Formatting Tasks

**Format everything:**

```bash
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
./gradlew :metadata-ingestion:lintFix      # Format and fix Python code
./gradlew :metadata-ingestion:lint         # Check Python formatting
```

### When CI Formatting Checks Fail

If you see CI failures like:

- `markdown_format / markdown_format_check (pull_request)` - Use `./gradlew :datahub-web-react:mdPrettierWrite`
- `graphql_prettier_check` - Use `./gradlew :datahub-web-react:graphqlPrettierWrite`
- `spotlessJavaCheck` - Use `./gradlew spotlessApply`
- Python linting failures - Use `./gradlew :metadata-ingestion:lintFix`

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

**IMPORTANT**: Validation must work across all APIs (GraphQL, OpenAPI, RestLI).

- **Never add validation in API-specific layers** (GraphQL resolvers, REST controllers) - this only protects one API
- **Always implement AspectPayloadValidators** in `metadata-io/src/main/java/com/linkedin/metadata/aspect/validation/`
- **Register as Spring beans** in `SpringStandardPluginConfiguration.java`
- **Follow existing patterns**: See `SystemPolicyValidator.java` and `PolicyFieldTypeValidator.java` as examples

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

The final site is served from `docs-website/genDocs/` (gitignored). It is assembled at build time
from multiple hand-authored sources plus several generation steps:

1. **Gradle generation tasks** produce `docs/generated/` (connector docs, entity reference, schemas)
2. **`generateDocsDir.ts`** discovers all markdown in the repo, applies transformations (frontmatter,
   link rewriting, `{{ inline }}` directives), and writes the result to `genDocs/`
3. **Docusaurus** serves from `genDocs/`, additionally generating GraphQL API docs and Python SDK docs

See `docs-website/AGENTS.md` for full pipeline details.

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

### Adding or editing a hand-authored doc

1. Create/edit the markdown file in `docs/`
2. Add an entry in `docs-website/sidebars.js` (the doc ID is the file path minus `.md`)
3. Run `scripts/dev/datahub-dev.sh docs` to preview

If `sidebars.js` is missing the entry, the build will warn about an unaccounted file.

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
- **TypeScript**: Use Prettier formatting, strict types (no `any`), React Testing Library

### Frontend Theming (Colors)

**Always use semantic color tokens** from `datahub-web-react/src/conf/theme/colorThemes/types.ts`. Never use hardcoded hex values, `REDESIGN_COLORS`, `ANTD_GRAY`, or direct alchemy `colors.gray[X]` imports.

**In styled-components** (no import needed — `theme` is available via props):

```typescript
background: ${(props) => props.theme.colors.bg};
color: ${(props) => props.theme.colors.text};
border: 1px solid ${(props) => props.theme.colors.border};
```

**In React component bodies:**

```typescript
import { useTheme } from 'styled-components';
const theme = useTheme();
<Icon color={theme.colors.icon} />
```

**For alchemy components** (`<Text>`, `<Icon>`, etc.) — do not pass `color`/`colorLevel` props. Let them inherit from themed parent styled-components.

**Do not import from:**

- `datahub-web-react/src/alchemy-components/theme/foundations/colors.ts` (raw palette, only used internally by the theme)
- `REDESIGN_COLORS` or `ANTD_GRAY` from `entityV2/shared/constants.ts`

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

- Python: Tests go in the `tests/` directory alongside `src/`, use `assert` statements
- Java: Tests alongside source in `src/test/`
- Frontend: Tests in `__tests__/` or `.test.tsx` files
- Smoke tests go in the `smoke-test/` directory

#### Testing Principles: Focus on Value Over Coverage

**IMPORTANT**: Quality over quantity. Avoid AI-generated test anti-patterns that create maintenance burden without providing real value.

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
- Breaking Changes: Always update `docs/how/updating-datahub.md` for breaking changes. Write entries for non-technical audiences, reference the PR number, and focus on what users need to change rather than internal implementation details

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
- [ ] Breaking changes documented in `docs/how/updating-datahub.md`

## Starting / Operating DataHub

Use `scripts/dev/datahub-dev.sh` for **ALL** environment operations.
**Do NOT use `./gradlew quickstartDebug` directly** — always use the wrapper script.

### `datahub-dev` CLI Tool

A stdlib-only Python CLI for agent-driven development. No venv needed — runs with system `python3`.

**Always use the shell wrapper as the entry point:**

```bash
scripts/dev/datahub-dev.sh <command>
```

Run `scripts/dev/datahub-dev.sh --help` to see all available subcommands (`start`, `stop`, `setup`,
`frontend`, `docs`, `status`, `wait`, `rebuild`, `test`, `flag list/get`, `env`, `sync-flags`, `reset`, `nuke`).

### End-to-End Workflow

0. **Setup** (once): `scripts/dev/datahub-dev.sh setup` — installs Python dev environment (provides `datahub` CLI). For frontend work, also run `scripts/dev/datahub-dev.sh setup frontend`.
1. **Start**: `scripts/dev/datahub-dev.sh start`
2. **Code**: Make changes to Java/Python/frontend code
3. **Rebuild**: `scripts/dev/datahub-dev.sh rebuild --wait`
4. **Test**: `scripts/dev/datahub-dev.sh test <test-path>`
5. **Iterate**: Repeat steps 2–4

**Frontend hot-reload:** Run `scripts/dev/datahub-dev.sh frontend` to start the React dev server with hot-reload (instead of rebuilding the frontend container).

### Module-to-Container Mapping

| Source directory                  | Container                                     |
| --------------------------------- | --------------------------------------------- |
| `metadata-service/`               | `datahub-gms`                                 |
| `datahub-graphql-core/`           | `datahub-gms`                                 |
| `metadata-io/`                    | `datahub-gms`                                 |
| `datahub-frontend/`               | `datahub-frontend-react`                      |
| `metadata-jobs/mce-consumer-job/` | `datahub-mce-consumer`                        |
| `metadata-jobs/mae-consumer-job/` | `datahub-mae-consumer`                        |
| `metadata-models/`                | All (triggers full rebuild + code generation) |

### Environment Variables

Set any env var for DataHub containers via `env set` + `env restart`:

```bash
scripts/dev/datahub-dev.sh env set KEY=VALUE
scripts/dev/datahub-dev.sh env restart       # required — changes take effect on restart
scripts/dev/datahub-dev.sh env list           # show current vars and pending_restart status
```

**Do NOT** manually edit `.env` files, use `docker compose -e`, or `export` — always use the wrapper.

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

### Recovery Escalation

**When to use each:**

- `stop`: Just shut down DataHub — no restart, no data loss
- `reset`: GMS returns 503 and doesn't recover, frontend shows "Unable to connect", tests fail
  with connection errors
- `nuke --keep-data`: Containers in restart loops, port conflicts, `reset` didn't fix it
- `nuke`: ES index corruption, MySQL schema issues after model changes, PDL model changes needing
  clean slate, `nuke --keep-data` didn't fix it

### Structured Test Output

Set `AGENT_MODE=1` to get machine-readable JSON test reports at `smoke-test/build/test-report.json`:

```bash
AGENT_MODE=1 scripts/dev/datahub-dev.sh test tests/test_system_info.py
```

## Common Operations

These commands work against **any** DataHub instance — local dev, staging, or production.
Provide connection details via environment variables:

```bash
export DATAHUB_GMS_URL=http://localhost:8080  # or your instance URL
export DATAHUB_GMS_TOKEN=<your-token>         # omit if auth is not required
```

### Init (setup authentication)

`datahub init` writes `~/.datahubenv` with the GMS URL and an access token. Run it once before
using any other CLI commands that require authentication.

```bash
# Quickstart: local instance with default credentials
datahub init --username datahub --password datahub

# Full agent best-practices guide (defaults, env vars, all scenarios)
datahub init --agent-context
```

### GraphQL

`datahub graphql` executes queries and mutations against the DataHub GraphQL API and can
introspect the live schema to discover available operations.

```bash
# Discover what's available
datahub graphql --list-operations --format json

# Inspect a specific operation's arguments
datahub graphql --describe dataset --format json

# Preview a query before executing
datahub graphql --query "{ me { corpUser { urn } } }" --dry-run

# Execute a query
datahub graphql --query "{ me { corpUser { urn username } } }" --format json
```

For full agent best practices (discovery, dry-run, error codes, common recipes):

```bash
datahub graphql --agent-context
```

## Key Documentation

**Essential reading:**

- `docs/architecture/architecture.md` - System architecture overview
- `docs/modeling/metadata-model.md` - How metadata is modeled
- `docs/what-is-datahub/datahub-concepts.md` - Core concepts (URNs, entities, etc.)

**External docs:**

- https://docs.datahub.com/docs/developers - Official developer guide
- https://demo.datahub.com/ - Live demo environment

## Python Virtual Environments

Gradle tasks manage all venvs automatically. Never create, activate, or pip-install into them manually. When running smoke tests outside Gradle: `smoke-test/venv/bin/python -m pytest ...`

## Important Notes

- Entity Registry is defined in YAML, not code (`entity-registry.yml`)
- All metadata changes flow through the event streaming system
- GraphQL schema is generated from backend GMS APIs
