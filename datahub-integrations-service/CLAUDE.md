# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Development Commands

### Setup and Dependencies

```bash
# Install dependencies and set up dev environment
../gradlew installDev
source venv/bin/activate

# Update dependencies after changes to pyproject.toml
./scripts/lockfile.sh

# Update a specific package
./scripts/lockfile.sh --upgrade-package <package>
```

### Running the Service

```bash
# Local development (with hot reload)
uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003 --reload

# Development with remote GMS (e.g., dev01)
export DATAHUB_GMS_PROTOCOL='https'
export DATAHUB_GMS_HOST='dev01.acryl.io/api/gms'
export DATAHUB_GMS_PORT=''
export DATAHUB_GMS_API_TOKEN='<token>'
uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003 --reload
```

### Testing and Code Quality

Run formatting / linting / type checking / relevant tests after all changes

```bash
# Run tests
pytest

# Run formatting
ruff format

# Run linting
ruff check

# Run type checking
mypy src/ tests/
```

## Architecture Overview

This is a **FastAPI-based microservice** that provides integrations between DataHub's core metadata management system and external platforms. The service runs on port 9003 by default.

### Core Components

**Actions System** (`actions/`)

- Manages real-time event processing and automation workflows
- Kafka-based event sourcing for metadata changes
- Supports custom action plugins via entry points
- Includes ReactPy admin UI for monitoring

**Slack Integration** (`slack/`)

- Comprehensive Slack bot with command routing (`/datahub search`, `/datahub get`, `/datahub ask`)
- Entity preview and search results rendering
- @mention handling for AI-powered assistance
- OAuth flow management and incident handling

**Gen AI Module** (`gen_ai/`)

- AI-powered metadata enhancement (entity descriptions, glossary terms)
- Query description inference
- AWS Bedrock integration with MLflow for model management
- Cached graph client for performance optimization

**MCP Server** (`mcp/`)

- Model Context Protocol implementation for structured API access
- GraphQL query execution with Cloud/OSS compatibility
- Entity search and retrieval tools

**Analytics Engine** (`analytics/`)

- Data preview capabilities for BigQuery, Snowflake, DuckDB, S3
- Schema introspection and query execution
- Streaming response handling for large datasets

**Notifications System** (`notifications/`)

- Multi-channel delivery (Email via SendGrid, Slack)
- Template-based messaging with async delivery

**Data Sharing** (`share/`)

- Cross-platform data sharing and synchronization
- Entity sharing with lineage support

**Propagation Actions** (`propagation/`)

- Metadata propagation across platforms (BigQuery, Snowflake, Unity Catalog)

### Key Architecture Patterns

- **Plugin Architecture**: Extensible action system via entry points
- **Two-tier Routing**: Internal router (`/private`) and external router (`/public`)
- **Async Processing**: Background tasks for long-running operations
- **Caching Strategies**: TTL-based caching for metadata and S3 URIs
- **Environment-based Configuration**: DataHub GMS connectivity settings

### Development Notes

- Dependencies are managed with `uv` compile/sync commands
- The project has editable dependency on `acryl-datahub` via `metadata-ingestion`
- Hot-reload works for code changes, but Docker rebuild needed for dependency changes
- VSCode automatically detects the `venv` directory for Python environment

### Common File Locations

- Main server: `src/datahub_integrations/server.py`
- Global config: `src/datahub_integrations/app.py`
- Router modules: Each component has its own `router.py`
- Actions: `src/datahub_integrations/actions/`
- Tests: `tests/` (mirrors `src/` structure)

### Environment Variables

Key environment variables for different deployment scenarios:

- `DATAHUB_GMS_PROTOCOL`, `DATAHUB_GMS_HOST`, `DATAHUB_GMS_PORT`
- `DATAHUB_GMS_API_TOKEN` (for authenticated access)
- `EXTRA_UVICORN_ARGS` (additional server arguments)

## Code Quality and Development Guidelines

### Understanding Existing Code

Before making changes, always understand why current implementations exist:

- **Complex Logic Usually Has Reasons**: If you see intricate state management, callbacks, or filtering logic, investigate the edge cases and requirements it handles before simplifying
- **Read Related Code**: Look at how components are used, their tests, and related functionality to understand the full context
- **Check Git History**: Use `git log` and `git blame` to understand the evolution of complex code sections

### Refactoring Best Practices

**Start Small and Incremental:**

- Make the minimal change that solves the immediate problem
- Avoid over-engineering based on anticipated future requirements
- Prefer multiple small, reviewable changes over large rewrites

**Preserve Essential Behavior:**

- Identify what's truly core business logic vs. incidental complexity
- Don't remove functionality that handles edge cases without understanding why it exists
- Test thoroughly when simplifying interfaces or removing apparent redundancy

### Writing Tests

- We use `pytest` for testing and prefer pytest-style tests instead of unittest
- Tests should be reasonably comprehensive, but our goal is not 100% coverage. Instead, focus on testing the core functionality and important edge cases while ensuring that the test suite is simple, readable, and maintainable.
- Good code is easy to test - if the tests are super complex or require tons of mocks, it's a sign that the code is not well-designed

### Common Patterns to Consider

**Separation of Concerns:**

- Extract platform-specific logic (UI, rendering, formatting) from core business logic
- Create focused helper classes that encapsulate specific responsibilities
- Keep main classes clean by delegating specialized work

**Interface Design:**

- Design APIs that are stateless from the caller's perspective
- Use context managers for temporary state or configuration
- Prefer composition over inheritance for complex behaviors

**State Management:**

- Consider when state should be derived vs. cached vs. stored
- Avoid superflous state, and pay careful attention to data structures and their relationships and their lifecycle

**Error Handling:**

- Preserve existing error handling patterns unless explicitly improving them
- Add logging for debugging complex state transitions
- Consider writing tests for complex refactored logic, especially state management
