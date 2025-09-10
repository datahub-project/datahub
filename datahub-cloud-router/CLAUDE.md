# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this directory.

## Development Commands

### Setup and Dependencies

```bash
# Install dependencies and set up dev environment
../gradlew :datahub-cloud-router:installDev
source venv/bin/activate

# Direct setup (alternative)
uv venv --python 3.11 venv
source venv/bin/activate
./scripts/sync.sh
```

### Running the Router

```bash
# Local development (with hot reload)
datahub-router --host 0.0.0.0 --port 9005

# Or directly with Python
python -m datahub.cloud.router

# With custom database
datahub-router --db-type sqlite --db-path .dev/custom.db
```

### Testing and Code Quality

Run formatting / linting / type checking / relevant tests after all changes

```bash
# Using Gradle (recommended)
../gradlew :datahub-cloud-router:lintFix
../gradlew :datahub-cloud-router:test

# Direct commands
source venv/bin/activate
ruff check --fix datahub/ tests/
ruff format datahub/ tests/
mypy datahub/ tests/
pytest
```

## Architecture Overview

This is a **FastAPI-based multi-tenant router** that routes Microsoft Teams webhook events to the correct DataHub instances based on tenant and team mappings. The router acts as a central hub for platform integrations.

### Core Components

**Multi-Tenant Router** (`core.py`)

- Routes webhook events to different DataHub instances
- Supports tenant-specific and team-specific routing strategies
- Creates unknown tenant alerts for unrouted events
- Async event processing with timeout handling

**Database Abstraction** (`db/`)

- SQLite: File-based storage for development and small deployments
- MySQL: Production-ready with connection pooling and high concurrency
- In-Memory: Fast testing and development
- Abstract interface allows easy extension to other databases

**FastAPI Server** (`server.py`)

- Webhook endpoints for Teams events and Bot Framework messages
- OAuth callback handling with auto-registration
- Admin REST API for managing instances and tenant mappings
- Health monitoring and event logging endpoints

**Admin API** (`/admin/`)

- CRUD operations for DataHub instances
- Tenant mapping management
- Unknown tenant alerts and statistics
- Health status and routing configuration

### Key Architecture Patterns

- **Strategy Pattern**: Multiple routing strategies (tenant, team, fallback)
- **Factory Pattern**: Database backend selection via configuration
- **Auto-Registration**: OAuth success automatically creates tenant mappings
- **Event Sourcing**: Unknown tenant alerts for administrative review
- **Graceful Degradation**: Fallback routing to default instances

### Development Notes

- Dependencies managed with `uv` for fast, reliable installs
- Build system integrated with DataHub's Gradle infrastructure
- Hot-reload works for code changes during development
- Database migrations handled automatically on startup
- Admin API supports both development and production workflows

### Common File Locations

- Main router: `datahub/cloud/router/core.py`
- Server endpoints: `datahub/cloud/router/server.py`
- Database interfaces: `datahub/cloud/router/db/`
- Data models: `datahub/cloud/router/db/models.py`
- CLI interface: `datahub/cloud/router/cli.py`
- Tests: `tests/` (mirrors `datahub/` structure)

## Code Quality and Development Guidelines

### Conventions

- We use standard Python `logging` and never use `print` statements for production code
- All methods should have type annotations using proper typing imports
- Database operations use proper connection management and error handling
- Async/await patterns used consistently for I/O operations
- Configuration via environment variables with sensible defaults

### Understanding Existing Code

Before making changes, always understand the multi-tenant routing logic:

- **Routing Strategies**: The system tries team-specific → tenant-specific → unknown tenant
- **Database Abstraction**: Each backend (SQLite/MySQL/InMemory) implements the same interface
- **OAuth Flow**: Auto-registration creates tenant mappings after successful OAuth
- **Error Handling**: Unknown tenants are logged for admin review rather than silently dropped

### Refactoring Best Practices

**Preserve Multi-Tenant Routing Logic:**

- Don't break the routing strategy hierarchy
- Maintain backward compatibility with existing tenant mappings
- Test routing behavior across all database backends

**Database Interface Consistency:**

- All database backends must implement the same interface methods
- Connection pooling and error handling vary by backend
- Transaction semantics should be consistent across implementations

### Writing Tests

- We use `pytest` for testing with both sync and async test support
- Tests should cover all database backends using parametrized fixtures
- Mock external HTTP calls (Teams webhooks, DataHub instances)
- Test OAuth flows and error conditions thoroughly
- MySQL tests require special setup and are skipped by default

### Common Patterns to Consider

**Multi-Tenant Routing:**

- Tenant ID extraction from various webhook payload formats
- Fallback logic when primary routing fails
- Unknown tenant tracking and alerting

**Database Operations:**

- Connection lifecycle management (especially for MySQL)
- Proper error handling and transaction rollback
- Consistent data model serialization across backends

**Configuration Management:**

- Environment variable precedence and validation
- Database connection string parsing
- Default value handling for optional settings

**Error Handling:**

- Graceful degradation when DataHub instances are unavailable
- Proper HTTP status codes for different error conditions
- Structured logging for debugging multi-tenant issues

## MySQL Development

For MySQL testing, see `tests/README_MYSQL_TESTS.md` for setup instructions. MySQL tests are skipped by default and require the `--run-mysql-tests` flag.

## Production Deployment

- Use MySQL for production databases
- Set proper connection pooling limits
- Configure health check endpoints for load balancers
- Monitor unknown tenant alerts for routing issues
- Set up proper logging and metrics collection
