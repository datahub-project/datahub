# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Commands

**Build and test:**

```bash
./gradlew build           # Build entire project
./gradlew check           # Run all tests and linting

# Note that each directory typically has a build.gradle file, but the available tasks follow similar conventions.

# Java code.
./gradlew spotlessApply   # Java code formatting

# Python code.
./gradlew :metadata-ingestion:testQuick     # Fast Python unit tests
./gradlew :metadata-ingestion:testQuick     # Fast Python unit tests
./gradlew :metadata-ingestion:lint          # Python linting (ruff, mypy)
```

**Development setup:**

```bash
./gradlew :metadata-ingestion:installDev               # Setup Python environment
./gradlew quickstartDebug                              # Start full DataHub stack
cd datahub-web-react && yarn start                     # Frontend dev server
```

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

### Metadata Model Concepts

- **Entities**: Core objects (Dataset, Dashboard, Chart, CorpUser, etc.)
- **Aspects**: Metadata facets (Ownership, Schema, Documentation, etc.)
- **URNs**: Unique identifiers (`urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)`)
- **MCE/MCL**: Metadata Change Events/Logs for updates
- **Entity Registry**: YAML config defining entity-aspect relationships (`metadata-models/src/main/resources/entity-registry.yml`)

## Development Flow

1. **Schema changes** in `metadata-models/` trigger code generation across all languages
2. **Backend changes** in `metadata-service/` and other Java modules expose new REST/GraphQL APIs
3. **Frontend changes** in `datahub-web-react/` consume GraphQL APIs
4. **Ingestion changes** in `metadata-ingestion/` emit metadata to backend APIs

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

### Testing Strategy

- Python: Tests go in the `tests/` directory alongside `src/`, use `assert` statements
- Java: Tests alongside source in `src/test/`
- Frontend: Tests in `__tests__/` or `.test.tsx` files
- Smoke tests go in the `smoke-test/` directory

## Key Documentation

**Essential reading:**

- `docs/architecture/architecture.md` - System architecture overview
- `docs/modeling/metadata-model.md` - How metadata is modeled
- `docs/what-is-datahub/datahub-concepts.md` - Core concepts (URNs, entities, etc.)

**External docs:**

- https://docs.datahub.com/docs/developers - Official developer guide
- https://demo.datahub.com/ - Live demo environment

## Important Notes

- Entity Registry is defined in YAML, not code (`entity-registry.yml`)
- All metadata changes flow through the event streaming system
- GraphQL schema is generated from backend GMS APIs
- Follow Conventional Commits format for commit messages
