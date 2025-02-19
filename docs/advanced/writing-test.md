# DataHub Testing Guide

This guide explains DataHub's testing frameworks and how to use them effectively.

## Overview

DataHub uses multiple testing frameworks depending on the component being tested:

| Test Type | Directory | Framework | Purpose |
|-----------|-----------|-----------|----------|
| Cypress tests | `smoke-test/tests/cypress` | Cypress | End-to-end testing of the web interface |
| Ingestion tests | `metadata-ingestion/tests/integration` or `metadata-ingestion/tests/unit` | pytest | Testing ingestion connectors |
| GraphQL tests | `datahub-graphql-core/src/test` | TestNG | Testing GraphQL resolvers and mappers |
| Unit tests | Various component directories | JUnit/pytest | Testing individual component functionality |

## Ingestion Tests

### When to Write Ingestion Tests
Write ingestion tests when modifying or creating ingestion connectors to verify they can successfully extract and ingest metadata into DataHub.

### Setting Up the Test Environment
First, set up your virtual environment:

```bash
cd metadata-ingestion
../gradlew :metadata-ingestion:installDev
source venv/bin/activate
datahub version  # Should show: "DataHub CLI version: unavailable (installed in develop mode)"
```

### Running Tests
Install dependencies and run tests:

```bash
# Install all development requirements
pip install -e '.[dev]'

# For integration tests only
pip install -e '.[integration-tests]'

# Run all tests
pytest -vv

# Run unit tests only
pytest -m 'not integration'

# Run Docker-based integration tests
pytest -m 'integration'
```

You can also use Gradle:
```bash
../gradlew :metadata-ingestion:lint
../gradlew :metadata-ingestion:lintFix
../gradlew :metadata-ingestion:testQuick
../gradlew :metadata-ingestion:testFull
../gradlew :metadata-ingestion:check

# Run tests from a specific file
../gradlew :metadata-ingestion:testSingle -PtestFile=tests/unit/test_bigquery_source.py

# Run all tests in tests/unit directory
../gradlew :metadata-ingestion:testSingle -PtestFile=tests/unit
```

### Updating Golden Files
To regenerate golden files for testing a specific ingestion source:

```bash
pytest tests/integration/<source>/<source>.py --update-golden-files

# Example:
pytest tests/integration/dbt/test_dbt.py --update-golden-files
```

For more details, see the [golden files update guide](../../metadata-ingestion/developing.md#updating-golden-test-files).

## GraphQL Tests

### When to Write GraphQL Tests
Write GraphQL tests when modifying:
- GraphQL types
- Resolvers
- Mappers

Your tests should cover:
- Query resolution
- Type mapping
- Error handling
- Authorization

### Running GraphQL Tests
Use Gradle to run the tests:

```bash
# Run all GraphQL tests
./gradlew :datahub-graphql-core:test

# Run a specific test
./gradlew :datahub-graphql-core:test --tests com.linkedin.datahub.graphql.resolvers.browse.BrowseV2ResolverTest
```

## Cypress Tests

### When to Write Cypress Tests
Write Cypress tests when:
- Adding new UI features
- Modifying existing UI workflows
- Testing component integration

### Writing Cypress Tests
1. Add test data to `smoke-test/tests/data.json`
2. Create or modify test files in `smoke-test/tests/cypress/e2e/cypress/`
3. Write test cases that simulate user interactions

### Running Cypress Tests Locally
1. Start DataHub:
```bash
./gradlew quickstartDebug
```

2. Launch Cypress:
```bash
./gradlew cypressDev
```

This opens the Cypress UI at `localhost:9002` where you can run and monitor tests.