# Docker Test Environment

This directory contains a complete Docker-based test environment for the Airflow plugin.

## Files

- **`Dockerfile.test`** - Docker image that runs tests via tox
- **`docker-compose.test.yml`** - Docker Compose config with automatic volume mounts
- **`run-tests.sh`** - Wrapper script for the easiest test experience
- **`DOCKER_TEST_GUIDE.md`** - Complete documentation with examples

## Quick Start

### Easiest: Use the Wrapper Script

```bash
# Run all tests
./run-tests.sh

# Run specific test
./run-tests.sh py311-airflow31 -- tests/integration/test_plugin.py::test_v2_basic_dag -v

# Update golden files (automatically saved to your local filesystem!)
./run-tests.sh py311-airflow31 -- --update-golden-files
```

**Benefits:**

- ✅ Automatic volume mounts (no manual `-v` flags)
- ✅ Correct file permissions (no root-owned files)
- ✅ Auto-builds image if needed
- ✅ Simple to use

### Alternative: Docker Compose

```bash
# Run all tests
docker compose -f docker-compose.test.yml run --rm airflow-plugin-test

# Update golden files
docker compose -f docker-compose.test.yml run --rm airflow-plugin-test
```

## Features

### Automatic Volume Mounts

Both the wrapper script and Docker Compose automatically mount:

- **Source code** (`metadata-ingestion` and `airflow-plugin`)
- **Golden files** (`tests/integration/goldens`)
- **Tox cache** (`.tox` directory - Docker Compose only)

This means:

- ✅ No manual volume mount commands
- ✅ Golden files are automatically updated on your local filesystem
- ✅ Source code changes are immediately available
- ✅ Tox environments are cached between runs (Docker Compose)

### Tox-Based Dependency Management

The Docker image uses **tox** to install all dependencies:

- Airflow versions (2.7, 2.8, 2.9, 2.10, 3.1)
- Airflow constraints files (for reproducible builds)
- Provider packages (Airflow 3.x)
- All test dependencies

This ensures:

- ✅ No duplicate dependency logic
- ✅ Same dependencies as local development
- ✅ Automatic constraint file handling

### Multiple Airflow Versions

Test against any Airflow version in one image:

```bash
./run-tests.sh py310-airflow27  # Airflow 2.7
./run-tests.sh py311-airflow29  # Airflow 2.9
./run-tests.sh py311-airflow31  # Airflow 3.1
```

## Why Use Docker?

| Use Case                     | Recommended Approach            |
| ---------------------------- | ------------------------------- |
| **Local development**        | Local tox (faster iteration)    |
| **CI/CD pipelines**          | Docker (complete isolation)     |
| **Cross-platform testing**   | Docker (consistent environment) |
| **Sharing test environment** | Docker (works everywhere)       |
| **Testing on different OS**  | Docker (same Linux base)        |

## Documentation

See **`DOCKER_TEST_GUIDE.md`** for:

- Detailed usage examples
- All three methods (wrapper, compose, CLI)
- Advanced configuration
- CI/CD integration examples
- Troubleshooting guide

## Quick Reference

```bash
# Wrapper Script (easiest)
./run-tests.sh                                              # All tests
./run-tests.sh py311-airflow29                              # Airflow 2.9
./run-tests.sh py311-airflow31 -- -k snowflake -v           # Specific tests
./run-tests.sh py311-airflow31 -- --update-golden-files     # Update golden files
REBUILD=true ./run-tests.sh                                 # Force rebuild

# Docker Compose (recommended for CI/CD)
docker compose -f docker-compose.test.yml run --rm airflow-plugin-test
docker compose -f docker-compose.test.yml run --rm airflow-plugin-test py311-airflow29
docker compose -f docker-compose.test.yml build

# Docker CLI (most control)
docker build -f Dockerfile.test -t airflow-plugin-test ../../
docker run --rm airflow-plugin-test
docker run --rm airflow-plugin-test py311-airflow31 -- -v
```

## Support

- **Issues with tests**: See `tox.ini` and test files
- **Issues with Docker**: See `DOCKER_TEST_GUIDE.md`
- **Issues with dependencies**: See `setup.py` and tox configuration
