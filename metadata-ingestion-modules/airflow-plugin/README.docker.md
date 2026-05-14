# Docker Test Environment

Docker-based test environment for the Airflow plugin. The only supported tox
env is `py311-airflow31` (Airflow 2.x and 3.0.x are no longer supported).

## Files

- **`Dockerfile.test`** — Docker image that runs tests via tox
- **`docker-compose.test.yml`** — Docker Compose config with automatic volume mounts
- **`run-tests.sh`** — Wrapper script for the easiest test experience
- **`DOCKER_TEST_GUIDE.md`** — Full documentation with examples

## Quick Start

### Wrapper script

```bash
# Run all tests
./run-tests.sh

# Run a specific test
./run-tests.sh py311-airflow31 -- tests/integration/test_plugin.py::test_airflow_plugin -v

# Update golden files (saved back to your local filesystem)
./run-tests.sh py311-airflow31 -- --update-golden-files
```

### Docker Compose

```bash
docker compose -f docker-compose.test.yml run --rm airflow-plugin-test
```

## Features

### Automatic volume mounts

Both the wrapper script and Docker Compose mount:

- Source (`metadata-ingestion` and `airflow-plugin`)
- Golden files (`tests/integration/goldens`)
- Tox cache (`.tox`, Docker Compose only)

Golden files get updated on your local filesystem; source changes are visible
to the container immediately.

### Tox-based dependency management

The image runs tests via `tox -e py311-airflow31`, which installs
`apache-airflow~=3.1.0` and the providers/constraint file pinned in `tox.ini`.

## When to use Docker

| Use Case                     | Recommended Approach            |
| ---------------------------- | ------------------------------- |
| Local development            | Local tox (faster iteration)    |
| CI/CD pipelines              | Docker (complete isolation)     |
| Cross-platform testing       | Docker (consistent environment) |

## More

See **`DOCKER_TEST_GUIDE.md`** for advanced usage, CI/CD examples, and
troubleshooting.
