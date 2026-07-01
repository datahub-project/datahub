# Docker Test Guide

Run Airflow plugin tests in Docker with automatic volume mounts for source and
golden files.

## Overview

The Docker environment runs `tox -e py311-airflow31` by default. Three tox envs
are supported — `py311-airflow30`, `py311-airflow31`, and `py311-airflow32`
(Airflow 2.x is no longer supported) — and you can select one as the first
argument (see examples below). Each env installs the matching `apache-airflow~=3.x`
line plus its constraints file and provider packages.

Three entry points, in increasing order of control:

1. **Wrapper script** — `./run-tests.sh`
2. **Docker Compose** — `docker-compose.test.yml`
3. **Docker CLI** — raw `docker build` / `docker run`

## Quick Start

### Wrapper script

```bash
cd metadata-ingestion-modules/airflow-plugin

./run-tests.sh                                                          # all tests
./run-tests.sh py311-airflow31 -- tests/integration/test_plugin.py -v   # specific file
./run-tests.sh py311-airflow31 -- --update-golden-files                 # rewrite goldens
REBUILD=true ./run-tests.sh                                              # force image rebuild
```

The wrapper handles volume mounts and user permissions automatically.

### Docker Compose

```bash
docker-compose -f docker-compose.test.yml run --rm airflow-plugin-test
docker-compose -f docker-compose.test.yml run --rm airflow-plugin-test py311-airflow31 -- --update-golden-files
docker-compose -f docker-compose.test.yml build
```

Compose persists the `.tox` cache between runs, so subsequent invocations are
much faster.

### Docker CLI

```bash
# Build from repository root (needs access to ../../metadata-ingestion)
cd /path/to/datahub
docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test -t airflow-plugin-test .

docker run --rm airflow-plugin-test
docker run --rm airflow-plugin-test py311-airflow31 -- tests/integration/test_plugin.py::test_airflow_plugin -v

# Updating goldens requires a manual mount
docker run --rm \
  -v $(pwd)/metadata-ingestion-modules/airflow-plugin/tests/integration/goldens:/app/metadata-ingestion-modules/airflow-plugin/tests/integration/goldens \
  airflow-plugin-test py311-airflow31 -- --update-golden-files
```

## Common pytest patterns

```bash
docker run airflow-plugin-test py311-airflow31 -- -k "snowflake" -v   # match by name
docker run airflow-plugin-test py311-airflow31 -- -x                  # stop on first failure
docker run airflow-plugin-test py311-airflow31 -- -s                  # show captured stdout
```

## Updating golden files

The wrapper script and Compose mount the goldens dir; just pass
`--update-golden-files`:

```bash
./run-tests.sh py311-airflow31 -- --update-golden-files
docker-compose -f docker-compose.test.yml run --rm airflow-plugin-test py311-airflow31 -- --update-golden-files
```

With raw Docker CLI you have to mount the dir yourself (see above).

## Building

```bash
# Default: Python 3.11, Airflow 3.1
docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test -t airflow-plugin-test .

# Custom Python version
docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test \
  --build-arg PYTHON_VERSION=3.11 \
  -t airflow-plugin-test .
```

## Troubleshooting

**Build context** — must run from the repo root; the Dockerfile reaches up
into `metadata-ingestion`.

```bash
# correct
cd /path/to/datahub
docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test -t airflow-plugin-test .
```

**Stale tox env** — `docker run airflow-plugin-test py311-airflow31 --recreate`
or rebuild the image with `--no-cache`.

**Permissions on updated goldens** — pass `--user $(id -u):$(id -g)` when
using raw Docker CLI (the wrapper does this for you).

**OOM during tox install** — give the container more memory:
`docker run --memory=4g airflow-plugin-test`.

## When to use Docker

| Aspect          | Docker                   | Local Tox                   |
| --------------- | ------------------------ | --------------------------- |
| Setup           | Build once, run anywhere | Requires local Python setup |
| Reproducibility | Guaranteed               | Varies                      |
| CI/CD           | Native                   | Needs Python preinstalled   |
| First-run speed | Slower (image build)     | Faster                      |
| Subsequent runs | Fast if cached           | Fast if cached              |
| Disk usage      | Higher                   | Lower                       |

Use Docker for CI, cross-platform testing, or sharing a reproducible
environment. Use local tox for day-to-day development.

## How it works

The entrypoint script routes commands:

```bash
docker run airflow-plugin-test                              # → tox -e py311-airflow31
docker run airflow-plugin-test py311-airflow31              # → tox -e py311-airflow31
docker run airflow-plugin-test -- -k snowflake -v           # → tox -e py311-airflow31 -- -k snowflake -v
docker run airflow-plugin-test py311-airflow31 -- -v        # → tox -e py311-airflow31 -- -v
```

## CI/CD example

```yaml
name: Test Airflow Plugin
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build test image
        run: docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test -t airflow-plugin-test .
      - name: Run tests
        run: docker run airflow-plugin-test py311-airflow31
```
