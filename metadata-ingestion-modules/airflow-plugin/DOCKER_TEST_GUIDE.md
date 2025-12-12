# Docker Test Guide

This guide explains how to run Airflow plugin tests in Docker with automatic volume mounts for source code and golden files.

## Overview

The Docker test environment uses **tox** to manage all dependencies:

- ✅ Airflow (multiple versions: 2.7, 2.8, 2.9, 2.10, 3.1)
- ✅ Airflow constraints files (for reproducible builds)
- ✅ Provider packages (for Airflow 3.x)
- ✅ All test dependencies
- ✅ **Automatic volume mounts** for source code and golden files

**Three ways to run tests:**

1. **Wrapper Script** (easiest) - `./run-tests.sh`
2. **Docker Compose** (recommended for CI/CD)
3. **Docker CLI** (most control)

## Quick Start

### Option 1: Wrapper Script (Easiest)

The wrapper script automatically handles volume mounts and permissions:

```bash
cd metadata-ingestion-modules/airflow-plugin

# Run all tests with Airflow 3.1 (default)
./run-tests.sh

# Run tests with Airflow 2.9
./run-tests.sh py311-airflow29

# Run specific test
./run-tests.sh py311-airflow31 -- tests/integration/test_plugin.py::test_v2_basic_dag -v

# Update golden files (automatically mounted!)
./run-tests.sh py311-airflow31 -- --update-golden-files

# Rebuild Docker image before running
REBUILD=true ./run-tests.sh
```

**Benefits:**

- ✅ Automatic volume mounts (source code + golden files)
- ✅ Correct user permissions (no root-owned files)
- ✅ Auto-builds image if needed
- ✅ Simple, easy-to-remember commands

### Option 2: Docker Compose (Recommended)

Docker Compose automatically mounts volumes and caches tox environments:

```bash
cd metadata-ingestion-modules/airflow-plugin

# Run all tests with Airflow 3.1 (default)
docker-compose -f docker-compose.test.yml run --rm airflow-plugin-test

# Run tests with Airflow 2.9
docker-compose -f docker-compose.test.yml run --rm airflow-plugin-test py311-airflow29

# Run specific test
docker-compose -f docker-compose.test.yml run --rm airflow-plugin-test py311-airflow31 -- tests/integration/test_plugin.py::test_v2_basic_dag -v

# Update golden files (automatically mounted!)
docker-compose -f docker-compose.test.yml run --rm airflow-plugin-test py311-airflow31 -- --update-golden-files

# Build the image
docker-compose -f docker-compose.test.yml build
```

**Benefits:**

- ✅ Automatic volume mounts (source code + golden files)
- ✅ Persistent tox cache (faster subsequent runs)
- ✅ Easy to customize via docker-compose.test.yml
- ✅ Great for CI/CD

### Option 3: Docker CLI (Most Control)

If you need full control or don't want the wrapper:

```bash
# Build from repository root
cd /path/to/datahub
docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test -t airflow-plugin-test .

# Run all tests with Airflow 3.1 (default)
docker run --rm airflow-plugin-test

# Run tests with Airflow 2.9
docker run --rm airflow-plugin-test py311-airflow29

# Run specific test
docker run --rm airflow-plugin-test py311-airflow31 -- tests/integration/test_plugin.py::test_v2_basic_dag -v

# Update golden files (manual volume mount)
docker run --rm \
  -v $(pwd)/metadata-ingestion-modules/airflow-plugin:/app/metadata-ingestion-modules/airflow-plugin \
  airflow-plugin-test py311-airflow31 -- --update-golden-files
```

## Building the Image

### Default Build (Python 3.11, Airflow 3.1)

```bash
# Must be run from repository root
docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test -t airflow-plugin-test .
```

### Custom Python Version

```bash
docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test \
  --build-arg PYTHON_VERSION=3.10 \
  -t airflow-plugin-test:py310 .
```

### Custom Default Tox Environment

```bash
docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test \
  --build-arg TOX_ENV=py311-airflow29 \
  -t airflow-plugin-test:af29 .
```

## Running Tests

### Available Tox Environments

From `tox.ini`:

- `py39-airflow27` - Python 3.9, Airflow 2.7
- `py310-airflow27` - Python 3.10, Airflow 2.7
- `py310-airflow28` - Python 3.10, Airflow 2.8
- `py311-airflow29` - Python 3.11, Airflow 2.9
- `py311-airflow210` - Python 3.11, Airflow 2.10
- `py311-airflow31` - Python 3.11, Airflow 3.1 (default)

### Run All Tests

```bash
# With default environment (py311-airflow31)
docker run airflow-plugin-test

# With specific environment
docker run airflow-plugin-test py311-airflow29
```

### Run Specific Test File

```bash
docker run airflow-plugin-test py311-airflow31 -- tests/integration/test_plugin.py -v
```

### Run Specific Test Function

```bash
docker run airflow-plugin-test py311-airflow31 -- tests/integration/test_plugin.py::test_v2_snowflake_operator_airflow3 -v
```

### Run Tests Matching Pattern

```bash
docker run airflow-plugin-test py311-airflow31 -- -k "snowflake" -v
```

### Run with Additional pytest Options

```bash
# Stop on first failure
docker run airflow-plugin-test py311-airflow31 -- -x

# Show local variables in tracebacks
docker run airflow-plugin-test py311-airflow31 -- -l

# Verbose output
docker run airflow-plugin-test py311-airflow31 -- -vv

# Show captured output
docker run airflow-plugin-test py311-airflow31 -- -s
```

## Updating Golden Files

Golden files are **automatically mounted** when using the wrapper script or docker-compose!

### With Wrapper Script (Easiest)

```bash
# Update all golden files
./run-tests.sh py311-airflow31 -- --update-golden-files

# Update golden files for specific test
./run-tests.sh py311-airflow31 -- tests/integration/test_plugin.py::test_v2_snowflake_operator_airflow3 --update-golden-files
```

**That's it!** Golden files are automatically written to your local filesystem.

### With Docker Compose

```bash
# Update all golden files
docker-compose -f docker-compose.test.yml run --rm airflow-plugin-test py311-airflow31 -- --update-golden-files

# Update golden files for specific test
docker-compose -f docker-compose.test.yml run --rm airflow-plugin-test py311-airflow31 -- tests/integration/test_plugin.py::test_v2_snowflake_operator_airflow3 --update-golden-files
```

### With Docker CLI (Manual Volume Mount)

If you're using raw Docker commands, you need to mount the golden files directory:

```bash
# From repository root
docker run --rm \
  -v $(pwd)/metadata-ingestion-modules/airflow-plugin:/app/metadata-ingestion-modules/airflow-plugin \
  airflow-plugin-test py311-airflow31 -- --update-golden-files

# Or just mount the goldens directory
docker run --rm \
  -v $(pwd)/metadata-ingestion-modules/airflow-plugin/tests/integration/goldens:/app/metadata-ingestion-modules/airflow-plugin/tests/integration/goldens \
  airflow-plugin-test py311-airflow31 -- --update-golden-files
```

## Advanced Usage

### Interactive Shell

```bash
docker run -it --entrypoint /bin/bash airflow-plugin-test

# Inside container:
cd metadata-ingestion-modules/airflow-plugin
tox -e py311-airflow31 -- -v
```

### Run Tox Directly (Without Entrypoint)

```bash
docker run --entrypoint tox airflow-plugin-test -e py311-airflow29 -- tests/integration/ -v
```

### Volume Mount for Development

Mount your local code for live editing:

```bash
docker run -v $(pwd):/app airflow-plugin-test py311-airflow31 -- tests/integration/ -v
```

**Note:** You may need to rebuild the tox environment after code changes:

```bash
docker run -v $(pwd):/app airflow-plugin-test py311-airflow31 --recreate
```

### Override Default Tox Environment

```bash
# Set different default at runtime
docker run -e TOX_ENV=py311-airflow29 airflow-plugin-test
```

## Testing Different Airflow Versions

### Airflow 2.7

```bash
docker run airflow-plugin-test py310-airflow27
```

### Airflow 2.9

```bash
docker run airflow-plugin-test py311-airflow29
```

### Airflow 3.1

```bash
docker run airflow-plugin-test py311-airflow31
```

## Troubleshooting

### Build Context Issues

The Dockerfile must be run from the repository root because it needs access to `metadata-ingestion`:

```bash
# ✅ Correct
cd /path/to/datahub
docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test -t airflow-plugin-test .

# ❌ Wrong (will fail - can't find ../../metadata-ingestion)
cd /path/to/datahub/metadata-ingestion-modules/airflow-plugin
docker build -f Dockerfile.test -t airflow-plugin-test .
```

### Tox Cache Issues

If you encounter stale dependencies:

```bash
# Rebuild tox environment
docker run airflow-plugin-test py311-airflow31 --recreate

# Or rebuild Docker image with no cache
docker build --no-cache -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test -t airflow-plugin-test .
```

### Python Version Mismatch

Ensure the Python version in the Docker build matches the tox environment:

```bash
# For py310-* environments
docker build --build-arg PYTHON_VERSION=3.10 \
  -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test \
  -t airflow-plugin-test:py310 .

docker run airflow-plugin-test:py310 py310-airflow28
```

### Permission Issues with Golden Files

When using volume mounts:

```bash
# Run with current user's UID/GID
docker run --user $(id -u):$(id -g) \
  -v $(pwd)/metadata-ingestion-modules/airflow-plugin/tests/integration/goldens:/app/metadata-ingestion-modules/airflow-plugin/tests/integration/goldens \
  airflow-plugin-test py311-airflow31 -- --update-golden-files
```

### Out of Memory

Tox may use significant memory when building environments:

```bash
docker run --memory=4g airflow-plugin-test
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Test Airflow Plugin
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        tox-env:
          - py310-airflow27
          - py310-airflow28
          - py311-airflow29
          - py311-airflow210
          - py311-airflow31

    steps:
      - uses: actions/checkout@v3

      - name: Build test image
        run: |
          docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test \
            -t airflow-plugin-test .

      - name: Run tests
        run: docker run airflow-plugin-test ${{ matrix.tox-env }}
```

### GitLab CI Example

```yaml
test:
  image: docker:latest
  services:
    - docker:dind
  script:
    - docker build -f metadata-ingestion-modules/airflow-plugin/Dockerfile.test -t airflow-plugin-test .
    - docker run airflow-plugin-test $TOX_ENV
  parallel:
    matrix:
      - TOX_ENV:
          - py310-airflow27
          - py310-airflow28
          - py311-airflow29
          - py311-airflow31
```

## How It Works

### Tox Configuration

The `tox.ini` file defines all test environments with:

- Airflow version constraints
- Official Airflow constraints files for reproducible builds
- Provider packages for Airflow 3.x
- All necessary dependencies

### Entrypoint Logic

The entrypoint script intelligently routes commands:

```bash
# No args → run default tox environment
docker run airflow-plugin-test
# Executes: tox -e $TOX_ENV

# Tox env specified → run that environment
docker run airflow-plugin-test py311-airflow29
# Executes: tox -e py311-airflow29

# Other args → pass to pytest via default environment
docker run airflow-plugin-test -- -k snowflake -v
# Executes: tox -e $TOX_ENV -- -k snowflake -v

# Tox env + pytest args
docker run airflow-plugin-test py311-airflow31 -- tests/integration/test_plugin.py -v
# Executes: tox -e py311-airflow31 -- tests/integration/test_plugin.py -v
```

## Comparison: Docker vs Local Tox

| Aspect                 | Docker                         | Local Tox                   |
| ---------------------- | ------------------------------ | --------------------------- |
| **Setup**              | Build once, run anywhere       | Requires local Python setup |
| **Reproducibility**    | Guaranteed (same OS, packages) | Varies by local environment |
| **CI/CD**              | Native support                 | Needs Python pre-installed  |
| **Speed (first run)**  | Slower (Docker build)          | Slower (tox setup)          |
| **Speed (subsequent)** | Fast if cached                 | Fast if cached              |
| **Disk Usage**         | Higher (Docker layers)         | Lower                       |
| **Isolation**          | Complete (OS level)            | Python environment only     |
| **Golden Files**       | Volume mounts needed           | Direct access               |

**Use Docker for:**

- CI/CD pipelines
- Consistent cross-platform testing
- Complete environment isolation
- Sharing test environments

**Use Local Tox for:**

- Day-to-day development
- Faster iteration
- Direct file access
- Debugging

## Best Practices

1. **Always build from repository root** - The Dockerfile needs access to `metadata-ingestion`
2. **Use volume mounts for golden files** - Makes it easy to extract updated files
3. **Match Python versions** - Build arg should match tox environment (py310 → PYTHON_VERSION=3.10)
4. **Cache Docker layers** - Organize changes to maximize layer reuse
5. **Clean up** - Remove unused images: `docker image prune`
6. **Use specific tox envs** - Don't rely on defaults in CI/CD

## Support

For issues:

- **Tox configuration**: See `tox.ini` in the airflow-plugin directory
- **Docker logs**: `docker logs <container-id>`
- **Build output**: `docker build --progress=plain`
- **Interactive debugging**: `docker run -it --entrypoint /bin/bash airflow-plugin-test`
