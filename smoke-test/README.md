# DataHub Smoke Tests

This directory contains end-to-end smoke tests for DataHub functionality. These tests can be run locally for faster development and debugging compared to the full CI pipeline.

> 📖 **Official Documentation**: For additional setup details and troubleshooting, see the [Running smoke tests locally](https://www.notion.so/acryldata/Running-smoke-tests-locally-23ffc6a6427780df8f22dd0a4e57d793) guide.

## Quick Start

### Prerequisites

1. **DataHub must be running locally**

   ```bash
   # From project root
   ./gradlew quickstartDebug
   ```

2. **Set up Python environment** (one-time setup)

   ```bash
   # From project root - sets up metadata-ingestion venv
   ./gradlew :metadata-ingestion:installDev

   # Set up smoke-test specific environment
   cd smoke-test
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip wheel setuptools
   pip install -r requirements.txt
   ```

### Environment Variables

```bash
export DATAHUB_VERSION=v1.0.0rc3-SNAPSHOT  # or current version
export TEST_STRATEGY=no_cypress_suite0     # for non-Cypress tests
```

### Running Tests

```bash
cd smoke-test
source venv/bin/activate

# Set environment variables
export DATAHUB_VERSION=v1.0.0rc3-SNAPSHOT
export TEST_STRATEGY=no_cypress_suite0

# Run all tests (WARNING: Takes a long time, requires full setup)
pytest -vv

# Run specific test file (RECOMMENDED for development)
pytest test_system_info.py -vv

# Run specific test method
pytest test_system_info.py::test_system_info_main_endpoint -vv

# Run multiple specific tests
pytest test_e2e.py::test_healthchecks test_e2e.py::test_gms_usage_fetch -v
```

## Test Categories

### System Info Tests (`test_system_info.py`)

**✅ FAST - Can run independently**

Tests the system info API endpoints:

- `/openapi/v1/system-info` - Spring components only
- `/openapi/v1/system-info/properties` - Detailed properties
- `/openapi/v1/system-info/spring-components` - Component status

```bash
# Run all system info tests (takes ~30 seconds)
pytest test_system_info.py -vv
```

### Core E2E Tests (`test_e2e.py`)

**⚠️ SLOW - Requires full ingestion pipeline**

Tests that require data ingestion and full DataHub functionality. Many tests depend on the initial ingestion fixture which can fail if Kafka/Schema Registry aren't properly configured.

```bash
# Run health checks only (fast)
pytest test_e2e.py::test_healthchecks -v

# Run authentication tests (fast)
pytest test_e2e.py::test_frontend_auth -v

# Run full e2e tests (slow, requires full setup)
pytest test_e2e.py -vv
```

## Development Workflow

### Testing System Info Changes

After making changes to system info APIs:

1. **Restart DataHub**

   ```bash
   # Kill existing processes
   ./gradlew :datahub-frontend:stop :datahub-gms:stop

   # Restart
   ./gradlew quickstartDebug
   ```

2. **Run System Info Tests**

   ```bash
   cd smoke-test
   source venv/bin/activate
   export DATAHUB_VERSION=v1.0.0rc3-SNAPSHOT
   export TEST_STRATEGY=no_cypress_suite0

   pytest test_system_info.py -vv
   ```

### Quick API Verification

```bash
# Check if DataHub is running
curl -s http://localhost:8080/health | head -5

# Test system info endpoint directly
curl -s http://localhost:8080/openapi/v1/system-info | jq . | head -20
```

## Troubleshooting

### Common Issues

**❌ "Connection refused" errors**

- DataHub is not running
- Wrong port (should be 8080 for GMS)
- Services still starting up (wait a few minutes)

**❌ "401 Unauthorized" for direct curl**

- Expected behavior - tests handle authentication
- Use the test suite instead of direct curl for authenticated endpoints

**❌ Kafka/Schema Registry connection errors**

- Only affects full e2e tests with ingestion
- System info tests should still work
- Try running individual test methods instead of full suite

**❌ Python environment issues**

```bash
# Recreate virtual environment
rm -rf venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Environment Debug

```bash
# Check if services are running
curl -s http://localhost:8080/health
curl -s http://localhost:9092  # Kafka (will show connection refused if not running)

# Verify Python environment
source venv/bin/activate
which python
python --version
pip list | grep datahub

# Check environment variables
echo "DATAHUB_VERSION: $DATAHUB_VERSION"
echo "TEST_STRATEGY: $TEST_STRATEGY"
```

## CI vs Local Testing

- **CI**: Uses `./gradlew :smoke-test:pytest` - full pipeline with Docker containers
- **Local**: Uses direct pytest - faster, uses locally running DataHub instance
- **Recommendation**: Use local for development, CI for final validation

## Test Organization

- `test_e2e.py` - Main test suite (1387 lines)
- `test_system_info.py` - System info API tests (169 lines)
- `conftest.py` - Test configuration and fixtures
- `tests/utils.py` - Test utilities and helpers

---

💡 **Pro Tip**: For rapid development, use `pytest test_system_info.py -vv` which runs in ~30 seconds vs the full test suite which can take 30+ minutes.
