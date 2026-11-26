# DataHub Smoke Tests

This directory contains end-to-end smoke tests for DataHub functionality. These tests can be run locally for faster development and debugging compared to the full CI pipeline.

**Writing tests:** follow [`AGENTS.md`](AGENTS.md) (isolation, fixtures, markers, cleanup).
**Running tests:** this README, or from the repo root
`scripts/dev/datahub-dev.sh test <test-path>` (preferred over `./gradlew quickstartDebug`).

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
export TEST_STRATEGY=pytests
```

On the Docker-free stack (`scripts/dev/datahub-dev.sh start --local`), `datahub-dev.sh test <path>`
takes the test env from the stack's `stack.json`:
GMS, frontend, OpenSearch and running-service URLs, the index prefix and usage index, the admin
login (no `DATAHUB_GMS_TOKEN`; tests log in as `datahub`/`datahub`) and `SMOKE_BACKEND=local`.
To run pytest or Playwright directly, first run
`eval "$(scripts/dev/datahub-dev.sh shell-env --tests)"` from the repo root.

### Running Tests

```bash
cd smoke-test
source venv/bin/activate

# Set environment variables
export DATAHUB_VERSION=v1.0.0rc3-SNAPSHOT
export TEST_STRATEGY=pytests

# Run all live-stack tests (WARNING: Takes a long time, requires full setup)
pytest -vv

# Run specific test file (RECOMMENDED for development)
pytest tests/e2e/test_system_info.py -vv

# Run specific test method
pytest tests/e2e/test_system_info.py::test_system_info_main_endpoint -vv

# Run multiple specific tests
pytest tests/e2e/test_e2e.py::test_healthchecks tests/e2e/test_e2e.py::test_gms_usage_fetch -v

# CPU tests (no DataHub stack)
./gradlew :smoke-test:stackFreePytest
# or: pytest tests/unit -vv
```

#### Selecting tests by domain

Tests can declare the product domain that owns them with
`@pytest.mark.domain(...)`, using the `Domain` enum in
`utilities/domains.py` (`platform`, `observe`, `ingestion`, `ai`,
`catalog`). The `--domain` option then runs only the tests those domains own:

```bash
# One domain
pytest --domain catalog -vv

# Several — a test owned by any of them runs
pytest --domain catalog --domain ingestion -vv
```

A test that spans domains declares all of them, e.g.
`@pytest.mark.domain(Domain.CATALOG, Domain.INGESTION)`, and is selected by
either.

#### Selecting tests by criticality tier

Tests critical enough to gate every pull request carry `@pytest.mark.p0`. Select
them with an ordinary marker expression:

```bash
# Only the p0 tier
pytest -m p0 -vv

# The p0 tier within one domain
pytest -m p0 --domain catalog -vv
```

CI drives the choice through the `SMOKE_TIER` env var that `smoke.sh` reads:
`docker-unified.yml` sets it to `p0` for pull requests while the
`PYTEST_P0_SMOKE` repository variable is `true`, and leaves post-merge runs on
the full suite. Repository variables don't reach pull requests from forks, so
those always run the full suite.

On a pull request CI additionally runs any test module the PR itself touches,
even when it carries no `p0` marker — conftest marks those modules p0 during
collection so `-m p0` keeps them. That is per-module: a change to a shared
fixture or to `conftest.py` pulls in no test module of its own, so a PR needing
broader coverage asks for the whole suite with the `run-all-tests` PR label.

Add `run-all-tests` to a pull request to run every test on it instead of the `p0`
tier — for changes whose blast radius is wider than their own touched modules,
such as a shared fixture, `conftest.py`, or a broad refactor. `run-all-tests`
also turns off the Playwright P0 tier; `run-all-tests:pytest` runs every pytest
test and leaves Playwright on its tier. Both labels are read
from the event payload, as every other label this CI honours is, so they apply
on the PR's next push; re-running an existing workflow replays the original
payload and will not see them.

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
   export TEST_STRATEGY=pytests

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

### Pointing the tests at a non-default stack

The tests read service addresses and index names from these variables. Each defaults to the
Docker quickstart value, so set them only when you run against another stack, such as a
Docker-free one:

| Variable               | Docker default                            |
| ---------------------- | ----------------------------------------- |
| `DATAHUB_GMS_URL`      | `http://localhost:8080`                   |
| `DATAHUB_FRONTEND_URL` | `http://localhost:9002`                   |
| `ELASTICSEARCH_URL`    | `http://localhost:9200`                   |
| `INDEX_PREFIX`         | none                                      |
| `ELASTICSEARCH_INDEX`  | `datahub_usage_event`                     |
| `SMOKE_BACKEND`        | `docker` (`local` skips Kafka-only waits) |

### Simulate batch packing without running tests

CI splits pytests with `BATCH_COUNT` (5 on forks, 3 for the `p0` tier, 5 for the full suite) and `PYTEST_XDIST_WORKERS=3`. To dump the predicted pack plan for one of those counts without executing tests, load the opt-in plugin with `-p` (it is not registered in `conftest.py`):

```bash
cd smoke-test
source venv/bin/activate
export PYTHONPATH=.
PLUGIN="-p batch_plan_dump"

# Full suite, 5 batches
BATCH_COUNT=5 PYTEST_XDIST_WORKERS=3 \
  pytest $PLUGIN --collect-only --dump-batch-plan /tmp/plan-5.json

# p0 gate, 3 batches
BATCH_COUNT=3 PYTEST_XDIST_WORKERS=3 \
  pytest $PLUGIN --collect-only -m p0 --dump-batch-plan /tmp/plan-p0-3.json

# CI: artifact path from env (overrides --dump-batch-plan)
SMOKE_DUMP_BATCH_PLAN=/tmp/plan-catalog-3.json BATCH_COUNT=3 PYTEST_XDIST_WORKERS=3 \
  pytest $PLUGIN --collect-only --domain catalog
```

`PYTHONPATH=.` is required: pytest loads `-p` plugins before the smoke-test directory is on `sys.path`. Use `-p batch_plan_dump` from `smoke-test/` (loader next to `shard_pack.py`). The dump logic lives in `utilities/batch_plan_dump.py`.

`--collect-only` is required if you do not want tests to run; collection still imports the suite and needs this venv, but it does not start DataHub. The plugin also clears collected items as a backstop. Compare shard counts by invoking the same command again with a different `BATCH_COUNT`. If the plugin is loaded and neither `SMOKE_DUMP_BATCH_PLAN` nor `--dump-batch-plan` is set, JSON is written to stdout.

### Plan a smart-selected batch matrix

Under active smart testing, CI plans the matrix with `--size-batches 9`. The
plugin applies `SMOKE_SMART_TAGS` and picks the fewest batches (at most 9)
whose predicted pytest time is under 8 minutes, or under the slowest single
loadscope when that alone takes longer (a scope never splits across workers).
`SMOKE_PLAN_TARGET_BATCH_SECONDS` overrides the 8 minute target. It ignores
`BATCH_COUNT`:

```bash
SMOKE_SMART_TAGS='{"feature-user": ["documentation"]}' PYTEST_XDIST_WORKERS=3 \
  pytest $PLUGIN --collect-only \
  --size-batches 9 --dump-batch-plan /tmp/plan.json

# Collect what batch 0 of that plan runs
SMOKE_BATCH_PLAN=/tmp/plan.json BATCH_NUMBER=0 \
  pytest --collect-only -q
```

### Env parity with the Docker-free stack

The Docker quickstart reads `smoke.common.env`, the compose and GMS image env, and the inline flags
in `run-quickstart.sh`. The Docker-free stack (`datahub-dev.sh start --local --job smoke-test`)
reads a copy of them in `smoke-test/env/` and `env/common/` (see [env/README.md](../env/README.md)).
When you change a smoke value, change it in both places, then run:

```shell
python3 smoke-test/check_env_parity.py
```

It fails on a difference not listed in `env-parity-allowlist.txt`, and on an allowlist line that
no longer matches a difference. CI runs it in report-only mode in the GMS-lite Local Stack workflow.

### The Docker-free pytest lane in CI

`.github/workflows/docker-free-e2e.yml` runs the full pytest suite on the Docker-free stack, in
9 batches. One job builds the jars, the embedded OpenSearch node and the frontend bundle; each
batch boots its own stack with the `.github/actions/docker-free-stack` action and runs `smoke.sh`
with `SMOKE_BACKEND=local`, which deselects the tests marked `requires(...)` for what that stack
lacks.

- **When it runs:** on every push to `master`. On a pull request only with the
  `docker-free:pytests` label. A failing batch fails the run, on pull requests and on `master`.
- **Results:** each batch uploads `Test Results (docker-free smoke tests) pytests <N>` and, on failure,
  `logs-docker-free-pytests-<N>` (launcher, GMS, system-update, frontend and OpenSearch logs). The
  Boot stack step writes the boot timings to the job summary.
- **Batching:** `SMOKE_BACKEND=local` packs batches on `pytest_test_weights_docker_free.json`, the
  lane's own durations, instead of the Docker lane's `pytest_test_weights.json`. Tests run at
  different speeds on the two stacks. `update-test-weights.yml` refreshes both files weekly from
  each lane's `master` runs.
- **Retry:** "Re-run failed jobs" runs only the failed modules of each failed batch, as on the
  Docker lane. The build artifact lives one day, so re-run within that.

## Test Organization

- `test_e2e.py` - Main test suite (1387 lines)
- `test_system_info.py` - System info API tests (169 lines)
- `conftest.py` - Test configuration and fixtures
- `tests/utils.py` - Test utilities and helpers

---

💡 **Pro Tip**: For rapid development, use `pytest test_system_info.py -vv` which runs in ~30 seconds vs the full test suite which can take 30+ minutes.
