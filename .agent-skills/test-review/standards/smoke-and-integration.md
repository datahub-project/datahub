# Smoke Test & Integration Test Standards

Standards extracted from the DataHub repository. Every rule cites its source file.

---

## 1. Smoke Test Standards

### 1.1 Fixture Conventions

**Session-scoped fixtures** provide shared state across all tests in a session.

| Fixture                | Scope              | Source                         |
| ---------------------- | ------------------ | ------------------------------ |
| `auth_session`         | session            | `smoke-test/conftest.py:42-46` |
| `graph_client`         | session            | `smoke-test/conftest.py:60-62` |
| `openapi_graph_client` | session            | `smoke-test/conftest.py:65-67` |
| `clear_graph_cache`    | function (autouse) | `smoke-test/conftest.py:70-79` |

**Module-scoped data fixtures** must follow the `_ingest_cleanup_data_impl` pattern:

```python
@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/<module>/data.json", "<module>"
    )
```

**Source:** `smoke-test/conftest.py:82-117`, used by 13+ test modules including `tests/incidents/incidents_test.py:10-14`, `tests/domains/domains_test.py`, `tests/tags_and_terms/`.

**Rules:**

- BLOCKER: Data fixture must be `scope="module"` with `autouse=True`
- BLOCKER: Must include both pre-delete (for idempotency) and post-delete (cleanup)
- WARNING: Custom lifecycle fixtures should follow the same pre-delete/ingest/yield/cleanup pattern

### 1.2 Data Lifecycle

The canonical pattern is: **pre-delete -> ingest -> wait -> yield -> cleanup -> wait**.

**Source:** `smoke-test/conftest.py:82-117`

```python
def _ingest_cleanup_data_impl(auth_session, graph_client, data_file, test_name, to_delete_urns=None):
    delete_urns_from_file(graph_client, data_file)   # Pre-delete for idempotency
    ingest_file_via_rest(auth_session, data_file)     # Ingest test data
    wait_for_writes_to_sync()                         # Wait for consistency
    yield                                              # Tests run here
    delete_urns_from_file(graph_client, data_file)   # Cleanup
    if to_delete_urns:
        delete_urns(graph_client, to_delete_urns)
    wait_for_writes_to_sync()                         # Wait after cleanup
```

**Rules:**

- BLOCKER: Tests that create entities MUST clean them up
- BLOCKER: Pre-delete step required for idempotent test runs
- WARNING: `wait_for_writes_to_sync()` should follow both ingest and cleanup

### 1.3 Authentication

**Source:** `smoke-test/tests/utils.py:363-478`

Authentication uses `TestSessionWrapper` which:

- Wraps `requests.Session` with `Authorization: Bearer` token injection
- Auto-generates GMS token via exponential backoff (10 attempts, 4-30s)
- Clones header dicts to prevent cross-test pollution (line 388)
- Auto-waits for sync on POST/PUT calls (line 396-402)
- Revokes token on session destroy

**Rules:**

- BLOCKER: Never create auth tokens inline -- use the `auth_session` fixture
- BLOCKER: Never hardcode credentials -- use `get_admin_credentials()` from `utils.py`
- WARNING: Don't bypass `TestSessionWrapper` with raw `requests.get/post`

### 1.4 Retry and Consistency Patterns

Six mechanisms exist, ordered from most precise to most general:

**A. Trace API (write confirmation)** -- confirms a specific async write completed

**Source:** `smoke-test/tests/trace/test_api_trace.py`

```python
# After an async write, extract trace_id from response headers/system metadata
trace_id = response.headers.get("X-DataHub-Trace-Id")

# Query the trace endpoint to confirm processing
trace_resp = auth_session.post(
    f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
    params={"onlyIncludeErrors": "false", "detailed": "true"},
    json={urn: [aspect_name]},
)
trace_data = trace_resp.json()
assert trace_data["success"] is True
assert trace_data["primaryStorage"]["writeStatus"] == "ACTIVE_STATE"
assert trace_data["searchStorage"]["writeStatus"] == "ACTIVE_STATE"
```

**B. `wait_for_writes_to_sync()` with consumer group targeting** -- polls Kafka lag for specific consumers

**Source:** `smoke-test/tests/consistency_utils.py:31-119`

```python
# Target only the MCP consumer for faster waits (primary storage only)
wait_for_writes_to_sync(mcp_only=True)

# Target only the MAE consumer (search indexing only)
wait_for_writes_to_sync(mae_only=True)

# Shorter timeout for scoped waits (default is 120s)
wait_for_writes_to_sync(max_timeout_in_sec=30)

# Target a specific consumer group
wait_for_writes_to_sync(consumer_group="my-consumer-group")
```

**C. `@with_test_retry()` decorator** -- for read-after-write assertions

**Source:** `smoke-test/tests/utils.py:126-156`

```python
@with_test_retry()
def _ensure_entity_present(auth_session, urn):
    response = auth_session.get(...)
    assert response.json()["value"]
```

**D. Assertion-scoped waits** -- time-boxed retries for specific assertions

**Source:** `smoke-test/tests/assertions/sdk/helpers.py`, `smoke-test/tests/schema_fields/schema_evolution.py`

```python
# Pattern 1: scoped wait helper with shorter timeout
def wait_for_assertion_sync():
    wait_for_writes_to_sync(max_timeout_in_sec=30)

# Pattern 2: tenacity stop_after_delay for time-boxed assertions
@retry(stop=stop_after_delay(30), wait=wait_fixed(2), reraise=True)
def _verify_schema_field():
    result = graph_client.get_aspect(...)
    assert result is not None
```

**E. Integration service status polling** -- for action/integration lifecycle tests

**Source:** `smoke-test/tests/integrations_service_utils.py`

```python
# Wait until an action has processed at least one event
wait_until_action_has_processed_event(action_urn, integrations_url, event_time)

# Wait until an action finishes reloading after config change
wait_for_reload_completion(action_urn, integrations_url)
```

**F. Direct `tenacity.retry`** -- for custom retry requirements

**Source:** `smoke-test/tests/search/test_lineage_search_index_fields.py:99-104`

**Choosing the right mechanism:**

| Scenario                                                 | Mechanism                                            | Why                                             |
| -------------------------------------------------------- | ---------------------------------------------------- | ----------------------------------------------- |
| Confirming a single async write was processed            | **Trace API** (A)                                    | Most precise; confirms the exact write          |
| Waiting for all pending writes after bulk ingest/cleanup | **`wait_for_writes_to_sync()`** (B)                  | Drains Kafka lag to zero across consumers       |
| Waiting for only primary storage or only search          | **`wait_for_writes_to_sync(mcp_only/mae_only)`** (B) | Faster; skips irrelevant consumers              |
| Read-after-write assertion (entity should exist)         | **`@with_test_retry()`** (C)                         | Retries the assertion until consistent          |
| Time-boxed assertion (must pass within N seconds)        | **Assertion-scoped wait** (D)                        | `stop_after_delay(N)` caps total wait time      |
| Action/integration lifecycle completed                   | **Service status polling** (E)                       | Polls action-specific status endpoints          |
| Custom retry logic not covered above                     | **Direct `tenacity.retry`** (F)                      | Full control over stop/wait/retry conditions    |
| Cypress UI element should appear                         | **`cy.waitTextVisible` / `cy.intercept`**            | Built-in Cypress retry; never use `cy.wait(ms)` |

**Rules:**

- BLOCKER: Never use bare `time.sleep()` for eventual consistency. Use one of the mechanisms above.
- WARNING: Prefer Trace API over blanket `wait_for_writes_to_sync()` when confirming a single known write
- WARNING: Prefer `@with_test_retry()` over custom `tenacity.retry` for standard read-after-write patterns
- SUGGESTION: Use `max_timeout_in_sec` parameter to scope `wait_for_writes_to_sync()` waits to the minimum needed
- SUGGESTION: Use consumer group targeting (`mcp_only`, `mae_only`) when only one storage layer is relevant
- SUGGESTION: Use `max_attempts` parameter when fewer retries are appropriate

### 1.5 GraphQL Testing

**Source:** `smoke-test/tests/utils.py:188-224`

The standard pattern:

```python
res_data = execute_graphql(auth_session, query, variables)
assert res_data["data"]["entity"]["field"] == expected_value
```

`execute_graphql()` already asserts:

- Response is not empty
- `res_data["data"]` is not None
- No `errors` key in response

**Rules:**

- WARNING: Use `execute_graphql()` instead of manual `auth_session.post()` to GraphQL endpoint
- WARNING: Assert specific field values, not just that the response exists
- SUGGESTION: Inline GraphQL queries are acceptable but should be readable (use triple-quoted strings)

### 1.6 REST API Testing

**Source:** `smoke-test/test_e2e.py:32-34`, `smoke-test/tests/utils.py:243-265`

```python
restli_default_headers = {"X-RestLi-Protocol-Version": "2.0.0"}
```

Ingestion via REST uses `ingest_file_via_rest(auth_session, filename)` which creates a Pipeline with datahub-rest sink.

**Rules:**

- WARNING: Use `restli_default_headers` constant for RestLi API calls
- WARNING: Use `ingest_file_via_rest()` helper instead of manual Pipeline creation for test data ingestion
- SUGGESTION: For OpenAPI v3 tests, use the `concurrent_openapi.evaluate_test()` JSON fixture pattern

### 1.7 Marker Conventions

**Source:** `smoke-test/pyproject.toml:84-88`

| Marker              | Purpose                       | When to Use                                  |
| ------------------- | ----------------------------- | -------------------------------------------- |
| `read_only`         | Tests that don't mutate data  | Service health, search, analytics            |
| `no_cypress_suite1` | Module-level batch separation | Large test modules                           |
| `dependency()`      | Test ordering                 | When test B depends on test A's side effects |

**Rules:**

- WARNING: `read_only` tests must not create, modify, or delete any entities
- WARNING: `@pytest.mark.dependency()` chains should be kept short (ideally <=3 levels)
- SUGGESTION: New test modules should specify batch markers for CI parallelism

### 1.8 Environment Variable Discipline

**Source:** `smoke-test/tests/utilities/env_vars.py` (250 lines, 30+ variables)

Categories: Core DataHub config, admin credentials, database config, testing config, consistency testing, Cypress, integration testing, Slack.

**Violations found:**

- `tests/cypress/integration_test.py:279-280` -- direct `os.getenv("BATCH_NUMBER")`
- `tests/analytics/conftest.py:147` -- direct `ELASTICSEARCH_URL` read
- Multiple files set `os.environ["DATAHUB_TELEMETRY_ENABLED"]` redundantly

**Rules:**

- BLOCKER: New tests must use `env_vars.py` getters for all DataHub configuration
- WARNING: Do not hardcode URLs, ports, or hostnames -- use env_vars registry
- SUGGESTION: Consolidate telemetry suppression to conftest.py session scope

### 1.9 Idempotent Test Setup

Tests must create exactly what they need and be safely re-runnable. Two patterns ensure this:

**A. Pre-delete before ingest** (the data lifecycle pattern)

**Source:** `smoke-test/conftest.py:107` -- `delete_urns_from_file(graph_client, data_file)` runs _before_ ingest to clear stale data from previous runs.

**B. UUID-based unique entity names** for tests that create entities mid-test

**Source:** `smoke-test/tests/cli/user_cmd/test_user_add.py:15-16`

```python
def generate_test_email():
    """Generate a unique email for testing to avoid conflicts."""
    return f"test-user-{uuid.uuid4()}@example.com"
```

**Source:** `smoke-test/tests/assertions/assertions_test.py:369`

```python
assertion_urn = f"urn:li:assertion:{uuid.uuid4()}"
dataset_urn = make_dataset_urn(platform="postgres", name=f"assertion_patch_{uuid.uuid4()}")
```

**Rules:**

- BLOCKER: Tests must be safely re-runnable (idempotent). Either pre-delete existing data or use UUID-based unique names.
- WARNING: Do not assume a clean environment. Previous test runs, failed tests, or parallel batches may have left data behind.
- SUGGESTION: Prefer UUID-based unique names for entities created mid-test; use pre-delete for fixture-managed bulk data.

### 1.10 Guaranteed Cleanup

Cleanup must be guaranteed even when tests fail. Two mechanisms exist:

**A. Fixture teardown via `yield`** (preferred for bulk test data)

**Source:** `smoke-test/conftest.py:82-117` -- the `_ingest_cleanup_data_impl` pattern uses `yield` to separate setup from teardown. pytest guarantees the code after `yield` runs even if the test fails.

```python
@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(...)
    # Cleanup runs even on test failure
```

**B. `try/finally` blocks** (required for entities created mid-test)

**Source:** `smoke-test/tests/assertions/assertions_test.py:373-452`

```python
def test_assertion_info_patch_preserves_note(graph_client):
    assertion_urn = f"urn:li:assertion:{uuid.uuid4()}"
    dataset_urn = make_dataset_urn(platform="postgres", name=f"assertion_patch_{uuid.uuid4()}")
    try:
        graph_client.emit(MetadataChangeProposalWrapper(...))
        wait_for_writes_to_sync()
        # ... test logic and assertions ...
    finally:
        delete_urn(graph_client, assertion_urn)
        delete_urn(graph_client, dataset_urn)
        wait_for_writes_to_sync()
```

**Source:** `smoke-test/test_authentication_e2e.py:381-384`

```python
    finally:
        # Cleanup
        try:
            if token_id:
                revoke_api_token(auth_session, token_id)
```

**Source:** `smoke-test/test_system_info.py:324-327` (same pattern for token cleanup)

**Rules:**

- BLOCKER: Entities created mid-test (not via fixture) MUST be cleaned up in a `try/finally` block
- BLOCKER: Fixture-managed data MUST use `yield`-based teardown (pytest guarantees execution)
- WARNING: Cleanup `finally` blocks should themselves be wrapped in `try/except` to avoid masking the original test failure
- SUGGESTION: Prefer fixture-based lifecycle over `try/finally` when the data setup/teardown is shared across tests

### 1.11 Test Isolation

Tests must be independently runnable without relying on side effects from other tests.

**Source:** Observed anti-pattern in `smoke-test/tests/cli/datahub_cli.py:14-15`

```python
# ANTI-PATTERN: global mutable state
ingested_dataset_run_id = ""
ingested_editable_run_id = ""
```

**Source of isolation mechanisms:**

1. **Cache clearing** -- `smoke-test/conftest.py:70-79`: The `clear_graph_cache` autouse function-scoped fixture clears the `get_default_graph` LRU cache before each test, preventing stale credentials from leaking between tests.

2. **Header dict cloning** -- `smoke-test/tests/utils.py:388`: `TestSessionWrapper` clones header dicts before modification (`kwargs["headers"] = dict(kwargs["headers"])`) to prevent cross-test pollution.

3. **Unique entity names** -- `smoke-test/tests/cli/user_cmd/test_user_add.py:215`: Uses `f"testuser_{uuid.uuid4().hex[:8]}"` to avoid collisions with other tests or parallel batches.

**Rules:**

- BLOCKER: No global mutable state -- use fixture return values or `request.config` cache
- BLOCKER: No cross-test dependencies via shared module-level variables
- WARNING: Tests should be independently runnable (order-independent where possible)
- WARNING: Use unique identifiers (UUID) when creating entities to avoid collisions with parallel test batches
- SUGGESTION: If test ordering is truly required, use `@pytest.mark.dependency()` and keep chains short (<=3 levels)

### 1.12 Multi-Environment Configuration

Tests must be configurable to run against different environments (local quickstart, CI, remote DataHub instances) without code changes.

**Source:** `smoke-test/tests/utilities/env_vars.py` (full file, 250 lines)

The `env_vars.py` module provides environment-based configuration for all infrastructure endpoints:

| Getter                    | Env Var                | Default                 | Purpose             |
| ------------------------- | ---------------------- | ----------------------- | ------------------- |
| `get_gms_url()`           | `DATAHUB_GMS_URL`      | None                    | GMS endpoint        |
| `get_frontend_url()`      | `DATAHUB_FRONTEND_URL` | None                    | Frontend endpoint   |
| `get_kafka_url()`         | `DATAHUB_KAFKA_URL`    | None                    | Kafka broker        |
| `get_mysql_url()`         | `DATAHUB_MYSQL_URL`    | `localhost:3306`        | MySQL database      |
| `get_postgres_url()`      | `DATAHUB_POSTGRES_URL` | `localhost:5432`        | PostgreSQL database |
| `get_elasticsearch_url()` | `ELASTICSEARCH_URL`    | `http://localhost:9200` | Elasticsearch       |
| `get_admin_username()`    | `ADMIN_USERNAME`       | `datahub`               | Auth credentials    |
| `get_admin_password()`    | `ADMIN_PASSWORD`       | `datahub`               | Auth credentials    |

**Consistency mode toggle:**

**Source:** `smoke-test/tests/consistency_utils.py:7`, `smoke-test/tests/utilities/env_vars.py:157-159`

```python
USE_STATIC_SLEEP: bool = env_vars.get_use_static_sleep()
```

When `USE_STATIC_SLEEP=true`, `wait_for_writes_to_sync()` falls back to a fixed sleep instead of polling Kafka consumer lag. This is required for environments where the test runner cannot access the Kafka broker container (e.g., remote DataHub instances, k8s clusters without docker exec).

**Environment-aware features:**

**Source:** `smoke-test/tests/utilities/env_vars.py:142-149`

- `K8S_CLUSTER_ENABLED` -- toggles Kubernetes-specific behavior
- `TEST_DATAHUB_VERSION` -- allows version-specific test logic

**Source:** `smoke-test/smoke.sh:15` -- `RUN_QUICKSTART` controls whether to launch DataHub locally or use an existing instance.

**Source:** `metadata-ingestion/tests/test_helpers/docker_helpers.py:28-29` -- `cleanup_image()` skips image cleanup when not in CI (`is_ci()` check) to speed up local development.

**Rules:**

- BLOCKER: Never hardcode `localhost`, port numbers, or URLs. Use `env_vars.py` getters.
- WARNING: Tests that depend on Docker container access (e.g., `docker exec` for Kafka lag checks) must have a `USE_STATIC_SLEEP` fallback path.
- WARNING: Credential defaults (`datahub`/`datahub`) are acceptable for local dev, but tests must support override via `ADMIN_USERNAME`/`ADMIN_PASSWORD`.
- WARNING: When deploying a DataHub instance for testing, always use `--local` unless actively told not to. This ensures the instance runs locally and avoids remote deployment surprises.
- SUGGESTION: Use `env_vars.get_k8s_cluster_enabled()` to skip Docker-dependent tests in Kubernetes environments.
- SUGGESTION: Document which environment variables must be set for each test execution mode (local, CI, remote).

### 1.13 Concurrent Testing

**Source:** `smoke-test/tests/utilities/concurrent_test_runner.py`, `concurrent_openapi.py`

Worker pool pattern for parallel test execution within a single test function:

```python
run_concurrent_tests(test_cases, test_fn, num_workers=5)
```

**Rules:**

- SUGGESTION: Use `run_concurrent_tests()` for parametric API testing
- WARNING: Concurrent tests must be thread-safe (no shared mutable state)

---

## 2. Integration Test Standards (Cypress)

Integration tests are Cypress UI tests located in `smoke-test/tests/cypress/`. They are launched by the Python wrapper `integration_test.py` and run actual browser-based tests against a running DataHub instance.

### 2.1 Cypress Test Launcher (Python)

**Source:** `smoke-test/tests/cypress/integration_test.py`

The Python pytest wrapper handles:

- Data ingestion (lines 144-173): Ingests multiple JSON fixture files via REST
- Fixture teardown (lines 176-202): Cleans up all ingested data
- Batching (lines 214-242): Uses `bin_pack_tasks` with `test_weights.json` for CI parallelism
- Filtered tests (lines 245-269): Supports `FILTERED_TESTS` env var for retry mode
- Cypress execution (lines 272-343): Launches `npx cypress run` via subprocess

```python
@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    ingest_data(auth_session, graph_client)  # Ingest multiple JSON fixtures
    yield
    # Cleanup: delete_urns_from_file for each fixture
    delete_urns_from_file(graph_client, f"{CYPRESS_TEST_DATA_DIR}/{TEST_DATA_FILENAME}")
    # ... more cleanup ...
```

**Rules:**

- BLOCKER: Cypress launcher must clean up ALL ingested data in fixture teardown
- WARNING: Use `env_vars.py` getters, not direct `os.getenv()` (violation at lines 279-280)
- WARNING: Must support `FILTERED_TESTS` for CI retry workflows
- WARNING: The `CYPRESS_BASE_URL` env var does NOT override Cypress `e2e.baseUrl` -- Cypress expects `CYPRESS_baseUrl` (case-sensitive) or `--config baseUrl=<url>`. On k3d where the frontend is not on `localhost:9002`, pass `--config baseUrl=http://<k3d-host>:<port>` to the `npx cypress run` command.

### 2.2 Cypress Spec Structure

**Source:** `smoke-test/tests/cypress/cypress/e2e/mutations/domains.js:1-60`

Cypress specs follow the `describe`/`it` pattern:

```javascript
const test_domain_id = Math.floor(Math.random() * 100000);
const test_domain = `CypressDomainTest ${test_domain_id}`;

describe("add remove domain", () => {
  beforeEach(() => {
    cy.intercept("POST", "/api/v2/graphql", (req) => {
      aliasQuery(req, "appConfig");
    });
  });

  it("create domain", () => {
    cy.login();
    cy.goToDomainList();
    cy.clickOptionWithText("New Domain");
    cy.get('[data-testid="create-domain-name"]').click().type(test_domain);
    // ...
    cy.waitTextVisible(test_domain);
  });
});
```

**Rules:**

- WARNING: Each `describe` block should use unique test data (randomized IDs) for isolation
- WARNING: Use `cy.login()` per test or in `beforeEach` -- do not assume a logged-in state
- SUGGESTION: Use `data-testid` selectors (not CSS classes or tag selectors) for stability

### 2.3 Cypress Test Data Management

**Source:** `smoke-test/tests/cypress/integration_test.py:28-34`, `smoke-test/tests/cypress/data.json`

Test data files:

- `data.json` -- primary test entities (datasets, dashboards, users)
- `cypress_dbt_data.json` -- dbt-specific test data
- `patch-data.json` -- data for patch/update tests
- `incidents_test.json` -- incident test data
- `onboarding.json` -- generated dynamically for onboarding step states

**Source:** `smoke-test/tests/cypress/integration_test.py:125-137` -- timestamp updater for fixture freshness:

```python
def update_fixture_timestamps(cypress_test_data_dir):
    updater = TimestampUpdater(timestamp_config)
    updater.update_all_configured_files(cypress_test_data_dir)
```

**Rules:**

- BLOCKER: Test data JSON files must be committed to the repository
- WARNING: Use `TimestampUpdater` for files with time-sensitive data
- WARNING: Dynamically generated data files (like onboarding.json) must be cleaned up in teardown

### 2.4 Cypress Batching and CI

**Source:** `smoke-test/tests/cypress/integration_test.py:214-242`

```python
def _get_cypress_tests_batch():
    all_tests = _get_js_files("tests/cypress/cypress/e2e")
    # Load weights from test_weights.json
    test_batches = bin_pack_tasks(tests_with_weights, env_vars.get_batch_count())
    return test_batches[env_vars.get_batch_number()]
```

Uses the same `bin_pack_tasks` algorithm as smoke tests but with Cypress-specific test weights.

**Rules:**

- WARNING: New Cypress specs should be added to `test_weights.json` after initial runs
- SUGGESTION: Keep Cypress specs small (one feature per spec) for better batch distribution

### 2.5 Cypress Assertions and Selectors

**Source:** `smoke-test/tests/cypress/cypress/e2e/mutations/domains.js`

Good patterns observed:

- `cy.waitTextVisible(text)` -- waits for text to appear (handles async rendering)
- `cy.get('[data-testid="..."]')` -- stable selectors via test IDs
- `cy.clickOptionWithText(text)` -- custom command for text-based clicks
- `cy.intercept("POST", "/api/v2/graphql", ...)` -- GraphQL request interception

**Rules:**

- BLOCKER: Every Cypress `it` block must have at least one assertion (`.should()`, `cy.waitTextVisible`, or `assert`)
- WARNING: Prefer `data-testid` selectors over CSS class selectors (classes change with styling)
- WARNING: Use `cy.intercept` for GraphQL mocking/waiting, not arbitrary `cy.wait(ms)`
- SUGGESTION: Use Cypress custom commands (`cy.login()`, `cy.goToDomainList()`) for common operations

---

## 3. Anti-Patterns (Automatic Blockers)

These patterns trigger automatic BLOCKER findings:

### 3.1 Empty or Trivial Tests

```python
# ANTI-PATTERN
def test_basic():
    assert True

def test_defaults():
    config = MyConfig()
    assert config.platform == "myplatform"  # Testing defaults is trivial
```

### 3.2 Missing Cleanup

```python
# ANTI-PATTERN: Creates data but never cleans up
def test_create_entity(auth_session):
    ingest_file_via_rest(auth_session, "data.json")
    # ... assertions ...
    # No cleanup! Data persists across tests.
```

**Source of correct pattern:** `smoke-test/conftest.py:82-117`

### 3.3 Hardcoded URLs and Ports

```python
# ANTI-PATTERN
response = requests.get("http://localhost:8080/config")

# CORRECT
response = auth_session.get(f"{auth_session.gms_url()}/config")
```

**Source:** `smoke-test/tests/utilities/env_vars.py`

### 3.4 Inline Authentication

```python
# ANTI-PATTERN
token = generate_token(username="datahub", password="datahub")
headers = {"Authorization": f"Bearer {token}"}

# CORRECT: Use auth_session fixture
def test_something(auth_session):
    response = auth_session.get(...)
```

**Source:** `smoke-test/tests/utils.py:363-478`

### 3.5 Bare Sleep for Consistency

```python
# ANTI-PATTERN
time.sleep(5)  # Wait for indexing
assert search_results(query) == expected

# CORRECT: Use the most precise mechanism available

# Option 1 (best for single writes): Trace API
trace_resp = auth_session.post(
    f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
    params={"onlyIncludeErrors": "false", "detailed": "true"},
    json={urn: [aspect_name]},
)
assert trace_resp.json()["success"] is True

# Option 2: Targeted consumer wait with scoped timeout
wait_for_writes_to_sync(mae_only=True, max_timeout_in_sec=30)

# Option 3: Retry the assertion itself
@with_test_retry()
def _verify_search():
    assert search_results(query) == expected
```

**Source of anti-pattern:** `smoke-test/tests/incidents/incidents_test.py:25-26` (60+ occurrences across suite)
**Source of Trace API pattern:** `smoke-test/tests/trace/test_api_trace.py`
**Source of targeted wait:** `smoke-test/tests/consistency_utils.py:31-119`

### 3.6 Cross-Test Dependencies via Global State

```python
# ANTI-PATTERN
global shared_run_id
shared_run_id = pipeline.run_id()
```

**Source of anti-pattern:** `smoke-test/tests/cli/datahub_cli.py:14-15`

### 3.7 Overly Broad Assertions

```python
# ANTI-PATTERN
assert response.status_code == 200  # Only checks HTTP status, not content

# CORRECT
response.raise_for_status()
data = response.json()
assert data["value"]["entityName"] == "expected_name"
```

### 3.8 Commented-Out Test Code

```python
# ANTI-PATTERN
# breakpoint()
# TODO: Re-enable this test
# def test_important_feature():
```

**Source of anti-pattern:** `smoke-test/tests/lineage/test_lineage.py:119,757,763,777,789,791`

---

## 4. Quality Gates

These are the minimum requirements for test approval:

| Gate                   | Smoke Tests (Python)                                                                                         | Integration Tests (Cypress)               |
| ---------------------- | ------------------------------------------------------------------------------------------------------------ | ----------------------------------------- |
| Idempotent setup       | REQUIRED (pre-delete or UUID names)                                                                          | REQUIRED (random IDs per `describe`)      |
| Guaranteed cleanup     | REQUIRED (fixture `yield` or `try/finally`)                                                                  | REQUIRED (launcher fixture teardown)      |
| Data lifecycle pattern | REQUIRED (`_ingest_cleanup_data_impl`)                                                                       | Launcher handles ingest/cleanup           |
| Non-trivial assertions | >= 1 per test function                                                                                       | >= 1 per `it` block (`.should()`, etc.)   |
| Test isolation         | No global state, UUID entity names                                                                           | Random IDs, `cy.login()` per test         |
| Descriptive test names | REQUIRED (not `test_1`)                                                                                      | REQUIRED (descriptive `it("...")`)        |
| Environment config     | From `env_vars.py` (no hardcoded URLs)                                                                       | Launcher uses `env_vars.py`               |
| Retry/consistency      | Trace API, `wait_for_writes_to_sync` (targeted), `@with_test_retry`, assertion-scoped waits, service polling | `cy.waitTextVisible`, `cy.intercept`      |
| Markers                | `read_only`, `no_cypress_suite1`, etc.                                                                       | N/A (spec files, not pytest markers)      |
| Multi-env support      | URLs via env vars, `USE_STATIC_SLEEP` fallback                                                               | Launcher handles env config               |
| Stable selectors       | N/A                                                                                                          | REQUIRED (`data-testid`, not CSS classes) |
