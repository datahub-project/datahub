# Smoke Test Patterns Reference

Code examples extracted from the DataHub smoke test suite at `smoke-test/`.

---

## Data Lifecycle Pattern

The canonical fixture for test data management. Pre-deletes for idempotency, ingests, yields for tests, then cleans up.

**Source:** `smoke-test/conftest.py:82-117`

```python
def _ingest_cleanup_data_impl(
    auth_session, graph_client, data_file, test_name, to_delete_urns=None,
):
    print(f"deleting {test_name} test data for idempotency")
    delete_urns_from_file(graph_client, data_file)
    print(f"ingesting {test_name} test data")
    ingest_file_via_rest(auth_session, data_file)
    wait_for_writes_to_sync()
    yield
    print(f"removing {test_name} test data")
    delete_urns_from_file(graph_client, data_file)
    if to_delete_urns:
        delete_urns(graph_client, to_delete_urns)
    wait_for_writes_to_sync()
```

**Usage in test modules** (`smoke-test/tests/incidents/incidents_test.py:10-14`):

```python
@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/incidents/data.json", "incidents"
    )
```

---

## Authentication Pattern

**Source:** `smoke-test/tests/utils.py:363-478`

Session creation chain:

1. `wait_for_healthcheck_util(requests)` -- waits for GMS to be healthy
2. `get_frontend_session()` -- logs in via frontend
3. `TestSessionWrapper(session)` -- wraps with token injection

Token generation with exponential backoff:

```python
@tenacity.retry(
    stop=tenacity.stop_after_attempt(10),
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=30),
    retry=tenacity.retry_if_exception_type(Exception),
    reraise=True,
)
def _generate_gms_token(self):
    actor_urn = self._upstream.cookies["actor"]
    # Creates PERSONAL access token via GraphQL mutation
```

Header injection with dict cloning:

```python
kwargs["headers"] = dict(kwargs["headers"])  # Clone to prevent mutation
kwargs["headers"].update({"Authorization": f"Bearer {self._gms_token}"})
```

---

## Retry Pattern: `@with_test_retry()`

**Source:** `smoke-test/tests/utils.py:126-156`

```python
def with_test_retry(max_attempts=None):
    sleep_sec, sleep_times = get_sleep_info()
    retry_count = max_attempts if max_attempts is not None else sleep_times
    return tenacity.retry(
        stop=tenacity.stop_after_attempt(retry_count),
        wait=tenacity.wait_fixed(sleep_sec),
        reraise=True,
    )
```

Usage for read-after-write (`smoke-test/test_e2e.py:38-53`):

```python
@with_test_retry()
def _ensure_user_present(auth_session, urn):
    response = auth_session.get(
        f"{auth_session.gms_url()}/entities/{urllib.parse.quote(urn)}",
        headers={**restli_default_headers},
    )
    response.raise_for_status()
    data = response.json()
    assert data["value"]
```

---

## GraphQL Test Pattern

**Source:** `smoke-test/tests/utils.py:188-224`

```python
def execute_graphql(auth_session, query, variables=None, expect_errors=False):
    json_payload = {"query": query}
    if variables:
        json_payload["variables"] = variables
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json_payload
    )
    response.raise_for_status()
    res_data = response.json()
    assert res_data, "GraphQL response is empty"
    assert res_data.get("data") is not None, "GraphQL response.data is None"
    assert "errors" not in res_data, f"GraphQL errors: {res_data.get('errors')}"
    return res_data
```

Typical test function (`smoke-test/test_e2e.py:323-345`):

```python
def test_frontend_browse_datasets(auth_session):
    query = """query browse($input: BrowseInput!) {
        browse(input: $input) {
            start count total
            groups { name }
            entities { ... on Dataset { urn name } }
        }
    }"""
    variables = {"input": {"type": "DATASET", "path": ["prod"]}}
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["browse"]
    assert len(res_data["data"]["browse"]["groups"]) > 0
```

---

## REST API Pattern

**Source:** `smoke-test/test_e2e.py:32-34`

```python
restli_default_headers = {"X-RestLi-Protocol-Version": "2.0.0"}
```

File ingestion via REST (`smoke-test/tests/utils.py:243-265`):

```python
def ingest_file_via_rest(auth_session, filename, mode="ASYNC_BATCH"):
    pipeline = Pipeline.create({
        "source": {"type": "file", "config": {"filename": filename}},
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": auth_session.gms_url(),
                "token": auth_session.gms_token(),
                "mode": mode,
            },
        },
    })
    pipeline.run()
    pipeline.raise_from_status()
    wait_for_writes_to_sync()
```

---

## Consistency: `wait_for_writes_to_sync()`

**Source:** `smoke-test/tests/consistency_utils.py:31-119`

Polls Kafka consumer group lag via `docker exec` until lag reaches zero, with configurable timeout.

```python
def wait_for_writes_to_sync(
    max_timeout_in_sec=120, mcp_only=False, mae_only=False,
    cdc_only=False, consumer_group=None,
):
    if USE_STATIC_SLEEP:
        time.sleep(ELASTICSEARCH_REFRESH_INTERVAL_SECONDS)
        return
    # ... polls kafka-consumer-groups --describe via docker exec
    # After lag=0, adds ELASTICSEARCH_REFRESH_INTERVAL_SECONDS sleep
```

### Consumer Group Targeting

Target specific consumers for faster, more precise waits:

```python
# Only wait for MCP consumer (primary storage writes)
wait_for_writes_to_sync(mcp_only=True)

# Only wait for MAE consumer (search index updates)
wait_for_writes_to_sync(mae_only=True)

# Only wait for CDC consumer
wait_for_writes_to_sync(cdc_only=True)

# Target a specific consumer group by name
wait_for_writes_to_sync(consumer_group="myConsumerGroup")
```

### Scoped Timeout

Use `max_timeout_in_sec` for tighter waits in focused assertions:

**Source:** `smoke-test/tests/assertions/sdk/helpers.py`

```python
def wait_for_assertion_sync():
    """Scoped wait for assertion tests -- 30s instead of default 120s."""
    wait_for_writes_to_sync(max_timeout_in_sec=30)
```

---

## Trace API (Write Confirmation)

The most precise wait mechanism: confirms a specific async write was fully processed across all storage layers.

**Source:** `smoke-test/tests/trace/test_api_trace.py`

**Endpoint:** `POST /openapi/v1/trace/write/{trace_id}`

After an async write, the response includes a `trace_id` in headers or system metadata. Query the trace endpoint to confirm the write completed:

```python
# 1. Perform an async write and capture the trace ID
response = auth_session.post(
    f"{auth_session.gms_url()}/openapi/v1/entities",
    json=entity_payload,
)
trace_id = response.headers.get("X-DataHub-Trace-Id")

# 2. Query the trace endpoint to confirm processing
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

**When to use:** After a single async write where you need to confirm that exact write was processed. Avoids the problem of `wait_for_writes_to_sync()` returning before your specific write is indexed.

---

## Event API (Offset-Based Polling)

Polls for events on a Kafka topic using offset tracking. Useful for tests that need to verify specific events were produced.

**Source:** `smoke-test/tests/semantic/test_current_offset_api.py`

**Endpoint:** `GET /openapi/v1/events/poll`

```python
# Poll for events on MetadataChangeLog topic
response = auth_session.get(
    f"{auth_session.gms_url()}/openapi/v1/events/poll",
    params={
        "topic": "MetadataChangeLog_Versioned_v1",
        "limit": 10,
        "pollTimeoutSeconds": 5,
        "offsetId": last_known_offset,  # Resume from previous position
    },
)
data = response.json()
offset_id = data.get("offsetId")  # Save for next poll
events = data.get("events", [])
assert any(e["entityUrn"] == expected_urn for e in events)
```

**When to use:** When your test needs to verify that specific Kafka events were produced (e.g., MCL events after a mutation). Offset tracking allows resuming without reprocessing old events.

---

## Integration Service Status Polling

For tests involving DataHub actions/integrations, poll the action status API instead of sleeping.

**Source:** `smoke-test/tests/integrations_service_utils.py`

```python
def wait_until_action_has_processed_event(action_urn, integrations_url, event_time, timeout=120):
    """Poll action stats endpoint until at least one event has been processed."""
    url = f"{integrations_url}/private/actions/{action_urn}/stats"
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = requests.get(url)
        if response.status_code == 200:
            stats = response.json()
            last_processed = stats.get("live", {}).get(
                "customProperties", {}
            ).get("event_processing_stats.last_event_processed_time")
            if last_processed:
                return
        time.sleep(1)
    raise TimeoutError(...)

def wait_for_reload_completion(action_urn, integrations_url, timeout=120):
    """Poll action status until it transitions back to 'running' after a config reload."""
    # Polls until status == "running" or timeout
```

**When to use:** In tests that configure and trigger DataHub actions/integrations, and need to confirm the action has finished initializing or processing events.

---

## GraphQL with Exponential Backoff

For GraphQL calls that may fail under server load, use the retry-aware wrapper.

**Source:** `smoke-test/tests/utils.py` (`execute_gql_with_retry`)

```python
# Standard GraphQL call (no retry)
res_data = execute_graphql(auth_session, query, variables)

# With exponential backoff for server overload scenarios
res_data = execute_gql_with_retry(auth_session, query, variables)
# Retries with exponential backoff (2^attempt + jitter) on ConnectionError/Timeout
# CI: 5 retries, base_delay=2s; local: 3 retries, base_delay=1s
```

**When to use:** When tests hit GraphQL endpoints that may return 429 or 503 under heavy load (e.g., during parallel test execution). Prefer `execute_graphql()` for normal cases.

---

## Bulk Deletion Pattern

**Source:** `smoke-test/tests/utils.py:277-300`

Uses `joblib.Parallel` for parallel bulk deletion (10 workers):

```python
def delete_urns_from_file(graph_client, filename, shared_data=False):
    if not env_utils.get_boolean_env_variable("CLEANUP_DATA", True):
        return
    def delete(entry):
        urn = entry["entityUrn"] if "entityUrn" in entry else ...
        delete_urn(graph_client, urn)
    with open(filename) as f:
        d = json.load(f)
        Parallel(n_jobs=10)(delayed(delete)(entry) for entry in d)
    wait_for_writes_to_sync()
```

---

## Concurrent Test Runner

**Source:** `smoke-test/tests/utilities/concurrent_test_runner.py`

```python
def run_concurrent_tests(test_cases, test_fn, num_workers=5, test_name="test"):
    failures = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_case = {executor.submit(test_fn, case): case for case in test_cases}
        for future in concurrent.futures.as_completed(future_to_case):
            try:
                future.result()
            except Exception as e:
                failures[future_to_case[future]] = e
    if failures:
        raise AssertionError(f"Failed for {len(failures)}/{len(test_cases)} cases")
```

---

## Batch Distribution

**Source:** `smoke-test/conftest.py:125-322`

Bin-packing algorithm distributes test modules across CI batches:

- Reads `BATCH_COUNT` and `BATCH_NUMBER` from environment
- Loads weights from `pytest_test_weights.json`
- Groups tests by module, sums weights
- Greedy bin-packing: assigns heaviest module first to lightest bucket
