# Smoke Test Guidelines

## Test Principles

- Test should be idempotent
    - should not depend on an empty state of the system
    - should not depend on the order in which they run
    - should not depend on another test doing their cleanup

## Logging

**IMPORTANT: Use logger.info() instead of print() in all test files**

- Always use `logger.info()` for test logging instead of `print()`
- Add `import logging` at the top of the file
- Define logger: `logger = logging.getLogger(__name__)`
- This ensures consistent, structured logging across all smoke tests

Example:
```python
import logging

logger = logging.getLogger(__name__)

@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    logger.info("ingesting test data")
    ingest_file_via_rest(auth_session, "tests/my_test/data.json")
    yield
    logger.info("removing test data")
    delete_urns_from_file(graph_client, "tests/my_test/data.json")
```

## Common Utilities


### Core Utilities (`tests/utils.py`)

- **`execute_graphql(auth_session, query, variables)`** - Execute GraphQL queries with standard error handling
- **`ingest_file_via_rest(auth_session, file_path)`** - Ingest metadata from JSON file
- **`delete_urns_from_file(graph_client, file_path)`** - Clean up entities from JSON file
- **`delete_urns(graph_client, urns)`** - Delete specific URNs from DataHub
- **`get_sleep_info()`** - Get retry timing for eventual consistency (advanced usage; prefer `with_test_retry()`)
- **`with_test_retry()`** - Decorator for retrying functions with environment-based sleep settings
- **`wait_for_writes_to_sync()`** - Wait for async operations to complete
- **`_ingest_cleanup_data_impl(auth_session, graph_client, data_file, test_name, to_delete_urns=None)`** - Helper for ingesting test data with automatic cleanup (in `conftest.py`)

### Metadata Operations (`tests/utilities/metadata_operations.py`)

Common operations for adding/removing tags, terms, and updating descriptions:

- **`add_tag(auth_session, resource_urn, tag_urn, sub_resource=None, sub_resource_type=None)`** - Add a tag to a resource
- **`remove_tag(auth_session, resource_urn, tag_urn, sub_resource=None, sub_resource_type=None)`** - Remove a tag from a resource
- **`add_term(auth_session, resource_urn, term_urn, sub_resource=None, sub_resource_type=None)`** - Add a glossary term to a resource
- **`remove_term(auth_session, resource_urn, term_urn, sub_resource=None, sub_resource_type=None)`** - Remove a glossary term from a resource
- **`update_description(auth_session, resource_urn, description, sub_resource=None, sub_resource_type=None)`** - Update resource description

### Concurrent Test Runner (`tests/utilities/concurrent_test_runner.py`)

Execute test functions in parallel using ThreadPoolExecutor:

- **`run_concurrent_tests(test_cases, test_fn, num_workers=3, test_name="test")`** - Run test function for each test case concurrently
- **`run_concurrent_tests_with_args(test_cases, test_fn, num_workers=3, test_name="test")`** - Run test function with tuple arguments concurrently

### Concurrent OpenAPI (`tests/utilities/concurrent_openapi.py`)

Execute JSON fixture-based OpenAPI tests with multi-step request/response validation:

- **`run_tests(auth_session, fixture_globs, num_workers=3)`** - Run JSON fixture tests concurrently
- **When to use**: Multi-step API integration tests with JSON fixtures, DeepDiff validation, complex response verification

**JSON Fixture Format**: Array of objects with `request` and optional `response` fields:
- Request: `url`, `method` (default: post), `json`, `params`, `description`, `wait` (sleep seconds)
- Response: `json` (expected response), `status_codes`, `exclude_regex_paths` (for DeepDiff)

### Retry Pattern

Use `with_test_retry()` for functions that need to retry for eventual consistency:

```python
from tests.utils import with_test_retry

@with_test_retry()
def check_eventual_consistency(auth_session):
    # This function will retry with environment-based sleep settings
    res_data = execute_graphql(auth_session, query, variables)
    assert res_data["data"]["dataset"]["name"] == "expected"

check_eventual_consistency(auth_session)
```

The decorator reads `DATAHUB_TEST_SLEEP_BETWEEN` and `DATAHUB_TEST_SLEEP_TIMES` environment variables.

### GraphQL Pattern

```python
from tests.utils import execute_graphql
from typing import Any, Dict

query = """query getDataset($urn: String!) { dataset(urn: $urn) { name } }"""
variables: Dict[str, Any] = {"urn": dataset_urn}
res_data = execute_graphql(auth_session, query, variables)
assert res_data["data"]["dataset"]["name"] == "expected"
```

### Fixture Pattern

Basic pattern (manual cleanup):
```python
@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    logger.info("ingesting test data")
    ingest_file_via_rest(auth_session, "tests/my_test/data.json")
    yield
    logger.info("removing test data")
    delete_urns_from_file(graph_client, "tests/my_test/data.json")
```

Using `_ingest_cleanup_data_impl` helper:
```python
from conftest import _ingest_cleanup_data_impl

@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client,
        "tests/my_test/data.json",
        "my_test"
    )

# With additional URNs to delete:
@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client,
        "tests/my_test/data.json",
        "my_test",
        to_delete_urns=["urn:li:dataset:additional1", "urn:li:dataset:additional2"]
    )
```

### Concurrent Test Patterns

**Function-based parameterized tests** (`concurrent_test_runner`):

```python
from tests.utilities.concurrent_test_runner import run_concurrent_tests, run_concurrent_tests_with_args

# Single argument per test case
def test_entity(entity_type: str) -> None:
    result = get_search_results(auth_session, entity_type)
    assert result["total"] > 0

run_concurrent_tests(["dataset", "dashboard"], test_entity)

# Multiple arguments per test case
def test_entity(entity_type: str, api_name: str) -> None:
    result = search(auth_session, entity_type, api_name)
    assert result["total"] > 0

run_concurrent_tests_with_args([("dataset", "dataset"), ("dashboard", "dashboard")], test_entity)
```

**JSON fixture-based API tests** (`concurrent_openapi`):

```python
from tests.utilities.concurrent_openapi import run_tests

def test_openapi_endpoints(auth_session):
    run_tests(auth_session, fixture_globs=["tests/openapi/v3/*.json"], num_workers=10)
```

Example JSON fixture (`tests/openapi/v3/example.json`):
```json
[
  {
    "request": {
      "url": "/openapi/v3/entity/dataset",
      "description": "Create dataset",
      "json": [{"urn": "urn:li:dataset:(...)"}]
    }
  },
  {
    "request": {
      "url": "/openapi/v3/entity/dataset/urn%3Ali%3Adataset%3A...",
      "method": "get",
      "description": "Get created dataset"
    },
    "response": {
      "json": {"urn": "urn:li:dataset:(...)"},
      "exclude_regex_paths": ["root\\['scrollId'\\]"]
    }
  }
]
```

### Metadata Operations Pattern

```python
from tests.utilities.metadata_operations import add_tag, remove_tag, add_term, remove_term, update_description

# Add a tag to a dataset
assert add_tag(auth_session, dataset_urn, "urn:li:tag:Legacy")

# Remove a tag from a dataset
assert remove_tag(auth_session, dataset_urn, "urn:li:tag:Legacy")

# Add a glossary term to a dataset
assert add_term(auth_session, dataset_urn, "urn:li:glossaryTerm:SavingAccount")

# Remove a glossary term from a dataset
assert remove_term(auth_session, dataset_urn, "urn:li:glossaryTerm:SavingAccount")

# Update description
assert update_description(auth_session, dataset_urn, "Updated description")

# Add a tag to a schema field (sub-resource)
assert add_tag(
    auth_session,
    dataset_urn,
    "urn:li:tag:Legacy",
    sub_resource="[version=2.0].field_name",
    sub_resource_type="DATASET_FIELD",
)

# Update schema field description
assert update_description(
    auth_session,
    dataset_urn,
    "Field description",
    sub_resource="[version=2.0].field_name",
    sub_resource_type="DATASET_FIELD",
)
```