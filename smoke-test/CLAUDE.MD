# Smoke Test Guidelines

## Test Principles

- Test should be idempotent
    - should not depend on an empty state of the system
    - should not depend on the order in which they run
    - should not depend on another test doing their cleanup

## Common Utilities

### Core Utilities (`tests/utils.py`)

- **`execute_graphql(auth_session, query, variables)`** - Execute GraphQL queries with standard error handling
- **`ingest_file_via_rest(auth_session, file_path)`** - Ingest metadata from JSON file
- **`delete_urns_from_file(graph_client, file_path)`** - Clean up entities from JSON file
- **`get_sleep_info()`** - Get retry timing for eventual consistency
- **`wait_for_writes_to_sync()`** - Wait for async operations to complete

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

```python
@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    print("ingesting test data")
    ingest_file_via_rest(auth_session, "tests/my_test/data.json")
    yield
    print("removing test data")
    delete_urns_from_file(graph_client, "tests/my_test/data.json")
```