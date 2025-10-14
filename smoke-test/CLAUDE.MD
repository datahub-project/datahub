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
- **`delete_urns(graph_client, urns)`** - Delete specific URNs from DataHub
- **`get_sleep_info()`** - Get retry timing for eventual consistency
- **`wait_for_writes_to_sync()`** - Wait for async operations to complete
- **`_ingest_cleanup_data_impl(auth_session, graph_client, data_file, test_name, to_delete_urns=None)`** - Helper for ingesting test data with automatic cleanup (in `conftest.py`)

### Metadata Operations (`tests/utilities/metadata_operations.py`)

Common operations for adding/removing tags, terms, and updating descriptions:

- **`add_tag(auth_session, resource_urn, tag_urn, sub_resource=None, sub_resource_type=None)`** - Add a tag to a resource
- **`remove_tag(auth_session, resource_urn, tag_urn, sub_resource=None, sub_resource_type=None)`** - Remove a tag from a resource
- **`add_term(auth_session, resource_urn, term_urn, sub_resource=None, sub_resource_type=None)`** - Add a glossary term to a resource
- **`remove_term(auth_session, resource_urn, term_urn, sub_resource=None, sub_resource_type=None)`** - Remove a glossary term from a resource
- **`update_description(auth_session, resource_urn, description, sub_resource=None, sub_resource_type=None)`** - Update resource description

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
    print("ingesting test data")
    ingest_file_via_rest(auth_session, "tests/my_test/data.json")
    yield
    print("removing test data")
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