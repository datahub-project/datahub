# Smoke Test Guidelines

Canonical authoring guide for pytest smoke tests against a running DataHub
instance. How to run the suite locally is in `[README.md](README.md)`.

## What this suite is for

Smoke tests exercise **user-visible behavior through public APIs** (GraphQL,
OpenAPI, Rest.li, CLI) on a live GMS. They are not unit tests.

- Put new tests in `tests/<feature>/`, not in `test_e2e.py`.
- UI belongs in Cypress (`tests/cypress/`) or Playwright (`e2e-test/ui/playwright/`).
- Connector and ingestion-framework tests belong in `metadata-ingestion/tests/`.
- Assert specific fields that prove the behavior. Do not dump full JSON, match
exact error strings, or combinatorial input matrices (whitespace × case ×
special characters).
- Reuse helpers in this file. Do not copy-paste `addTag` mutations or mint
local `uuid` / string-replace isolation when `unique_suffix()` exists.
- Fixture and entity names are generic placeholders (`my_db.my_schema.events`,
`col_a`). No customer identifiers, ticket IDs, or real hostnames.



## When to add smoke tests

These are E2E API tests and need the full DataHub stack (slow, shared GMS, xdist).

Prefer a unit test next to the code, an ingestion test in `metadata-ingestion/tests/`, a unit test in Java/Python backend services

**Add a smoke test when** the behavior needs a live GMS: cross-service
(write → search), a public API flow with no existing coverage, a production
regression you want to keep, or authz on the running stack.

**Do not add a smoke test when**

- The logic is a validator, parser, or in-process service (unit-test it).
- The endpoint/flow is already covered; extend that module instead of a new file.
- You are adding positive/negative variants of the same call. One happy path
is enough. A negative case belongs here only if it requires GMS (e.g. policy
deny), not invalid input.
- The cases are combinatorial (every 4xx, case, whitespace).



## Test Principles

- Tests must be **idempotent**: safe to re-run. Do not assume an empty GMS.
- Tests must be **order-independent**. Do not depend on another test's setup or
cleanup. No module-level mutable globals for run IDs or URNs.
- Every entity a test creates, mutates, or deletes needs a **run-unique name**
(see Data Isolation Utilities). Shared hardcoded URNs flake under xdist.
- Cleanup must run even when the test fails: fixture `yield` teardown, or
`try/finally` for entities created inside the test.
- Do not mutate the admin user, default "All Users" policies, or other shared
platform state unless the module is marked `global_policy_mutator`.



## Markers

Every module (or test) must declare a product domain. CI selects by `--domain`
and routes failures to the owning team.

```python
from tests.utilities.domains import Domain

pytestmark = pytest.mark.domain(Domain.CATALOG)

# Spans two areas — selected by either --domain catalog or --domain ingestion
pytestmark = pytest.mark.domain(Domain.CATALOG, Domain.INGESTION)
```


| Marker                  | When to use                                                                                                                                 |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `domain(...)`           | **Required.** Values: `platform`, `observe`, `ingestion`, `ai`, `catalog` (`Domain` enum).                                                  |
| `p0`                    | Only for regressions that must run on every pull request.                                                                                   |
| `global_policy_mutator` | Module disables default policies or mutates shared platform policy. CI runs these **serially after** parallel modules (`smoke.sh` phase 2). |


Keep `@pytest.mark.dependency()` chains short (ideally ≤3) — prefer isolation
over ordered tests.

## Auth and HTTP

- Use the session-scoped `**auth_session**` and `**graph_client**` fixtures.
Do not mint tokens inline, hardcode `datahub`/`datahub`, or call raw
`requests`.
- Extra users: `make_step_actor_user()` in `tests/utilities/multi_user.py`,
not a one-off signup.
- Config: getters in `tests/utilities/env_vars.py`, not `os.getenv` or
hardcoded `localhost:8080`.
- GraphQL: `execute_graphql()`. It already asserts a non-empty body, `data` is
not `None`, and no `errors` key. Assert the fields you care about.
- Rest.li: include `restli_default_headers`. Ingest fixtures with
`ingest_file_via_rest()`, not a hand-rolled Pipeline.



## Logging

Use `logger.info()` instead of `print()`:

```python
import logging

logger = logging.getLogger(__name__)
```



## Consistency (no bare sleeps)

Never use `time.sleep()` to wait for GMS, search, or Kafka. Prefer:


| Situation                               | Use                                                                                          |
| --------------------------------------- | -------------------------------------------------------------------------------------------- |
| Read-after-write assertion              | `@with_test_retry()`                                                                         |
| After bulk ingest or cleanup            | `wait_for_writes_to_sync()` — `mcp_only=True` or `mae_only=True` when only one store matters |
| One known async write with a `trace_id` | Trace API (`/openapi/v1/trace/write/{trace_id}`)                                             |
| Custom timing                           | `tenacity` with `stop_after_delay`, not a fixed sleep                                        |


`TestSessionWrapper` already waits on POST/PUT. Do not add another full sync
after every GraphQL mutation unless you are asserting search or index state.

`@with_test_retry()` reads `DATAHUB_TEST_SLEEP_BETWEEN` and
`DATAHUB_TEST_SLEEP_TIMES`.

```python
from tests.utils import execute_graphql, with_test_retry

@with_test_retry()
def check_eventual_consistency(auth_session, dataset_urn):
    res_data = execute_graphql(
        auth_session,
        """query getDataset($urn: String!) { dataset(urn: $urn) { name } }""",
        {"urn": dataset_urn},
    )
    assert res_data["data"]["dataset"]["name"] == "expected"

check_eventual_consistency(auth_session, dataset_urn)
```



## Cleanup


| Where the entity is created           | How to tear it down                                                                                                                            |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Module fixture (shared JSON / ingest) | `yield` in the fixture (pytest always runs teardown). Prefer `_ingest_cleanup_unique_dataset_impl` or `materialize_with_unique_name` + delete. |
| Inside a single test                  | `try/finally`. Wrap cleanup itself in `try/except` so a delete failure does not hide the original assertion error.                             |


```python
def test_creates_dataset(graph_client):
    dataset_urn = unique_dataset_urn("my-feature")
    try:
        graph_client.emit(...)
        wait_for_writes_to_sync()
        # assertions
    finally:
        try:
            delete_urn(graph_client, dataset_urn)
        except Exception:
            logger.warning("cleanup failed for %s", dataset_urn, exc_info=True)
```

`_ingest_cleanup_data_impl` pre-deletes then ingests: use it only when fixture
keys are already unique (or you accept a shared URN). For shared dataset names,
use `_ingest_cleanup_unique_dataset_impl` instead — the URN is new each run, so
there is nothing to pre-delete.

## Common Utilities



### Data Isolation Utilities (`tests/utils.py` / `conftest.py`)

Smoke tests share one GMS. Under pytest-xdist `--dist=loadscope` (see `smoke.sh`),
different **modules** run in parallel, so a hardcoded URN in two modules races
and flakes. Give every entity a test creates, mutates, or deletes a run-unique
name. Do not hardcode URNs that another module could also touch.

Prefer these helpers over rolling a local `uuid` / string replace:

- `unique_suffix()` — 8-char hex suffix. Append to any entity key (user, domain, PAT, …).
- `unique_dataset_urn(name, platform="kafka", env="PROD")` — dataset URN
`name-<suffix>`. Use when the test creates the dataset in code (GraphQL / SDK),
not from a JSON fixture.
- `materialize_with_unique_name(src_file, name, dest_dir)` — copy a fixture and
replace every occurrence of `name` with `name-<suffix>`. Returns
`(dest_file, unique_name)`. Build URNs from `unique_name` yourself. `name`
must be a token that appears **only** where a rename is intended (entity keys
in URNs), not in descriptions or other free text.
- `materialize_unique_dataset(src_file, dataset_name, dest_dir, platform="kafka", env="PROD")` —
like `materialize_with_unique_name` for a dataset fixture. Returns
`(dest_file, dataset_urn)` so field-level and reference URNs stay consistent.
- `_ingest_cleanup_unique_dataset_impl(auth_session, graph_client, data_file, test_name, dataset_name, tmp_dir, platform="kafka", env="PROD")` —
in `conftest.py`. Rewrites `dataset_name` in `data_file`, ingests, yields the
unique dataset URN, then deletes. No pre-ingest delete: the URN is new each
run. Use this instead of `_ingest_cleanup_data_impl` when the fixture owns a
shared dataset name.

**Programmatic dataset** (`unique_dataset_urn`):

```python
from tests.utils import unique_dataset_urn

dataset_urn = unique_dataset_urn("projection-payload")
```

**File-driven dataset** (`_ingest_cleanup_unique_dataset_impl`):

```python
from conftest import _ingest_cleanup_unique_dataset_impl

@pytest.fixture(scope="module", autouse=True)
def dataset_urn(auth_session, graph_client, tmp_path_factory):
    yield from _ingest_cleanup_unique_dataset_impl(
        auth_session,
        graph_client,
        "tests/tags_and_terms/data.json",
        "tags_and_terms",
        "test-tags-terms-sample-kafka",
        tmp_path_factory.mktemp("tags_and_terms"),
    )
```

Tests then take `dataset_urn` as a fixture argument instead of a hardcoded URN.

**Other entity types in a fixture** (`materialize_with_unique_name`): rewrite
each key in sequence (dataset, tag, term, container, …), ingest the last
`dest_file`, and construct URNs from the returned unique names. See
`tests/containers/containers_test.py` and `tests/cli/search_cmd/test_search_cmd.py`.

### Core Utilities (`tests/utils.py`)

- `execute_graphql(auth_session, query, variables)` - Execute GraphQL queries with standard error handling
- `ingest_file_via_rest(auth_session, file_path)` - Ingest metadata from JSON file
- `delete_urns_from_file(graph_client, file_path)` - Clean up entities from JSON file
- `delete_urn(graph_client, urn)` / `delete_urns(graph_client, urns)` - Delete URNs
- `get_sleep_info()` - Get retry timing for eventual consistency (advanced usage; prefer `with_test_retry()`)
- `with_test_retry()` - Decorator for retrying functions with environment-based sleep settings
- `wait_for_writes_to_sync()` - Wait for async operations to complete
- `_ingest_cleanup_data_impl(...)` / `_ingest_cleanup_unique_dataset_impl(...)` - Ingest helpers in `conftest.py`



### Metadata Operations (`tests/utilities/metadata_operations.py`)

Common operations for adding/removing tags, terms, and updating descriptions:

- `add_tag(auth_session, resource_urn, tag_urn, sub_resource=None, sub_resource_type=None)` - Add a tag to a resource
- `remove_tag(auth_session, resource_urn, tag_urn, sub_resource=None, sub_resource_type=None)` - Remove a tag from a resource
- `add_term(auth_session, resource_urn, term_urn, sub_resource=None, sub_resource_type=None)` - Add a glossary term to a resource
- `remove_term(auth_session, resource_urn, term_urn, sub_resource=None, sub_resource_type=None)` - Remove a glossary term from a resource
- `update_description(auth_session, resource_urn, description, sub_resource=None, sub_resource_type=None)` - Update resource description



### Concurrent Test Runner (`tests/utilities/concurrent_test_runner.py`)

Execute test functions in parallel using ThreadPoolExecutor. Callers must be
thread-safe (no shared mutable state):

- `run_concurrent_tests(test_cases, test_fn, num_workers=3, test_name="test")` - Run test function for each test case concurrently
- `run_concurrent_tests_with_args(test_cases, test_fn, num_workers=3, test_name="test")` - Run test function with tuple arguments concurrently



### Concurrent OpenAPI (`tests/utilities/concurrent_openapi.py`)

Execute JSON fixture-based OpenAPI tests with multi-step request/response validation:

- `run_tests(auth_session, fixture_globs, num_workers=3)` - Run JSON fixture tests concurrently
- **When to use**: Multi-step API integration tests with JSON fixtures, DeepDiff validation, complex response verification. Do not add fixtures that only re-check a GraphQL path already covered.

**JSON Fixture Format**: Array of objects with `request` and optional `response` fields:

- Request: `url`, `method` (default: post), `json`, `params`, `description`, `wait` (sleep seconds)
- Response: `json` (expected response), `status_codes`, `exclude_regex_paths` (for DeepDiff)



## Patterns



### GraphQL

```python
from typing import Any, Dict

from tests.utils import execute_graphql

query = """query getDataset($urn: String!) { dataset(urn: $urn) { name } }"""
variables: Dict[str, Any] = {"urn": dataset_urn}
res_data = execute_graphql(auth_session, query, variables)
assert res_data["data"]["dataset"]["name"] == "expected"
```



### Fixture ingest (shared keys, pre-delete)

Prefer unique-name ingest above. Use `_ingest_cleanup_data_impl` when the
fixture's URNs are already unique to this module:

```python
from conftest import _ingest_cleanup_data_impl

@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client,
        "tests/my_test/data.json",
        "my_test",
        to_delete_urns=["urn:li:dataset:additional1"],  # optional extras
    )
```



### Concurrent tests

**Function-based parameterized tests** (`concurrent_test_runner`):

```python
from tests.utilities.concurrent_test_runner import run_concurrent_tests, run_concurrent_tests_with_args

def test_entity(entity_type: str) -> None:
    result = get_search_results(auth_session, entity_type)
    assert result["total"] > 0

run_concurrent_tests(["dataset", "dashboard"], test_entity)

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
      "json": [{ "urn": "urn:li:dataset:(...)" }]
    }
  },
  {
    "request": {
      "url": "/openapi/v3/entity/dataset/urn%3Ali%3Adataset%3A...",
      "method": "get",
      "description": "Get created dataset"
    },
    "response": {
      "json": { "urn": "urn:li:dataset:(...)" },
      "exclude_regex_paths": ["root\\['scrollId'\\]"]
    }
  }
]
```



### Metadata operations

```python
from tests.utilities.metadata_operations import add_tag, remove_tag, add_term, remove_term, update_description

assert add_tag(auth_session, dataset_urn, "urn:li:tag:Legacy")
assert remove_tag(auth_session, dataset_urn, "urn:li:tag:Legacy")
assert add_term(auth_session, dataset_urn, "urn:li:glossaryTerm:SavingAccount")
assert remove_term(auth_session, dataset_urn, "urn:li:glossaryTerm:SavingAccount")
assert update_description(auth_session, dataset_urn, "Updated description")

assert add_tag(
    auth_session,
    dataset_urn,
    "urn:li:tag:Legacy",
    sub_resource="[version=2.0].field_name",
    sub_resource_type="DATASET_FIELD",
)
assert update_description(
    auth_session,
    dataset_urn,
    "Field description",
    sub_resource="[version=2.0].field_name",
    sub_resource_type="DATASET_FIELD",
)
```

