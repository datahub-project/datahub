# Smoke Test Guidelines

Canonical authoring guide for pytest smoke tests against a running DataHub
instance. How to run the suite: `[README.md](README.md)`.

## When to add smoke tests

These are E2E API tests on a **live GMS** (slow, shared stack, xdist). Prefer a
unit test next to the code, an ingestion test in `metadata-ingestion/tests/`, or a backend unit/integration test **before** adding a smoke test.

**Add one** when the behavior needs GMS: write → search, a public API flow with
no existing coverage, a production regression to keep, or live-stack authz.

**Do not add** when:

- The logic is a validator, parser, or in-process service.
- The flow is already covered — extend that module; do not add `test_*_v2.py`.
- You are stacking positive/negative variants of the same call. One happy path
  is enough. A negative case belongs here only if it needs GMS (e.g. policy
  deny), not invalid input or combinatorial 4xx/case/whitespace.

Put new tests in `tests/<feature>/`, not `test_e2e.py`. Connector tests are placed in `metadata-ingestion/tests/`. Generic placeholder names only
(`my_db.my_schema.events`) — no customer identifiers or ticket IDs.

## Rules

- **Idempotent and order-independent.** Do not assume an empty GMS or another
  test's cleanup. No module-level mutable globals.
- **Run-unique names** for every entity you create, mutate, or delete (see
  Isolation). Shared hardcoded URNs flake under xdist.
- **Cleanup even on failure:** fixture `yield`, or `try/finally` inside a test
  (wrap delete in `try/except` so cleanup cannot hide the assertion).
- **Do not mutate** the admin user, default All Users policies, or other shared
  platform state unless the module is marked `global_policy_mutator`.
- `logger.info()`, not `print()`.
- **No** `time.sleep()` for GMS/search/Kafka. Read-after-write:
  `@with_test_retry()`. Bulk ingest/cleanup: `wait_for_writes_to_sync()`
  (`mcp_only` / `mae_only` when only one store matters). Known `trace_id`: Trace
  API. `TestSessionWrapper` already waits on POST/PUT — do not extra-sync unless
  you are asserting search/index.
- **Auth:** `auth_session` / `graph_client`. Extra users:
  `make_step_actor_user()`. Config: `tests/utilities/env_vars.py`, not `os.getenv` or
  `localhost:8080`. GraphQL: `execute_graphql()` (it already checks `data` /
  `errors`) — assert the fields you care about. Ingest:
  `ingest_file_via_rest()`.
- **Reuse helpers.** Do not copy-paste `addTag` or roll a local `uuid` when  
  `unique_suffix()` exists. Tags/terms/descriptions:  
  `tests/utilities/metadata_operations.py`.
- Extra users: `make_step_actor_user()` in `tests/utilities/multi_user.py`,  
  not a one-off signup.
- `execute_graphql()` already asserts a non-empty body, `data` is not `None`,
  and no `errors` key, only assert the data that the test requires

## Markers

```python
from tests.utilities.domains import Domain

pytestmark = pytest.mark.domain(Domain.CATALOG)
# pytestmark = pytest.mark.domain(Domain.CATALOG, Domain.INGESTION)
```

| Marker                  | When                                                                                                       |
| ----------------------- | ---------------------------------------------------------------------------------------------------------- |
| `domain(...)`           | **Required.** `platform`, `observe`, `ingestion`, `ai`, `catalog`.                                         |
| `p0`                    | Must run on every PR.                                                                                      |
| `global_policy_mutator` | Disables default policies / mutates shared platform policy. CI runs these serially after parallel modules. |

Keep `@pytest.mark.dependency()` chains short (ideally ≤3).

## Isolation

Modules run in parallel against one GMS. Prefer these over a local `uuid` /
string replace:

| Helper                                              | Use                                                                                                                  |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `unique_suffix()`                                   | Any entity key (user, domain, PAT, …).                                                                               |
| `unique_dataset_urn(name)`                          | Dataset created in code (GraphQL / SDK).                                                                             |
| `materialize_with_unique_name(src, name, dest_dir)` | Fixture rewrite. `name` must appear **only** in URN keys, not descriptions. Returns `(dest_file, unique_name)`.      |
| `materialize_unique_dataset(...)`                   | Dataset fixture → `(dest_file, dataset_urn)`.                                                                        |
| `_ingest_cleanup_unique_dataset_impl(...)`          | Rewrite + ingest + yield URN + delete. No pre-delete (URN is new). Default for a shared dataset name in `data.json`. |
| `_ingest_cleanup_data_impl(...)`                    | Pre-delete → ingest → cleanup. Only when keys are already unique to this module.                                     |

```python
from conftest import _ingest_cleanup_unique_dataset_impl

@pytest.fixture(scope="module", autouse=True)
def dataset_urn(auth_session, graph_client, tmp_path_factory):
    yield from _ingest_cleanup_unique_dataset_impl(
        auth_session, graph_client,
        "tests/tags_and_terms/data.json", "tags_and_terms",
        "test-tags-terms-sample-kafka",
        tmp_path_factory.mktemp("tags_and_terms"),
    )
```

Tests take `dataset_urn` as a fixture argument. Multi-entity fixtures: rewrite
each key with `materialize_with_unique_name` — see
`tests/containers/containers_test.py`.

Mid-test creates:

```python
dataset_urn = unique_dataset_urn("my-feature")
try:
    graph_client.emit(...)
    wait_for_writes_to_sync()
finally:
    try:
        delete_urn(graph_client, dataset_urn)
    except Exception:
        logger.warning("cleanup failed for %s", dataset_urn, exc_info=True)
```

## Other helpers

- `tests/utils.py`: `execute_graphql`, `ingest_file_via_rest`,
  `delete_urn` / `delete_urns` / `delete_urns_from_file`, `with_test_retry`,
  `wait_for_writes_to_sync`.
- `tests/utilities/concurrent_test_runner.py`: thread-safe
  `run_concurrent_tests` / `run_concurrent_tests_with_args`.
- `tests/utilities/concurrent_openapi.py`: `run_tests(auth_session, fixture_globs=...)`.
  JSON fixtures: `{request, response}` with DeepDiff `exclude_regex_paths`. Do
  not add OpenAPI fixtures that only re-check a GraphQL path already covered.
