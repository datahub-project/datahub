# Smoke Test Standards

Review rubric for pytest smoke tests. Authoring how-to lives in
[`smoke-test/AGENTS.md`](../../../smoke-test/AGENTS.md). Cite that guide and
the symbols below; do not invent rules.

**In scope:** `smoke-test/**/*.py` (API tests against a running DataHub).

**Out of scope:** Cypress (`smoke-test/tests/cypress/`), Playwright
(`e2e-test/ui/playwright/`), connector tests (`metadata-ingestion/tests/`).

---

## 1. Isolation and unique names

**Source:** `smoke-test/AGENTS.md` (Isolation),
`unique_suffix` / `unique_dataset_urn` / `materialize_with_unique_name` /
`materialize_unique_dataset` in `smoke-test/tests/utils.py`,
`_ingest_cleanup_unique_dataset_impl` in `smoke-test/conftest.py`.

Smoke tests share one GMS. Under xdist `--dist=loadscope`, modules run in
parallel, so a hardcoded URN in two modules races.

Prefer the shared helpers over a local `uuid` or string replace.

**Rules:**

- BLOCKER: Entities the test creates, mutates, or deletes must not use a
  hardcoded URN another module could share. Use `unique_suffix()`,
  `unique_dataset_urn()`, `materialize_with_unique_name()`,
  `materialize_unique_dataset()`, or `_ingest_cleanup_unique_dataset_impl`.
- BLOCKER: No global mutable state (run IDs, URNs) at module level. Use
  fixture return values.
- WARNING: New ad-hoc UUID / email helpers when `unique_suffix()` or
  `make_step_actor_user()` already cover the case.
- WARNING: `materialize_with_unique_name` tokens must appear only in URN keys,
  not in descriptions or other free text.
- SUGGESTION: If ordering is required, `@pytest.mark.dependency()` chains
  should stay short (ideally ≤3). Prefer isolation.

**Anti-pattern:**

```python
# ANTI-PATTERN: global mutable state
ingested_dataset_run_id = ""
```

**Source:** `smoke-test/tests/cli/datahub_cli.py` (observed).

---

## 2. Fixtures and data lifecycle

**Source:** `smoke-test/AGENTS.md` (Isolation),
`auth_session` / `graph_client` / `openapi_graph_client` / `clear_graph_cache`
in `smoke-test/conftest.py`.

**Session-scoped:** `auth_session`, `graph_client`, `openapi_graph_client`.
**Function-scoped autouse:** `clear_graph_cache`.

Default for a fixture that owns a **shared dataset name**: rewrite to a
run-unique name, ingest, yield the URN, delete. No pre-ingest delete — the URN
is new each run.

```python
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

**Source:** `smoke-test/tests/tags_and_terms/tags_and_terms_test.py`.

Fallback when fixture keys are **already unique** to the module:
`_ingest_cleanup_data_impl` (pre-delete → ingest → yield → cleanup).

**Rules:**

- BLOCKER: Tests that create entities MUST clean them up.
- BLOCKER: Fixture-managed data MUST use `yield` teardown (pytest always runs it).
- WARNING: `_ingest_cleanup_data_impl` only when URNs are already unique to
  this module. Do not require pre-delete on `_ingest_cleanup_unique_dataset_impl`.
- SUGGESTION: Multi-entity fixtures rewrite each key with
  `materialize_with_unique_name` (see `tests/containers/containers_test.py`).

---

## 3. Authentication and HTTP

**Source:** `smoke-test/AGENTS.md` (Rules), `TestSessionWrapper` in
`smoke-test/tests/utils.py`, `make_step_actor_user` in
`smoke-test/tests/utilities/multi_user.py`.

`TestSessionWrapper` injects a Bearer token, clones header dicts, waits on
POST/PUT, and revokes the token on destroy.

**Rules:**

- BLOCKER: Never create auth tokens inline — use the `auth_session` fixture.
- BLOCKER: Never hardcode credentials — use `get_admin_credentials()` /
  `env_vars` getters.
- WARNING: Do not bypass `TestSessionWrapper` with raw `requests.get/post`.
- WARNING: Extra users via `make_step_actor_user()`, not a one-off signup.
- SUGGESTION: Do not call `wait_for_writes_to_sync()` after every GraphQL
  POST unless asserting search or index state — the wrapper already waits.

---

## 4. Retry and consistency

**Source:** `smoke-test/AGENTS.md` (Rules), `with_test_retry` in
`smoke-test/tests/utils.py`, `wait_for_writes_to_sync` in
`smoke-test/tests/consistency_utils.py`.

Never use `time.sleep()` to wait for GMS, search, or Kafka.

| Situation                               | Mechanism                                                                         |
| --------------------------------------- | --------------------------------------------------------------------------------- |
| Read-after-write assertion              | `@with_test_retry()`                                                              |
| After bulk ingest or cleanup            | `wait_for_writes_to_sync()` (`mcp_only` / `mae_only` when only one store matters) |
| One known async write with a `trace_id` | Trace API (`/openapi/v1/trace/write/{trace_id}`)                                  |
| Custom timing                           | `tenacity` with `stop_after_delay`                                                |

**Rules:**

- BLOCKER: Never use bare `time.sleep()` for eventual consistency.
- WARNING: Prefer `@with_test_retry()` over custom `tenacity.retry` for
  standard read-after-write.
- WARNING: Prefer Trace API over blanket `wait_for_writes_to_sync()` when
  confirming a single known write.
- SUGGESTION: Scope waits with `max_timeout_in_sec`, `mcp_only`, `mae_only`.

---

## 5. GraphQL and REST

**Source:** `execute_graphql` / `ingest_file_via_rest` in
`smoke-test/tests/utils.py`; per-module `restli_default_headers` (e.g.
`smoke-test/test_e2e.py`); `smoke-test/AGENTS.md` (Rules).

`execute_graphql()` already asserts a non-empty body, `data` is not `None`,
and no `errors` key.

**Rules:**

- WARNING: Use `execute_graphql()` instead of a manual GraphQL POST.
- WARNING: Assert specific field values, not that the response exists.
- WARNING: Rest.li calls need `X-RestLi-Protocol-Version: 2.0.0` (copy the
  per-module `restli_default_headers` dict; it is not in `utils.py`). Ingest
  with `ingest_file_via_rest()`, not a hand-rolled Pipeline.
- SUGGESTION: OpenAPI v3 multi-step tests use `concurrent_openapi.run_tests()`.
  Do not add fixtures that only re-check a GraphQL path already covered.
- SUGGESTION: Tags, terms, and descriptions via
  `tests/utilities/metadata_operations.py`, not copied GraphQL mutations.

---

## 6. Markers

**Source:** `smoke-test/pyproject.toml` (`[tool.pytest.ini_options] markers`),
`Domain` in `smoke-test/tests/utilities/domains.py`, `smoke-test/AGENTS.md`
(Markers), `global_policy_mutator` handling in `smoke-test/conftest.py`.

| Marker                  | When to use                                                                                                        |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `domain(...)`           | **Required** on new modules. `platform`, `observe`, `ingestion`, `ai`, `catalog`. Span by listing each.            |
| `p0`                    | Only for regressions that must run on every PR.                                                                    |
| `read_only`             | Only if the test never creates, mutates, or deletes entities.                                                      |
| `global_policy_mutator` | Module disables default policies or mutates shared platform policy. CI runs these serially after parallel modules. |
| `dependency()`          | Rare; keep chains short.                                                                                           |

`no_cypress_suite1` is legacy batching — do not require it on new tests.

**Rules:**

- WARNING: New test modules must declare `pytest.mark.domain(...)`.
- WARNING: `read_only` tests must not create, modify, or delete entities.
- BLOCKER: Mutating admin `corpUserInfo`, default "All Users" policies, or
  other shared platform state without `global_policy_mutator`.
- WARNING: `@pytest.mark.dependency()` chains should stay ≤3 levels.

---

## 7. Environment variables

**Source:** `smoke-test/tests/utilities/env_vars.py`, `smoke-test/AGENTS.md`
(Rules).

**Rules:**

- BLOCKER: New tests must use `env_vars.py` getters for DataHub configuration.
  Do not hardcode URLs, ports, or hostnames.
- WARNING: Tests that need Docker/`docker exec` for Kafka lag must honor
  `USE_STATIC_SLEEP`.
- WARNING: Credential defaults (`datahub`/`datahub`) are for local dev; tests
  must allow `ADMIN_USERNAME` / `ADMIN_PASSWORD` override.

---

## 8. Guaranteed cleanup

**Source:** `smoke-test/AGENTS.md` (Rules, Isolation), `_ingest_cleanup_*` in
`smoke-test/conftest.py`.

| Where created  | Teardown                                                                                    |
| -------------- | ------------------------------------------------------------------------------------------- |
| Module fixture | `yield` (pytest always runs it)                                                             |
| Inside a test  | `try/finally`; wrap cleanup in `try/except` so a delete failure does not hide the assertion |

**Rules:**

- BLOCKER: Entities created mid-test MUST be cleaned up in `try/finally`.
- BLOCKER: Fixture-managed data MUST use `yield` teardown.
- WARNING: Cleanup `finally` blocks should catch their own errors.
- SUGGESTION: Prefer fixture lifecycle when setup is shared across tests.

---

## 9. Multi-environment configuration

**Source:** `smoke-test/tests/utilities/env_vars.py`,
`smoke-test/tests/consistency_utils.py` (`USE_STATIC_SLEEP`).

**Rules:**

- BLOCKER: Never hardcode `localhost`, port numbers, or URLs. Use `env_vars.py`.
- WARNING: Docker-dependent waits must have a `USE_STATIC_SLEEP` fallback.
- SUGGESTION: Skip Docker-only tests when `env_vars.get_k8s_cluster_enabled()`.

---

## 10. Concurrent testing

**Source:** `smoke-test/tests/utilities/concurrent_test_runner.py`,
`concurrent_openapi.py`, `smoke-test/AGENTS.md`.

**Rules:**

- WARNING: Concurrent tests must be thread-safe (no shared mutable state).
- SUGGESTION: Use `run_concurrent_tests()` for parametric API testing.

---

## 11. Placement, logging, and quality

**Source:** `smoke-test/AGENTS.md` (When to add smoke tests, Rules),
root `AGENTS.md` (Testing Principles, Confidentiality).

**Rules:**

- WARNING: New tests go in `tests/<feature>/`, not `test_e2e.py`.
- WARNING: Use `logger.info()`, not `print()`.
- BLOCKER: No customer identifiers, ticket IDs, or real hostnames in fixtures,
  names, or comments. Use placeholders (`my_db.my_schema.events`, `col_a`).
- SUGGESTION: Do not match exact error strings or combinatorial input
  matrices. Assert user-visible behavior through public APIs.

---

## Anti-patterns (automatic blockers)

- Empty or trivial tests
- Missing cleanup
- Hardcoded URLs, ports, or credentials
- Inline authentication
- Bare `time.sleep()` for consistency
- Shared hardcoded URNs across modules
- Global mutable state
- Mutating shared platform policy without `global_policy_mutator`
- Overly broad assertions (response exists, full JSON dump)
- Commented-out test code
