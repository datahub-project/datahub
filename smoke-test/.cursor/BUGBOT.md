# smoke-test Bugbot rules

Authoring guide: `smoke-test/AGENTS.md`. Do not flag style (logging, `domain(...)`,
placement in `test_e2e.py`) here.

If code under `smoke-test/tests/read_only/` asserts a non-empty deployment state
(e.g. `total > 0`, required entity presence) without handling the empty case,
then:

- High: read-only tests must work on empty deployments.
- Do not flag merely for inlining GraphQL or using a test-local helper when the
  assertions tolerate empty results.

If a new or changed test ingests, mutates, or deletes a **hardcoded** entity URN /
dataset name that another module could share (fixture `data.json` keys, GraphQL
variables, OpenAPI bodies) without `unique_suffix()`, `unique_dataset_urn()`,
`materialize_with_unique_name()`, `materialize_unique_dataset()`, or
`_ingest_cleanup_unique_dataset_impl`, then:

- High flake under xdist `--dist=loadscope`.
- Title: "Shared smoke-test URN"
- Do not flag `_ingest_cleanup_data_impl` when the fixture keys are already
  unique to that module.
- Do not flag read-only assertions against platform builtins (admin user,
  built-in policies).

If a test uses bare `time.sleep()` to wait for GMS, search, or Kafka, then:

- High. Use `@with_test_retry()`, `wait_for_writes_to_sync()` (`mcp_only` /
  `mae_only` when only one store matters), or `tenacity` with `stop_after_delay`.
- Title: "Bare sleep in smoke test"
- Do not flag sleeps that are not waiting on async metadata (e.g. exercising a
  clock/TTL).

If a test creates entities in the test body (emit, ingest, GraphQL create) and
has neither fixture `yield` teardown nor `try/finally` cleanup, then:

- High leftover data on failure.
- Title: "Missing smoke-test cleanup"
- Do not flag tests that only read existing/unique-fixture data.

If a test disables default / All Users policies or overwrites admin
`corpUserInfo` without `@pytest.mark.global_policy_mutator` (module or test),
then:

- High: CI runs mutators serially after parallel modules; unmarked mutators
  race other tests.
- Title: "Unmarked policy mutator"
