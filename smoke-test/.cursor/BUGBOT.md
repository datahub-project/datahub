# smoke-test Bugbot rules

If code under `smoke-test/tests/read_only/` asserts a non-empty deployment state
(e.g. `total > 0`, required entity presence) without handling the empty case,
then:

- High: read-only tests must work on empty deployments.
- Do not flag merely for inlining GraphQL or using a test-local helper when the
  assertions tolerate empty results.
