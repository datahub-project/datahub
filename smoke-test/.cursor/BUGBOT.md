# smoke-test Bugbot rules

If a PR adds or extends tests under `smoke-test/tests/cypress/`, then:

- Flag: Cypress is deprecated (2026-06-30). New UI automation must use Playwright.

If code under `smoke-test/tests/read_only/` asserts `total > 0`, inlines GraphQL
in the test file, or adds test-local helpers instead of `metadata_operations.py`,
then:

- High: read-only tests must work on empty deployments.

If an existing smoke test is tagged `@pytest.mark.release_tests` (not
`release_tests_extended`), then:

- High: `smoke.sh` excludes `release_tests`, silently dropping smoke coverage.
- Body: "Use release_tests_extended for nightly-only extended coverage."

If a test registers multiple mocks/intercepts for the same GraphQL/operation, then:

- Flag flaky mock races; one active mock per operation.
