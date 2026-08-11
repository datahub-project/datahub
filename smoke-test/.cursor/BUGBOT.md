# smoke-test Bugbot rules

If a PR adds or extends tests under `smoke-test/tests/cypress/`, then:

- Flag: Cypress is deprecated (2026-06-30). New UI automation must use Playwright.

If code under `smoke-test/tests/read_only/` asserts a non-empty deployment state
(e.g. `total > 0`, required entity presence) without handling the empty case,
then:

- High: read-only tests must work on empty deployments.
- Do not flag merely for inlining GraphQL or using a test-local helper when the
  assertions tolerate empty results.

If a Cypress test registers multiple independently active `cy.intercept` handlers
that can concurrently match the same request without deterministic ordering,
then:

- Flag flaky mock races.
- Do not flag sequential or explicitly ordered intercepts, or intentional
  single-handler replacements.
- Playwright `page.route` guidance lives under `e2e-test/` (not here).
