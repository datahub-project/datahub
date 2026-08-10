# Playwright / e2e Bugbot rules

If a test or helper uses `page.addInitScript`, toast assertions, or other page
interactions outside a Page Object Model class, then:
- Blocking (for new helpers). Page interaction helpers must live in a POM class
  (e.g. `BaseSettingsPage`), not in the spec file.
- Title: "Move helper into POM"
- Body: "Use or extend the existing POM (`waitForToast`, `skipIntroducePage`, etc.)
  instead of inlining page interactions in the spec."

If a spec asserts toasts with raw `expect(...).toHaveText` / locators instead of
`waitForToast` / POM helpers, then:
- Flag as Medium and point to `BaseSettingsPage.waitForToast(text)`.

If a constructor, fixture, or `test.use` signature changes, then:
- Check sibling specs/fixtures for stale calls that would fail compilation or
  runtime. Flag missing updates as High.

If a spec under `e2e-test/ui/playwright/tests/` imports from `@playwright/test`
instead of `fixtures/base-test` or `fixtures/login-test`, then:
- Flag Medium: bypasses auth/logging/mocking composition.
- Body: "Import test from fixtures/base-test or fixtures/login-test."

If a suite expects pre-seeded entities but does not set
`test.use({ featureName: '...' })` and does not self-seed via `apiMock`/API, then:
- Flag High flaky/missing-data risk.
- Body: "featureName loads tests/<feature>/fixtures/data.json once per worker."


If a test registers multiple mocks/intercepts for the same GraphQL/operation, then:
- Flag flaky mock races; one active mock per operation.
