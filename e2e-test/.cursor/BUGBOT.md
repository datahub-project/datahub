# Playwright / e2e Bugbot rules

If a **new** page-interaction helper (toast wait, init script, navigation
shortcut) is defined in a spec file rather than a Page Object Model class, then:

- Blocking for new helpers. Put helpers on a POM (`BasePage` /
  `BaseSettingsPage` / feature page), not inline in the spec.
- Title: "Move helper into POM"

If a spec calls `page.addInitScript(...)` (or similar page-setup APIs) directly
instead of a POM wrapper (e.g. `HomeV2Page.skipIntroducePage()`), then:

- Blocking for new call sites. Move the call into the page object.
- Do not flag ordinary in-spec calls like `page.click`, `page.reload`,
  `page.waitForURL`, or assertions that use an existing POM.

If a spec asserts toasts with raw `expect(...).toHaveText` / locators instead of
the shared toast helper, then:

- Flag as Medium and point to `this.toast.expectVisible(...)` /
  `ToastComponent` on `BasePage` (there is no `BaseSettingsPage.waitForToast`).

If a constructor, fixture, or `test.use` signature changes, then:

- Check sibling specs/fixtures for stale calls that would fail compilation or
  runtime. Flag missing updates as High.

If a spec under `e2e-test/ui/playwright/tests/` imports `test` (the runner) from
`@playwright/test` instead of `fixtures/base-test` or `fixtures/login-test`,
then:

- Flag Medium: bypasses auth/logging/mocking composition.
- Body: "Import `test` from fixtures/base-test or fixtures/login-test."
- Type-only or utility imports (`Page`, `Locator`, `Route`, `request`, `expect`
  as a type/matcher when `test` already comes from fixtures) are fine.

If a suite expects pre-seeded entities but does not set
`test.use({ featureName: '...' })` and does not self-seed via `apiMock`/API, then:

- Flag High flaky/missing-data risk.
- Body: "featureName loads tests/<feature>/fixtures/data.json once per worker."

If a test registers multiple independently active `page.route` handlers that can
concurrently match the same request without deterministic ordering, then:

- Flag flaky mock races.
- Do not flag repeated `apiMock.mockGraphQL` / `interceptGraphQLResponse` calls
  that intentionally replace a single map entry for one operation.
