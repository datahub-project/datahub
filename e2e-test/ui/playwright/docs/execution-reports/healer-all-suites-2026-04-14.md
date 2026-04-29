# Execution Report: Full Suite Healing

## Run Details

- Date: 2026-04-14
- Environment: http://localhost:9002
- Affected Suites: search, business-attributes, incidents-v2, onboarding
- Total Failures Analyzed: 48

## Root Causes

### Root Cause 1 — Stale Auth State (47 tests)

All 47 test failures in `search/`, `business-attributes/`, and `incidents-v2/` shared the
same root cause: the saved auth state file (`.auth/datahub.json`) contained expired cookies.

- `PLAY_SESSION` cookie: expired ~108 hours before the test run
- `actor` cookie: expired ~108 hours before the test run

The `loginFixture` checked only if the file existed, not whether the session cookies were
still valid. All tests loaded the stale state and landed on the login dialog instead of the
expected authenticated page.

### Root Cause 2 — Over-broad Dialog Selector (1 test)

`onboarding/welcome-modal.spec.ts › LocalStorage Persistence › should not show modal again
after localStorage flag is set` used `page.getByRole('dialog')` without any further scoping.
This matched any dialog that appeared on the page — including the AntDesign Modal wrapper
around the "What's New In DataHub" toast or other transient dialogs — causing intermittent
failures in `expectModalNotVisible()`.

## Healing Actions

### Fix 1 — `fixtures/login.fixture.ts`

Added `isStateFileValid()` helper that reads the saved state file and checks whether any
explicitly-dated cookie has expired. The `context` fixture now:

1. Calls `isStateFileValid()` before reusing the saved file.
2. Logs a warning and deletes the stale file when cookies are expired.
3. Falls through to the normal UI-login path to obtain a fresh session.

### Fix 2 — `pages/welcome-modal-page.ts`

Narrowed the `modal` locator from the broad `page.getByRole('dialog')` to
`page.getByRole('dialog').filter({ hasText: 'Welcome to DataHub' })`. This scopes all
visibility assertions and waits to the specific welcome modal dialog, preventing other
dialogs visible on the home page from causing false failures.

## Files Modified

- `e2e-test/ui/playwright/fixtures/login.fixture.ts`
- `e2e-test/ui/playwright/pages/welcome-modal-page.ts`

## Expected Test Status After Fixes

| Suite                                                     | Tests | Expected                                |
| --------------------------------------------------------- | ----- | --------------------------------------- |
| search/search.spec.ts                                     | 7     | PASS                                    |
| search/searchv2.spec.ts                                   | 16    | PASS                                    |
| search/search-filters.spec.ts                             | 5     | PASS                                    |
| search/query-and-filter-search.spec.ts                    | 11    | PASS                                    |
| business-attributes/business-attribute-navigation.spec.ts | 2     | PASS (or SKIP if feature flag disabled) |
| business-attributes/business-attribute.spec.ts            | 6     | PASS (or SKIP if feature flag disabled) |
| business-attributes/check-feature-flag.spec.ts            | 1     | PASS (or SKIP)                          |
| incidents-v2/v2-incidents.spec.ts                         | 1     | PASS                                    |
| onboarding/welcome-modal.spec.ts (targeted test)          | 1     | PASS                                    |

Note: business-attribute tests call `test.skip(!enabled, ...)` when the
`businessAttributeEntityEnabled` feature flag is false, so they may show as SKIPPED rather
than FAILED on environments where the flag is not set.
