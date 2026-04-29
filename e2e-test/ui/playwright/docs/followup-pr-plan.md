# Follow-up PR Plan — Playwright Framework Cleanup

Tracks every open reviewer comment from PR #16479 not yet resolved.
Comments from `devashish2203`, `benjiaming`, and `chriscollins3456`.

---

## 1. File Naming & Structure

### 1a. Page file naming convention

**Reviewer:** devashish2203
Rename all files under `pages/` to `<name>.page.ts` or `<name>.component.ts`:

- `base-page.ts` → `base.page.ts`
- `search-page.ts` → `search.page.ts`
- `dataset-page.ts` → `dataset.page.ts`
- `business-attribute-page.ts` → `business-attribute.page.ts`
- `welcome-modal-page.ts` → `welcome-modal.page.ts`
- `login-page.ts` → `login.page.ts`

### 1b. Move non-fixture files out of `fixtures/`

**Reviewer:** devashish2203

- `fixtures/cleanup.ts` → `utils/cleanup.ts`
- `fixtures/test-data.ts` → `utils/test-data.ts`
- `fixtures/users.ts` → `data/users.ts`
- `fixtures/mock-data.ts` → rename to test-data and move to `data/`; stop calling these "mock-data"

### 1c. Remove `test-context.ts` pattern

**Reviewer:** devashish2203
`fixtures/test-context.ts` creates a shared context that needs updating for every new PageObject. Remove it — let tests import PageObjects directly.

### 1d. Move seeders into a subfolder

**Reviewer:** devashish2203
`helpers/cli-seeder.ts`, `helpers/graphql-seeder.ts`, `helpers/rest-seeder.ts`, `helpers/python-seeder.ts` → `helpers/seeders/`

### 1e. Move navigation helpers into BasePage

**Reviewer:** devashish2203
`helpers/navigation-helper.ts` functions use `page` — they should be methods on `BasePage`, not standalone helpers.

### 1f. Remove or justify `utils/selectors.ts`

**Reviewer:** devashish2203
Selectors outside Page Object Models. Move any still-needed selectors into the relevant page class and delete the file.

### 1g. Remove Python seeder if unused

**Reviewer:** devashish2203
`helpers/python-seeder.ts` appears unused. Remove it, or if needed, replace the inline Python string with a real `.py` file that gets read and executed.

---

## 2. Naming Renames

| Current                                       | Target                                                       | Reviewer      |
| --------------------------------------------- | ------------------------------------------------------------ | ------------- |
| `featureName` fixture param (base-test.ts:72) | `featureDataLoader`                                          | devashish2203 |
| `resolvedUsers` (users.ts:54)                 | `users`                                                      | devashish2203 |
| `users` (default list)                        | `defaultUsers`                                               | devashish2203 |
| `loginTest` fixture                           | keep, but compose via `mergeTests`                           | devashish2203 |
| `fixtures/auth.ts`                            | `fixtures/login.ts` (already partially done, finish cleanup) | devashish2203 |

---

## 3. User Credentials — Use Env Variables

**Reviewer:** benjiaming, devashish2203
Currently credentials are read from a file that CI must create. Fragile. Switch to env variables with defaults (like Cypress):

```ts
// playwright.config.ts
use: {
  adminUsername: process.env.ADMIN_USERNAME ?? 'datahub',
  adminPassword: process.env.ADMIN_PASSWORD ?? 'datahub',
}
```

Remove the file-based credential loading from `fixtures/users.ts:19` and the "hack" on line 48.

Also remove the `role` field from user objects — it can drift out of sync with the actual environment and serves no validated purpose.

---

## 4. Configuration

### 4a. Route `BASE_URL` through playwright config

**Reviewer:** devashish2203 (`login-test.ts:59`)
Tests read `process.env.BASE_URL` directly. Instead, set `baseURL` in `playwright.config.ts` and consume it via Playwright's built-in `baseURL` test parameter.

### 4b. Add blob reporter for CI sharding

**Reviewer:** devashish2203 (`playwright.config.ts:9`)
Enable `blob` reporter conditionally when `CI=true` so sharded runs can merge reports.

### 4c. Console logger only in non-CI runs

**Reviewer:** devashish2203 (`utils/logger.ts:53`)
Guard the console transport with `!process.env.CI`.

### 4d. Handle log file overwrite on retry

**Reviewer:** devashish2203 (`utils/logger.ts:61`)
`test.log` is overwritten when a test retries. Append retry index to the log filename so CI keeps both attempts.

---

## 5. Timeouts — Replace Magic Numbers with Constants

**Reviewer:** devashish2203 (`welcome-modal-page.ts:61`)
Define timeout constants and use them everywhere:

```ts
// utils/timeouts.ts
const SHORT_WAIT = ACTION_TIMEOUT * 2;
const MEDIUM_WAIT = ACTION_TIMEOUT * 3;
const LONG_WAIT = ACTION_TIMEOUT * 4;
```

Replace all raw `waitForTimeout(1000)` / `waitForTimeout(5000)` calls across pages and specs.

---

## 6. Reduce / Replace Explicit Waits

**Reviewer:** benjiaming, devashish2203

- `search-page.ts:92` — explicit timeout is a Cypress-style anti-pattern. Replace with `waitForResponse` on `getSearchResultsForMultiple` / `aggregateAcrossEntities`.
- `search` method vs `searchAndWait` — `search` should await GraphQL responses, not just a raw timeout.
- `welcome-modal-page.ts:90` — review `waitFor` / `waitForTimeout` calls; replace with auto-retrying `expect` assertions where possible.
- `waitForSlideChange` — move to `utils/wait-helpers.ts`.
- `waitForModalToClose` or similar patterns → `utils/wait-helpers.ts`.

---

## 7. BasePage Improvements

**Reviewer:** devashish2203 (`base-page.ts:35`)
`navigateTo` / `waitForPageLoad` should also call `waitForLoadState('domcontentloaded')`.

Clarify when `waitForPageLoad` in `login-page.ts:22` should vs. shouldn't be called — add a comment or rename for clarity.

---

## 8. Cleanup Helper

**Reviewer:** devashish2203

- Separate **global** cleanup (teardown of entire suites) from **per-test** cleanup helpers into two files: `utils/global-cleanup.ts` and `utils/cleanup.ts`.
- Make `namePattern` a required parameter (currently optional, which risks deleting all entities of a type).
- API endpoint URLs → named constants instead of inline strings.
- GraphQL mutation strings → named constants in a shared file.

---

## 9. Deduplication

**Reviewer:** benjiaming, devashish2203

| Duplicated thing                                        | Action                                                           |
| ------------------------------------------------------- | ---------------------------------------------------------------- |
| `extractUrn` (multiple implementations)                 | Single export from `utils/urn.ts`; add `type Urn = string` alias |
| `waitForSync` (cli-seeder + graphql-seeder)             | Extract to `utils/wait-helpers.ts`                               |
| GMS URL construction (appears in base-test and seeders) | Extract to `utils/api-utils.ts`                                  |
| `waitForTimeout` wrapping network calls in seeders      | Extract to shared `executeWithRetry` or similar                  |

---

## 10. Type Safety

**Reviewer:** devashish2203, benjiaming

- Eliminate `any` types in `factories/mock-response-factory.ts` and elsewhere — use proper response schema types or `unknown` + type guards.
- Add `type Urn = string` in `utils/urn.ts` and use it everywhere an URN string is passed.

---

## 11. Mock / GraphQL Helpers

**Reviewer:** devashish2203

- `helpers/graphql-helper.ts`: split real GraphQL request helpers and mock intercept helpers into separate files.
- Use `route.fallback()` instead of `route.continue()` in mock handlers so multiple mocks can compose correctly.
- Feature flag toggling → extract to a `featureFlagFixture` rather than inlining in graphql-helper.

---

## 12. Test Data Factory

**Reviewer:** devashish2203

- `test-data-factory.ts`: split into domain-scoped files under a `test-data/` folder as it grows.
- Extract `generateRandomString` to `utils/random.ts`.
- Replace per-entity random string functions with a single `appendRandom(prefix: string): string`.
- Consider using `YYYYMMDD_HHmmss` timestamp suffix instead of pure random for traceability.

---

## 13. Business Attribute Page Locators

**Reviewer:** devashish2203, benjiaming

- Locators are too broad (e.g., `getByText('Delete')`, `getByText('Yes')`). Narrow from a well-known parent element.
- Input locators: use `getByRole('textbox')` or include `type="text"` attribute, not just placeholder.
- Replace remaining `console.log` calls with `logger.info`.

---

## 14. Fixtures Re-export (base-test.ts:97)

**Reviewer:** devashish2203
Remove the re-export block at line 97 if it's just forwarding things test files shouldn't need from `base-test`. Let each test import directly from its source.

---

## 15. `login-test.ts` — Auth State Comment

**Reviewer:** devashish2203 (`login.ts:42`)
Clarify in the comment whether `storageState` / `auth-setup` is still required with the new login fixture, or if the auth setup project can be removed.

---

## 16. `auth.setup.ts` — testIgnore Conflict

**Reviewer:** benjiaming
The file header comment says setup files run separately, but `playwright.config.ts` has `testIgnore: /.*\.setup\.ts/`. Clarify whether setup files run as a project or are ignored, and update the comment accordingly.

---

## 17. README — Use yarn, Specific Test Targeting

**Reviewer:** benjiaming, chriscollins3456

- Replace all `npm` commands in `README.md` with `yarn`.
- Add a section on how to run a single test file or test by name (`yarn playwright test <file> --grep "<name>"`).

---

## 18. `search-filters.spec.ts` Duplicate Line

**Reviewer:** benjiaming (line 77)
Line 77 duplicates line 70. Remove the redundant assertion.

---

## 19. Clarify Multiple Seeders

**Reviewer:** devashish2203
Add a top-level comment or README section in `helpers/seeders/` explaining why each seeder exists and when to use CLI vs REST vs GraphQL seeding.

---

## 20. ESLint + Prettier

**Reviewer:** devashish2203 (top-level review body)
Add ESLint and Prettier to the Playwright project for consistent code style. Can be a standalone PR — configuration should mirror the rest of the repo where possible.

---

## 21. Hardcoded Absolute Paths in `search-data.setup.ts`

**Reviewer:** benjiaming (lines 42, 74, 106)
`cwd` is set to a hardcoded developer-local path (`'/Users/priyabratadas/datahub/...'`). This breaks on every other machine. Replace with `__dirname`-relative resolution:

```ts
// Before
cwd: '/Users/priyabratadas/datahub/e2e-test/ui/playwright/tests';

// After
cwd: path.resolve(__dirname);
```

Apply to all three `execAsync` call sites (lines 42, 74, 106).

---

## 22. `login-with-welcome-modal.spec.ts` — Move Setup to `beforeEach`

**Reviewer:** devashish2203 (`login-with-welcome-modal.spec.ts:25`)
The `localStorage.removeItem`, `page.goto`, and `loginPage.login` calls at lines 23–28 are inside the test body, not in `beforeEach`. Each test that needs this setup will repeat it. Move to `beforeEach` so the pattern is consistent with other spec files.

---

## 23. `WelcomeModalPage` — Method Rename + Unnecessary Wait

**Reviewer:** devashish2203 (`welcome-modal-page.ts:80`, `:86`)

- Rename `expectModalTitle()` → `expectModalTitleVisible()` to make the assertion nature explicit.
- `closeViaButton` calls `waitForCarouselReady()` unconditionally, which waits for loading dots to disappear. If the carousel has already loaded, this wait is wasted. Guard it: only wait if the loading indicator is actually present.

---

## 24. Product Tour / Welcome Modal Not Dismissed Before Search Tests

**Reviewer:** benjiaming (`search-filters.spec.ts:6`)
Search tests fail locally because the Welcome Modal / Product Tour overlays the page. The modal must be dismissed (or `skipWelcomeModal` set in localStorage) as part of the global or per-feature setup, not left to individual tests. Verify the base-test fixture handles this for all authenticated test sessions.

---

## 25. `constants.ts` — "Data Sources" Naming + Stale Comment

**Reviewer:** devashish2203 (`constants.ts`, `constants.ts:13`)

- The UI calls it "Data Sources" — align the constant label accordingly.
- Update the comment on line 13 after `users.ts` is moved to `data/users.ts` and variables are renamed (see Section 2).

---

## Out of Scope (Noted for Later)

- Blob reporter with multi-runner sharding (needs CI infra changes)
- Eliminating all `waitForTimeout` calls (some are unavoidable without app-side test hooks)
- `any` types in full (need GraphQL schema generation wired in)
- `featureFlagFixture` (needs more investigation of feature flag mechanism)
