# PR #17103 — devashish2203 Review Comments: Action Plan

Open comments from the review of https://github.com/datahub-project/datahub/pull/17103.
Comments already replied to in conversation are excluded.

---

## 1. Remove `@deprecated` alias for `resolvedUsers`

**File:** [data/users.ts](../data/users.ts#L54)
**Comment:** "Why the deprecation notice for something that isn't live? We can simply do the rename and fix any ongoing PRs"

**Action:** Delete the `/** @deprecated */` export alias entirely. Do a global search for any remaining `resolvedUsers` imports in the repo and update them to `users` in the same commit.

---

## 2. Create `GraphQLResponse` type alias

**File:** [helpers/graphql-helper.ts](../helpers/graphql-helper.ts#L29)
**Comment:** "nit: Create typealias for `Record<string, unknown>` as `GraphQLResponse` and use that as type."

**Action:** Add `export type GraphQLResponse = Record<string, unknown>;` in `graphql-helper.ts` (or a new `types/graphql.ts`). Replace every occurrence of `Record<string, unknown>` used as a GraphQL return type with `GraphQLResponse`.

---

## 3. Move `child-option` locator to constructor

**File:** [pages/incidents.page.ts](../pages/incidents.page.ts#L219)
**Comment:** "Locator should not be part of function calls. Move to constructor."

**Action:** This locator is parameterised by `status` so it cannot be a static constructor property. Two options:

- Define a factory method `childOption(status: string)` that returns `this.page.locator(...)` — called from `filterByStatus` but not inline inside `.click()`.
- Or use `this.page.getByTestId(`child-option-${status}`)` inline, which is the Playwright-recommended pattern for dynamic locators.

Preferred: factory method on the class so the selector string lives in one place.

---

## 4. Remove redundant `waitFor` in `filterByStatus`

**File:** [pages/incidents.page.ts](../pages/incidents.page.ts#L220)
**Comment:** "Same as above." (redundant `waitFor` before `childOption.click()`)

**Action:** Remove `await childOption.waitFor({ state: 'visible', timeout: 10000 })`. Playwright's actionability check on `.click()` handles this.

---

## 5. Remove `waitFor` in `login()` method

**File:** [pages/login.page.ts](../pages/login.page.ts#L19)
**Comment:** "Redundant check" — `await this.usernameInput.waitFor({ state: 'visible' })` before `.fill()`.

**Action:** Remove the `waitFor` line. Playwright's `.fill()` already waits for the element to be visible and enabled before interacting.

---

## 6. Remove `expect*` methods from `WelcomeModalPage` POM

**File:** [pages/welcome-modal.page.ts](../pages/welcome-modal.page.ts#L81)
**Comment:** "We agreed that having explicit `expect` functions are not needed in POM." (Slack thread linked)

**Action:** Delete `expectModalVisible()`, `expectModalTitleVisible()`, and any other `expect*` wrapper methods from `WelcomeModalPage`. Move the `expect(...)` assertions directly into the test files that call them. Page Objects should only expose actions and locators.

---

## 7. Document reason for Node engine downgrade

**File:** [package.json](../package.json#L20)
**Comment:** "Why the downgrade of node version? Is this related to the change to yarn?"

**Action:** Add an inline comment above the `engines` field explaining the reason:

- The CI environment (and some dev machines) run Node 18 LTS
- `>=22.0.0` was blocking those environments
- The downgrade is unrelated to yarn — yarn itself has no Node 22 requirement

---

## 9. Add `html` reporter back for CI

**File:** [playwright.config.ts](../playwright.config.ts#L12)
**Comment:** "We can leave html for CI as well, can be useful if the merge-reports step fails."

**Action:** Add `['html']` back to the CI reporter array alongside `blob` and `junit`:

```ts
reporter: process.env.CI
  ? [['blob'], ['html'], ['junit', { outputFile: 'test-results/junit.xml' }]]
  : [['html'], ['junit', { outputFile: 'test-results/junit.xml' }]];
```

---

## Summary

| #   | File                                  | Change type                                             | Effort  |
| --- | ------------------------------------- | ------------------------------------------------------- | ------- |
| 1   | `data/users.ts`                       | Remove deprecated alias + grep rename                   | Small   |
| 2   | `helpers/graphql-helper.ts`           | Add `GraphQLResponse` type alias                        | Small   |
| 3   | `pages/incidents.page.ts`             | Extract dynamic locator to factory method               | Small   |
| 4   | `pages/incidents.page.ts`             | Remove redundant `waitFor` before `childOption.click()` | Trivial |
| 5   | `pages/login.page.ts`                 | Remove redundant `waitFor` before `fill()`              | Trivial |
| 6   | `pages/welcome-modal.page.ts` + specs | Remove `expect*` from POM, move to tests                | Medium  |
| 7   | `package.json`                        | Add inline comment for Node version                     | Trivial |
| 8   | `playwright.config.ts`                | Add `html` to CI reporter                               | Trivial |
