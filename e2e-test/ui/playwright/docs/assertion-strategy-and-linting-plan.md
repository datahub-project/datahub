# Playwright Test Standards: Assertion Strategy + Linting Plan

Branch: `playwright/standardize-assertions-and-linting`

## Context

This plan covers three improvements to the Playwright test suite, surfaced in code review
[#16479](https://github.com/datahub-project/datahub/pull/16479/changes#r3057922456) and follow-up
discussion:

1. **Assertion strategy** — settle Option 1 vs Option 2 across all POM files
2. **waitFor / hardcoded timeout comments** — explain why auto-waiting is insufficient
3. **ESLint + Prettier setup** — the playwright directory currently has neither

---

## Decision: Assertion Strategy

**Adopted: Option 2 with Option 1 for multi-step assertions**

> Public locators in POM. Simple `expect()` calls written directly in tests.
> POM assertion methods kept only when multiple steps are needed (e.g., scroll + wait + assert).

### Rationale

- Locators are already `readonly` (public) in every POM — no structural change required.
- Cleaner POMs: removing single-line `expectX` wrappers reduces bloat without sacrificing clarity.
- Test readability improves: `await expect(page.modal).toBeVisible()` is self-documenting.
- POM still owns the locator _definition_, so renaming a selector stays a one-place change.
- Complex multi-step sequences (scroll into view → wait for DOM → assert) stay as POM methods
  because they encode non-obvious Playwright workarounds, not just assertion intent.

### Classification guide

| Keep as POM method                                                                       | Move assertion to test                                                 |
| ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Multi-step: scroll + wait + assert                                                       | Single `expect(locator).toBeVisible()`                                 |
| Works around a known Playwright limitation (e.g. IntersectionObserver, Ant Design focus) | Any `toContainText`, `toBeHidden`, `toHaveText` on a top-level locator |
| Shared across 3+ tests and the steps are non-trivial                                     | Locator attribute checks (`getAttribute`)                              |

---

## Files Affected

### POM files — assertion methods to remove (move to test)

#### `pages/welcome-modal.page.ts`

| Method                            | Disposition                                                                  |
| --------------------------------- | ---------------------------------------------------------------------------- |
| `expectModalTitleVisible()`       | Remove — replace with `expect(page.modalTitle).toBeVisible()` in test        |
| `expectSlide1Visible()`           | Borderline — calls `waitForCarouselReady()` first. **Keep** (multi-step)     |
| `expectSlide2Visible()`           | Keep — calls `waitForCarouselReady()` + `waitForSlideChange(1)`              |
| `expectSlide3Visible()`           | Keep — calls `waitForCarouselReady()`                                        |
| `expectFinalSlideVisible()`       | Keep — calls `waitForCarouselReady()`                                        |
| `expectGetStartedButtonVisible()` | Remove — single `expect().toBeVisible()`                                     |
| `expectDocsLinkVisible()`         | Remove — single `expect().toBeVisible()`                                     |
| `expectModalVisible()`            | Keep — has a custom timeout (10 000 ms) that should stay in POM with comment |
| `expectModalNotVisible()`         | Remove — simple `expect().not.toBeVisible()`                                 |

#### `pages/incidents.page.ts`

| Method                                    | Disposition                                                                                                                       |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `expectIncidentRowExists(title, timeout)` | Remove — replace with `await expect(incidentsPage.getIncidentRow(title)).toBeVisible({ timeout })` in test                        |
| `expectIncidentNameBadgeVisible()`        | **Keep** — `scrollIntoViewIfNeeded()` before the assertion is a multi-step workaround; removing it would silently skip the scroll |
| `expectIncidentGroupVisible()`            | Remove — single `expect().toBeVisible()`                                                                                          |
| `expectDrawerTitleContains()`             | Keep — custom timeout param is meaningful context                                                                                 |
| `expectIncidentRowStage()`                | Keep — scoped child locator lookup                                                                                                |
| `expectIncidentRowCategory()`             | Keep — scoped child locator lookup                                                                                                |
| `expectResolveButtonContains()`           | Keep — scoped child locator lookup                                                                                                |
| `expectResolveButtonVisible()`            | Keep — scoped child locator lookup                                                                                                |

### Test files — assertions to write directly

After removing the POM wrappers, update these test files:

- `tests/auth/login-with-welcome-modal.spec.ts`
- `tests/onboarding/welcome-modal.spec.ts`
- `tests/incidents-v2/v2-incidents.spec.ts`

---

## waitFor / Hardcoded Timeout Comment Policy

> **Rule (from Devashish's review):** Any use of `waitFor`, `page.waitForTimeout`, or a hardcoded
> `timeout:` value in a test or POM must be accompanied by a comment explaining why Playwright's
> [auto-waiting](https://playwright.dev/docs/actionability) is insufficient for that case.

### Locations requiring comments (current audit)

| File                    | Line / Pattern                                                              | Reason to document                                                       |
| ----------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `welcome-modal.page.ts` | `waitFor({ state: 'hidden', timeout: 15000 })` in `waitForCarouselReady`    | Loading spinner is a custom component, not a navigation event            |
| `welcome-modal.page.ts` | `page.waitForTimeout(500)` in `clickCarouselDot`                            | Slick carousel CSS animation has no DOM signal                           |
| `welcome-modal.page.ts` | `waitFor({ state: 'visible', timeout: 5000 })` in `closeViaButton`          | Modal render timing                                                      |
| `welcome-modal.page.ts` | `waitForFunction` in `waitForSlideChange`                                   | Active dot index is a CSS class change, not a visibility event           |
| `incidents.page.ts`     | `waitFor({ state: 'visible', timeout: 15000 })` in `clickCreateIncidentBtn` | Button cycles through loading → main test ID before being clickable      |
| `incidents.page.ts`     | `waitFor({ state: 'attached', timeout: 10000 })` in `selectStatus`          | IntersectionObserver gates rendering; element is attached before visible |
| `incidents.page.ts`     | `page.waitForLoadState('networkidle')` in multiple methods                  | GraphQL responses still in-flight after navigation                       |

---

## ESLint + Prettier Setup

The `e2e-test/ui/playwright/` directory currently has no linting or formatting tooling. This means
formatting inconsistencies and common anti-patterns go uncaught in CI.

### Proposed setup

1. **Prettier** — add `.prettierrc` (or `prettier.config.js`) with project-consistent settings
   (matching the root repo: `singleQuote: true, trailingComma: 'all', printWidth: 100`).
2. **ESLint** — add `eslint.config.mjs` (flat config) with:
    - `typescript-eslint` for TS rules
    - `eslint-plugin-playwright` for Playwright-specific rules (e.g., no raw `page.waitForTimeout` without justification, prefer locator assertions over element handles)
3. **package.json scripts** — add `"lint": "eslint ."` and `"format": "prettier --write ."`.
4. **CI** — wire `lint` + `format:check` into the existing Playwright CI job
   ([`.github/workflows/` playwright job](https://github.com/datahub-project/datahub/blob/master/.github/workflows/)).

### Packages to add (dev dependencies)

```
eslint
typescript-eslint
eslint-plugin-playwright
prettier
```

### Open question for team

Should we block CI on lint failures from the start, or introduce as a warning-only pass first to
avoid a flood of pre-existing issues? Recommend: fix all existing issues in this PR, then enforce
hard failures in CI.

---

## Implementation Order

1. [ ] Add ESLint + Prettier config and fix all existing issues (new files only, don't touch
       unrelated code).
2. [ ] Add `waitFor`/timeout comments to POM files per the audit table above.
3. [ ] Remove simple assertion wrappers from POMs (per classification table).
4. [ ] Update test files to write assertions directly.
5. [ ] Wire lint + format check into CI.
6. [ ] Update `README.md` in the playwright directory with the new conventions.

---

## Resolved Decisions

| Question                                                                                        | Decision                                                                                                                                                                    |
| ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `expectIncidentNameBadgeVisible` — keep POM method or split scroll into separate action?        | **Keep as-is.** The `scrollIntoViewIfNeeded()` call is integral; splitting it would risk callers forgetting the scroll step.                                                |
| `expectIncidentRowExists(title, timeout)` — timeout in POM or passed via `expect({ timeout })`? | **Pass from test.** Remove the POM wrapper; callers write `await expect(incidentsPage.getIncidentRow(title)).toBeVisible({ timeout: 15000 })` and own the timeout decision. |
