# Selector Migration Map

**Feature**: onboarding / welcome-modal
**Date**: 2026-04-28

---

## Selector Translations

### Direct Equivalents

| Cypress                                             | Playwright                                                           | Notes                                  |
| --------------------------------------------------- | -------------------------------------------------------------------- | -------------------------------------- |
| `cy.findByRole("dialog")`                           | `page.getByRole('dialog').filter({ hasText: 'Welcome to DataHub' })` | Scoped to avoid matching other dialogs |
| `cy.findByRole("button", { name: /get started/i })` | `page.getByRole('button', { name: /get started/i })`                 | Direct match                           |
| `cy.findByRole("heading", ...)`                     | `page.getByRole('heading', { name: '...' })`                         | Used for slide verification            |

### Updated Selectors

| Cypress                                     | Playwright                                         | Reason                                                                                              |
| ------------------------------------------- | -------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `cy.findByLabelText(/close/i)`              | `page.locator('[data-testid="modal-close-icon"]')` | The Ant Design close icon is not labeled by an aria-label; `data-testid` attribute is more reliable |
| `cy.findByRole("dialog").find("li").last()` | `page.locator('.slick-dots li').last()`            | Scoped to carousel dots specifically, not all `li` elements within dialog                           |

### Deprecated Selectors (Cypress only)

| Cypress                      | Notes                                                                                 |
| ---------------------------- | ------------------------------------------------------------------------------------- |
| `cy.get("@trackEvents.all")` | Cypress alias pattern; replaced by Playwright `page.route()` + push to array approach |

---

## Component Locations

| Selector                             | Component File                                                   |
| ------------------------------------ | ---------------------------------------------------------------- |
| `dialog` role ("Welcome to DataHub") | `datahub-web-react/src/app/onboarding/WelcomeToDataHubModal.tsx` |
| `[data-testid="modal-close-icon"]`   | Ant Design Modal component (via `@components` Modal wrapper)     |
| `.slick-dots li`                     | react-slick Carousel (via `@components` Carousel)                |
| `button` "Get started"               | `WelcomeToDataHubModal.tsx` line ~222                            |
| `link` "DataHub Docs"                | `WelcomeToDataHubModal.tsx` — `StyledDocsLink` on final slide    |

---

## Custom Command Translations

### `cy.loginForOnboarding()`

**Cypress implementation**: `cy.request({ method: "POST", url: "/logIn", body: { username, password } })`

- Does NOT set `skipOnboardingTour` or `skipWelcomeModal` flags

**Playwright equivalent**:

```typescript
await loginPage.navigateToLogin();
await loginPage.login(username, password);
// Do NOT call any method that sets skipWelcomeModal
```

The key difference: `loginPage.login()` submits the login form via the UI. The `clearLocalStorage()` call in `beforeEach` ensures `skipWelcomeModal` is absent before login.

### `cy.skipIntroducePage()`

**Cypress implementation**: `localStorage.setItem("skipAcrylIntroducePage", "true")`

**Playwright equivalent**: Not needed. The Playwright fixture context starts fresh; the introduce page is handled by the authenticated session flow and does not appear in Playwright tests targeting the welcome modal.

---

## Network Mocking Translation

### Track event stub

**Cypress**:

```javascript
cy.intercept('POST', '**/track**', { statusCode: 200 }).as('trackEvents');
```

**Playwright**:

```typescript
const trackRequests: Record<string, unknown>[] = [];
await page.route('**/track', (route) => {
  const postData = route.request().postData();
  if (postData) trackRequests.push(JSON.parse(postData));
  void route.continue(); // Let request through (don't stub)
});
```

Note: Playwright tests allow the track request through (`route.continue()`) rather than stubbing it. This is intentional — the real tracking endpoint is used, and the request body is captured for assertion.

---

## localStorage Translation

| Cypress                                                             | Playwright                                                        |
| ------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `cy.window().then(win => win.localStorage.removeItem("key"))`       | `await page.evaluate(() => localStorage.removeItem("key"))`       |
| `cy.window().then(win => win.localStorage.setItem("key", "value"))` | `await page.evaluate(() => localStorage.setItem("key", "value"))` |
| `cy.window().then(win => win.localStorage.getItem("key"))`          | `await page.evaluate(() => localStorage.getItem("key"))`          |

These are encapsulated in `WelcomeModalPage` methods:

- `clearLocalStorage()` — removes `skipWelcomeModal`
- `setSkipWelcomeModal()` — sets `skipWelcomeModal = 'true'`
- `getSkipWelcomeModalValue()` — returns current value

---

## Assertion Translation

| Cypress                              | Playwright                                                                                     |
| ------------------------------------ | ---------------------------------------------------------------------------------------------- |
| `.should("be.visible")`              | `await expect(locator).toBeVisible()`                                                          |
| `.should("not.exist")`               | `await expect(locator).toBeHidden()`                                                           |
| `expect(x).to.equal("true")`         | `expect(x).toBe('true')`                                                                       |
| `expect(x).to.equal("close_button")` | `expect(x?.['exitMethod']).toBe('close_button')`                                               |
| `expect(x).to.be.a("number")`        | Not explicitly tested in Playwright (currentSlide/totalSlides type check dropped as low-value) |
