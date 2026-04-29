# Cypress Test Analysis

**File**: `smoke-test/tests/cypress/cypress/e2e/onboarding/welcome-to-datahub-modal.cy.js`
**Feature**: onboarding / welcome-modal
**Analysis Date**: 2026-04-28

---

## Test Structure

### Test Suite

- `describe("WelcomeToDataHubModal")`
  - Tests: 3
  - Setup: `beforeEach` — removes `skipWelcomeModal` from localStorage; calls `cy.skipIntroducePage()`
  - Teardown: `afterEach` — removes `skipWelcomeModal` from localStorage

### Individual Tests

#### Test 1: "should display the modal for first-time users"

**Steps**:

1. Intercept POST `**/track**` → stub 200 (alias: `trackEvents`)
2. `cy.loginForOnboarding()` — POST `/logIn` without setting skip flags
3. `cy.visit("/")`
4. Assert dialog is visible
5. Click last carousel `li`
6. Click "Get Started" button
7. Assert dialog no longer exists
8. Read `localStorage.getItem("skipWelcomeModal")` and assert it equals `"true"`

**Assertions**:

- `cy.findByRole("dialog").should("be.visible")`
- `cy.findByRole("dialog").should("not.exist")`
- `expect(win.localStorage.getItem("skipWelcomeModal")).to.equal("true")`

---

#### Test 2: "should not display the modal if user has already seen it"

**Steps**:

1. Set `localStorage.setItem("skipWelcomeModal", "true")`
2. `cy.loginForOnboarding()`
3. `cy.visit("/")`
4. Assert dialog does not exist

**Assertions**:

- `cy.findByRole("dialog").should("not.exist")`

---

#### Test 3: "should handle user interactions and track events"

**Steps**:

1. Intercept POST `**/track**` → stub 200 (alias: `trackEvents`)
2. `cy.loginForOnboarding()`
3. `cy.visit("/")`
4. Assert dialog is visible
5. `cy.findByLabelText(/close/i).click()` — close via close button
6. Assert dialog does not exist
7. Use `cy.get("@trackEvents.all")` to find the `WelcomeToDataHubModalExitEvent`
8. Assert `exitMethod === "close_button"`
9. Assert `currentSlide` is a number
10. Assert `totalSlides` is a number
11. Read localStorage and assert `skipWelcomeModal === "true"`

**Assertions**:

- `cy.findByRole("dialog").should("be.visible")`
- `cy.findByRole("dialog").should("not.exist")`
- `modalExitEvent.request.body.exitMethod === "close_button"`
- `modalExitEvent.request.body.currentSlide` is a number
- `modalExitEvent.request.body.totalSlides` is a number
- `localStorage.getItem("skipWelcomeModal") === "true"`

---

## Cypress Patterns Found

### Selectors (5 total)

| Selector                                            | Type        | Usage                      |
| --------------------------------------------------- | ----------- | -------------------------- |
| `cy.findByRole("dialog")`                           | Role        | Modal detection            |
| `cy.findByRole("dialog").find("li").last()`         | Role + DOM  | Last carousel dot          |
| `cy.findByRole("button", { name: /get started/i })` | Role + Name | Get Started CTA            |
| `cy.findByLabelText(/close/i)`                      | Label regex | Close button               |
| `cy.get("@trackEvents.all")`                        | Alias       | Intercepted track requests |

### Custom Commands (2 total)

| Command                   | Implementation                                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `cy.loginForOnboarding()` | POST `/logIn` with admin credentials via `cy.request()` — does NOT set skipOnboardingTour or skipWelcomeModal |
| `cy.skipIntroducePage()`  | Sets `localStorage.setItem("skipAcrylIntroducePage", "true")` directly (no cy.window)                         |

### Network Mocking (1 total)

| Pattern      | Method | Alias         | Stub                  |
| ------------ | ------ | ------------- | --------------------- |
| `**/track**` | POST   | `trackEvents` | `{ statusCode: 200 }` |

### Storage Operations

| Operation    | Key                                  | Location                   |
| ------------ | ------------------------------------ | -------------------------- |
| `removeItem` | `skipWelcomeModal`                   | `beforeEach` / `afterEach` |
| `setItem`    | `skipWelcomeModal` (value: `"true"`) | Test 2 setup               |
| `getItem`    | `skipWelcomeModal`                   | Test 1, Test 3 assertions  |

### Assertions (8 total)

| Assertion                                           | Type          |
| --------------------------------------------------- | ------------- |
| `.should("be.visible")` (dialog)                    | Visibility    |
| `.should("not.exist")` (dialog)                     | Non-existence |
| `expect(...).to.equal("true")` (localStorage)       | Equality      |
| `expect(...).to.equal("close_button")` (exitMethod) | Equality      |
| `expect(...).to.be.a("number")` (currentSlide)      | Type check    |
| `expect(...).to.be.a("number")` (totalSlides)       | Type check    |

---

## Test Dependencies

- **Authentication**: `cy.loginForOnboarding()` (direct API POST to `/logIn`)
- **Backend Services**: Analytics `/track` endpoint (stubbed with 200)
- **Feature Flags**: None
- **Test Data**: Admin credentials from Cypress env

## Complexity Assessment

- **Test Count**: 3
- **Total Assertions**: 8
- **Async Operations**: Network intercept, localStorage
- **Custom Commands**: 2 (`loginForOnboarding`, `skipIntroducePage`)
- **Network Mocks**: 1 (`**/track**`)

**Migration Complexity**: Low

---

## Migration Status

**Status**: COMPLETE

The Playwright migration (`tests/onboarding/welcome-modal.spec.ts`) was completed prior to this
formal analysis. The Playwright test suite covers all 3 Cypress tests and adds 20 additional
tests covering carousel navigation, multiple close methods, documentation link, analytics tracking,
and edge cases.

See `docs/migration-analysis/welcome-to-datahub-modal-selector-mapping.md` for selector translations.
