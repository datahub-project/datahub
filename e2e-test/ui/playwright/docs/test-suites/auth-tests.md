# Authentication Test Documentation

## Overview

- **Feature**: Authentication (Login & Login with Welcome Modal)
- **Test Suite**: `tests/auth/`
- **Page Objects**: `pages/login-page.ts`, `pages/welcome-modal-page.ts`
- **Total Tests**: 9 (6 login + 3 login-with-welcome-modal)
- **Last Updated**: 2026-04-10

---

## Test Coverage Summary

### By Priority

- **P0 (Critical/Smoke)**: 3 tests
- **P1 (High/Functional)**: 4 tests
- **P2 (Medium/Edge Cases)**: 2 tests

### By Type

- **Smoke**: 3 tests
- **Functional**: 4 tests
- **Edge Cases**: 2 tests

---

## Test Cases

### File: `tests/auth/login.spec.ts`

Suite: `Login`

**Preconditions (beforeEach)**: `localStorage.setItem('skipWelcomeModal', 'true')` is injected to suppress the welcome modal across all tests in this file.

---

#### Test 1: logs in successfully with valid credentials

**Priority**: P0
**Type**: Smoke

**Objective**: Verify that a valid admin user can log in and land on the home page with a greeting.

**Prerequisites**:

- Fresh, unauthenticated browser context (provided by `login-test` fixture)
- Admin credentials available via `resolvedUsers.admin`

**Steps**:

1. Navigate to `/`
2. Enter admin username and password via `loginPage.login()`
3. Wait up to 10 seconds for greeting text

**Expected Results**:

- A greeting matching `/Good (morning|afternoon|evening)/` is visible
- URL matches `/$` or `/home`

---

#### Test 2: displays login form elements

**Priority**: P0
**Type**: Smoke

**Objective**: Verify the login page renders all required form elements.

**Prerequisites**:

- Fresh, unauthenticated context

**Steps**:

1. Navigate to the login page via `loginPage.navigateToLogin()`

**Expected Results**:

- Username input is visible
- Password input is visible
- Login button is visible

---

#### Test 3: shows error for invalid credentials

**Priority**: P1
**Type**: Functional

**Objective**: Verify that submitting invalid credentials shows an error message.

**Prerequisites**:

- Fresh, unauthenticated context

**Steps**:

1. Navigate to the login page
2. Fill username with `invalid_user`
3. Fill password with `invalid_password`
4. Click the login button
5. Wait up to 5 seconds for error text

**Expected Results**:

- Error text matching `/Invalid credentials|Login failed/i` is visible

---

#### Test 4: disables login button when username is empty

**Priority**: P1
**Type**: Functional

**Objective**: Verify the login button is disabled when only password is filled.

**Prerequisites**:

- Fresh, unauthenticated context

**Steps**:

1. Navigate to the login page
2. Fill password with `datahub`
3. Leave username empty

**Expected Results**:

- Login button is disabled
- URL still matches `/login`

---

#### Test 5: disables login button when password is empty

**Priority**: P1
**Type**: Functional

**Objective**: Verify the login button is disabled when only username is filled.

**Prerequisites**:

- Fresh, unauthenticated context

**Steps**:

1. Navigate to the login page
2. Fill username with `datahub`
3. Leave password empty

**Expected Results**:

- Login button is disabled
- URL still matches `/login`

---

#### Test 6: redirects to home page after successful login

**Priority**: P0
**Type**: Smoke

**Objective**: Verify post-login redirect and home page logo render.

**Prerequisites**:

- Fresh, unauthenticated context
- Admin credentials available

**Steps**:

1. Navigate to `/`
2. Log in with admin credentials
3. Wait for network idle

**Expected Results**:

- URL matches `/$` or `/home`
- Logo image with accessible name `/logo/i` is visible

---

### File: `tests/auth/login-with-welcome-modal.spec.ts`

Suite: `Login with Welcome Modal`

**Note**: Each test removes `skipWelcomeModal` from localStorage before login so the welcome modal appears.

---

#### Test 7: logs in and displays welcome modal for first-time users

**Priority**: P1
**Type**: Functional

**Objective**: Verify the welcome modal appears automatically after login when the skip flag is absent.

**Prerequisites**:

- Fresh, unauthenticated context
- `skipWelcomeModal` removed from localStorage

**Steps**:

1. Clear `skipWelcomeModal` from localStorage
2. Navigate to `/` and log in with admin credentials
3. Wait for modal to appear

**Expected Results**:

- Welcome modal is visible
- Modal title is rendered
- Modal can be closed via the close button
- Greeting text is visible after modal closes

---

#### Test 8: logs in and closes welcome modal via Get Started button

**Priority**: P1
**Type**: Functional

**Objective**: Verify the full carousel can be navigated and the modal dismissed via "Get Started".

**Prerequisites**:

- Fresh, unauthenticated context
- `skipWelcomeModal` removed from localStorage

**Steps**:

1. Clear `skipWelcomeModal` and log in
2. Confirm modal is visible
3. Click the last carousel dot to jump to the final slide
4. Verify the final slide is visible
5. Click "Get Started" to close the modal

**Expected Results**:

- Final slide is visible before dismissal
- Greeting text is visible after modal closes

---

#### Test 9: logs in and closes welcome modal via Escape key

**Priority**: P2
**Type**: Edge Case

**Objective**: Verify the welcome modal can be dismissed via the keyboard Escape key.

**Prerequisites**:

- Fresh, unauthenticated context
- `skipWelcomeModal` removed from localStorage

**Steps**:

1. Clear `skipWelcomeModal` and log in
2. Confirm modal is visible
3. Press the Escape key

**Expected Results**:

- Modal is dismissed
- Greeting text is visible after modal closes

---

## Page Objects Used

### LoginPage

**File**: `pages/login-page.ts`

**Key Properties**:

- `usernameInput` — username text field
- `passwordInput` — password text field
- `loginButton` — submit button

**Key Methods**:

- `navigateToLogin()` — navigate to `/login`
- `login(username, password)` — fill credentials and submit

### WelcomeModalPage

**File**: `pages/welcome-modal-page.ts`

**Key Methods**:

- `expectModalVisible()` / `expectModalNotVisible()` — assert modal state
- `expectModalTitle()` — assert title is rendered
- `closeViaButton()` — click the X close button
- `closeViaGetStarted()` — click the "Get Started" CTA
- `closeViaEscape()` — press Escape key
- `clickLastCarouselDot()` — navigate to the final carousel slide
- `expectFinalSlideVisible()` — assert the final slide content

---

## Test Execution

### Run All Auth Tests

```bash
cd e2e-test/ui/playwright
npx playwright test tests/auth/
```

### Run Login Tests Only

```bash
npx playwright test tests/auth/login.spec.ts
```

### Run Login + Welcome Modal Tests

```bash
npx playwright test tests/auth/login-with-welcome-modal.spec.ts
```

### Run a Specific Test by Name

```bash
npx playwright test tests/auth/login.spec.ts -g "logs in successfully"
```

---

## Dependencies

### Required Services

- DataHub backend (default: `localhost:9002` or env `BASE_URL`)
- Authentication service

### Fixtures Used

- `login-test` (from `fixtures/login-test.ts`) — provides fresh unauthenticated context, `loginPage`, `logger`, `logDir`

### Test Data

- Admin credentials: resolved from `fixtures/users.ts` → `resolvedUsers.admin`

---

## Known Issues / Limitations

- Welcome modal tests depend on the absence of `skipWelcomeModal` in localStorage; the `addInitScript` approach means this is set before page load, making it reliable but timing-sensitive on slow environments.
- The login error message test uses a regex (`/Invalid credentials|Login failed/i`) to accommodate different backend message formats.

---

## Related Documentation

- Login V2 Tests: `docs/test-suites/login-v2-tests.md`
- Onboarding / Welcome Modal Tests: `docs/test-suites/onboarding-tests.md`
- Page Object — LoginPage: `pages/login-page.ts`
- Page Object — WelcomeModalPage: `pages/welcome-modal-page.ts`

---

_Generated by The Scribe on 2026-04-10_
