# Onboarding (Welcome Modal) Test Documentation

## Overview

- **Feature**: Welcome to DataHub Onboarding Modal
- **Test Suite**: `tests/onboarding/`
- **Page Objects**: `pages/welcome-modal-page.ts`, `pages/login-page.ts`
- **Total Tests**: 20 (1 skipped)
- **Last Updated**: 2026-04-10

---

## Test Coverage Summary

### By Priority

- **P0 (Critical/Smoke)**: 4 tests
- **P1 (High/Functional)**: 8 tests
- **P2 (Medium/Edge Cases)**: 7 tests
- **Skipped**: 1 test (auto-advance timing — flaky by nature)

### By Type

- **Smoke**: 4 tests
- **Functional**: 8 tests
- **Edge Cases**: 7 tests
- **Regression**: 1 test

---

## Test Cases

### File: `tests/onboarding/welcome-modal.spec.ts`

Suite: `Welcome to DataHub Modal`

**Preconditions (beforeEach)**:

1. Instantiate `WelcomeModalPage` with page, logger, and logDir
2. Navigate to login page
3. Clear `skipWelcomeModal` from localStorage via `welcomeModalPage.clearLocalStorage()`
4. Log in with admin credentials

All tests start immediately post-login with a clean localStorage state.

---

### Sub-suite: First-Time User Experience

#### Test 1: should display modal automatically on first visit to homepage

**Priority**: P0
**Type**: Smoke

**Objective**: Verify the welcome modal appears automatically when `skipWelcomeModal` is absent from localStorage.

**Prerequisites**:

- `skipWelcomeModal` cleared from localStorage
- User logged in

**Steps**:

1. Navigate to the home page

**Expected Results**:

- Welcome modal is visible
- Modal title is rendered
- First slide content is visible

---

#### Test 2: should not display modal when skipWelcomeModal is set

**Priority**: P1
**Type**: Functional

**Objective**: Verify the modal is suppressed when the localStorage flag is present before navigation.

**Steps**:

1. Set `skipWelcomeModal` in localStorage
2. Navigate to the home page

**Expected Results**:

- Welcome modal is NOT visible

---

#### Test 3: should set skipWelcomeModal in localStorage after closing

**Priority**: P1
**Type**: Functional

**Objective**: Verify closing the modal persists the skip flag so it won't reappear.

**Steps**:

1. Navigate to home page — modal appears
2. Close via the X button
3. Read `skipWelcomeModal` from localStorage

**Expected Results**:

- `localStorage.getItem('skipWelcomeModal')` equals `'true'`

---

### Sub-suite: Carousel Navigation

#### Test 4: should display first slide by default

**Priority**: P0
**Type**: Smoke

**Steps**:

1. Navigate to home page

**Expected Results**:

- Modal is visible
- Slide 1 content is visible

---

#### Test 5: should navigate to slides via carousel dots

**Priority**: P1
**Type**: Functional

**Steps**:

1. Navigate to home page — modal visible on slide 1
2. Click carousel dot at index 1
3. Click carousel dot at index 2

**Expected Results**:

- After dot 1 click: slide 2 content is visible
- After dot 2 click: slide 3 content is visible

---

#### Test 6: should navigate to final slide and display CTA elements

**Priority**: P1
**Type**: Functional

**Steps**:

1. Navigate to home page
2. Click the last carousel dot

**Expected Results**:

- Final slide is visible
- "Get Started" button is visible
- Documentation link is visible

---

#### Test 7: (SKIPPED) should auto-advance slides after 10 seconds

**Priority**: P2
**Type**: Functional
**Status**: Skipped — timing-sensitive; reliable only in non-parallelised, full-speed runs.

---

### Sub-suite: Close Interactions

#### Test 8: should close modal via close button

**Priority**: P0
**Type**: Smoke

**Steps**:

1. Navigate to home page — modal visible
2. Click the X close button

**Expected Results**:

- Modal is no longer visible

---

#### Test 9: should close modal via Get Started button

**Priority**: P0
**Type**: Smoke

**Steps**:

1. Navigate to home page
2. Click the last carousel dot to reach the final slide
3. Click "Get Started"

**Expected Results**:

- Modal is no longer visible

---

#### Test 10: should close modal via ESC key

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Navigate to home page — modal visible
2. Press the Escape key

**Expected Results**:

- Modal is no longer visible

---

#### Test 11: should close modal by clicking outside

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Navigate to home page — modal visible
2. Click outside the modal overlay

**Expected Results**:

- Modal is no longer visible

---

### Sub-suite: LocalStorage Persistence

#### Test 12: should persist skipWelcomeModal after closing via close button

**Priority**: P1
**Type**: Functional

**Steps**:

1. Navigate to home page
2. Close via X button
3. Read localStorage

**Expected Results**:

- `skipWelcomeModal` equals `'true'`

---

#### Test 13: should persist skipWelcomeModal after closing via Get Started

**Priority**: P1
**Type**: Functional

**Steps**:

1. Navigate to home page
2. Click last carousel dot then "Get Started"
3. Read localStorage

**Expected Results**:

- `skipWelcomeModal` equals `'true'`

---

#### Test 14: should not show modal again after localStorage flag is set

**Priority**: P1
**Type**: Regression

**Steps**:

1. Navigate to home page — modal appears, close it
2. Navigate to home page again

**Expected Results**:

- Modal does NOT appear on the second visit

---

### Sub-suite: Documentation Link

#### Test 15: should display DataHub Docs link on final slide

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Navigate to home page, click last carousel dot
2. Read the `href` attribute of the docs link

**Expected Results**:

- Docs link `href` equals `https://docs.datahub.com/docs/category/features`

---

#### Test 16: should open DataHub Docs in new tab

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Navigate to home page, click last carousel dot
2. Read `target` and `rel` attributes of the docs link

**Expected Results**:

- `target` equals `_blank`
- `rel` equals `noopener noreferrer`

---

### Sub-suite: Analytics Tracking

#### Test 17: should track modal view event on open

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Intercept POST requests to `**/track`
2. Navigate to home page — modal visible
3. Wait 1 second for tracking to fire
4. Inspect intercepted requests

**Expected Results**:

- At least one request with `type === 'WelcomeToDataHubModalViewEvent'` is captured

---

#### Test 18: should track interact event on slide change

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Intercept `**/track` requests
2. Navigate to home page
3. Click carousel dot at index 1
4. Wait 1 second

**Expected Results**:

- At least one request with `type === 'WelcomeToDataHubModalInteractEvent'` is captured

---

#### Test 19: should track exit event with correct exit method

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Intercept `**/track` requests
2. Navigate to home page
3. Close via X button
4. Wait 1 second

**Expected Results**:

- One request with `type === 'WelcomeToDataHubModalExitEvent'` is captured
- `exitMethod` property equals `'close_button'`

---

#### Test 20: should track documentation link click event

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Intercept `**/track` requests
2. Navigate to home page, reach final slide
3. Click the docs link
4. Wait 1 second

**Expected Results**:

- One request with `type === 'WelcomeToDataHubModalClickViewDocumentationEvent'`
- `url` property equals `https://docs.datahub.com/docs/category/features`

---

### Sub-suite: Edge Cases

#### Test 21: should handle rapid carousel navigation

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Navigate to home page
2. Click dot 1, dot 2, then dot 0 in rapid succession

**Expected Results**:

- Final active slide is slide 1 (dot 0 was last clicked)

---

#### Test 22: should reset carousel to first slide when reopened

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Navigate to home page
2. Navigate to slide 3 (dot index 2)
3. Close the modal
4. Clear localStorage and navigate to home page again

**Expected Results**:

- Modal reopens on slide 1

---

#### Test 23: should handle closing modal mid-carousel

**Priority**: P2
**Type**: Edge Case

**Steps**:

1. Navigate to home page
2. Click dot 1 to reach slide 2
3. Close modal via X button

**Expected Results**:

- Modal is no longer visible
- `skipWelcomeModal` equals `'true'`

---

## Page Objects Used

### WelcomeModalPage

**File**: `pages/welcome-modal-page.ts`

**Key Properties**:

- `docsLink` — the DataHub Docs anchor element on the final slide

**Key Methods**:

- `clearLocalStorage()` — remove `skipWelcomeModal` and related flags
- `setSkipWelcomeModal()` — set `skipWelcomeModal = 'true'`
- `getSkipWelcomeModalValue()` — read current localStorage value
- `navigateToHome()` — navigate to the home page
- `expectModalVisible()` / `expectModalNotVisible()` — assert modal presence
- `expectModalTitle()` — assert title text rendered
- `expectSlide1Visible()` / `expectSlide2Visible()` / `expectSlide3Visible()` — per-slide assertions
- `expectFinalSlideVisible()` — assert final slide CTA content
- `expectGetStartedButtonVisible()` / `expectDocsLinkVisible()` — CTA element assertions
- `clickCarouselDot(index)` — click a numbered dot (0-based)
- `clickLastCarouselDot()` — click the last dot to jump to the final slide
- `waitForSlideChange(slideIndex, timeout)` — wait for carousel transition
- `closeViaButton()` — click the X close button
- `closeViaGetStarted()` — click the "Get Started" CTA
- `closeViaEscape()` — press Escape key
- `closeViaOutsideClick()` — click outside the modal

### LoginPage

**File**: `pages/login-page.ts`

**Key Methods**:

- `navigateToLogin()` — navigate to `/login`
- `login(username, password)` — submit login form

---

## Test Execution

### Run All Onboarding Tests

```bash
cd e2e-test/ui/playwright
npx playwright test tests/onboarding/
```

### Run a Specific Sub-Suite

```bash
npx playwright test tests/onboarding/welcome-modal.spec.ts -g "Carousel Navigation"
```

### Run Analytics Tests

```bash
npx playwright test tests/onboarding/welcome-modal.spec.ts -g "Analytics Tracking"
```

---

## Dependencies

### Required Services

- DataHub backend with `/track` analytics endpoint enabled
- Authentication service

### Fixtures Used

- `login-test` (from `fixtures/login-test.ts`) — unauthenticated context, `loginPage`, `logger`, `logDir`

### Test Data

- Admin credentials: `resolvedUsers.admin`
- Analytics tracking endpoint: `**/track` (intercepted via `page.route`)

---

## Known Issues / Limitations

- Test 7 (auto-advance) is skipped because a 10-second timer is inherently unreliable in CI environments with variable execution speed.
- Analytics tests use `page.waitForTimeout(1000)` to allow tracking events to fire — these may need adjustment on particularly slow CI runners.
- The `closeViaOutsideClick()` method depends on the modal overlay implementation; changes to the overlay structure may break selector resolution.

---

## Related Documentation

- Auth Tests (login + modal): `docs/test-suites/auth-tests.md`
- Login V2 Tests: `docs/test-suites/login-v2-tests.md`
- Page Object: `pages/welcome-modal-page.ts`

---

_Generated by The Scribe on 2026-04-10_
