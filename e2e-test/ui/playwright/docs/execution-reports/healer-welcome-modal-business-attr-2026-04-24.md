# Execution Report: Welcome Modal & Business Attributes Healing

## Run Details

- Date: 2026-04-24
- Environment: chromium
- Healer: playwright-test-healer

## Failing Tests (Input)

| Test                                                                                                              | File                                                 | Line | Status        |
| ----------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- | ---- | ------------- |
| Login with Welcome Modal › logs in and closes welcome modal via Get Started button                                | tests/auth/login-with-welcome-modal.spec.ts          | 38   | FAIL          |
| Business Attributes › Business Attribute Related Entities › should view related entities for a business attribute | tests/business-attributes/business-attribute.spec.ts | 88   | FAIL          |
| Business Attributes › Business Attribute Related Entities › should search related entities by query               | tests/business-attributes/business-attribute.spec.ts | 101  | SKIPPED (0ms) |
| Business Attributes › Business Attribute Field Operations › should remove business attribute from dataset field   | tests/business-attributes/business-attribute.spec.ts | 119  | SKIPPED (0ms) |
| Business Attributes › Business Attribute Data Type › should update the data type of a business attribute          | tests/business-attributes/business-attribute.spec.ts | 134  | SKIPPED (0ms) |

## Root Cause Analysis

### Failure 1: login-with-welcome-modal.spec.ts:38

**Root cause**: Timing race in the carousel `afterChange` callback.

The `clickLastCarouselDot()` method used a fixed 500ms wait (`page.waitForTimeout(500)`) after clicking the last dot. React-slick's default animation speed is 500ms, and `afterChange` fires _after_ the CSS animation completes. The 500ms fixed wait was unreliable — it could fire before `afterChange` propagated the state update (`currentSlide = lastIndex`) through React. As a result, when `closeViaGetStarted()` looked for the "Get started" button (which only renders in `rightComponent` when `currentSlide === TOTAL_CAROUSEL_SLIDES - 1`), the button was not yet present in the DOM, causing a 10s timeout and test failure.

The same race affected `expectFinalSlideVisible()` — the "Ready to Get Started?" heading can become partially visible in the DOM before `afterChange` fires (react-slick renders slides in an off-screen track), so the heading check passed while the button state had not yet updated.

### Failure 2: business-attribute.spec.ts:88

**Root cause**: Elasticsearch indexing lag after GraphQL mutation.

The test sequence is serial:

1. Inheritance test (line 68) adds `PlaywrightAttribute` to the `event_name` schema field via GraphQL mutation.
2. Related Entities test (line 88) immediately navigates to the Related Entities tab and expects non-zero results.

The Related Entities tab queries Elasticsearch (not the RDBMS). Elasticsearch indexing is asynchronous and can lag 1–30+ seconds behind GraphQL mutations. The `waitForLoadState('networkidle')` only confirms the HTTP response arrived, not that the ES index is current.

The `expectNoRelatedEntities()` call (`expect(page.getByText('of 0')).not.toBeVisible()`) had a 30s Playwright default timeout, but the React component does not automatically re-fetch data — it shows what ES returned on initial load. A page reload is required to trigger a fresh ES query. Without retry-with-reload logic, the test timed out after 30s showing "of 0".

Tests at lines 101, 119, 134 were **skipped** (0ms) due to `test.describe.configure({ mode: 'serial' })` — once test 88 fails, subsequent tests in the same describe group are not executed.

## Healing Actions

### 1. Fixed `clickLastCarouselDot` in `pages/welcome-modal.page.ts`

Replaced the unreliable `page.waitForTimeout(500)` with `waitForSlideChange(lastIndex, 10000)`, which polls the DOM until `slick-active` appears on the last dot. This confirms `afterChange` has fired and React's `currentSlide` state is up to date, guaranteeing the "Get started" button is rendered before `closeViaGetStarted()` attempts to click it.

```typescript
// BEFORE
await this.lastCarouselDot.click();
await this.page.waitForTimeout(500);

// AFTER
const totalDots = await this.carouselDots.count();
const lastIndex = totalDots - 1;
await this.lastCarouselDot.click();
await this.waitForSlideChange(lastIndex, 10000);
```

### 2. Fixed `expectFinalSlideVisible` in `pages/welcome-modal.page.ts`

Added `waitForSlideChange(lastIndex)` before the heading assertion. This ensures we wait for `afterChange` to fire (confirming slide transition is complete and `currentSlide` is updated) before asserting the heading is visible.

### 3. Added `expectRelatedEntitiesExist` in `pages/business-attribute.page.ts`

New method that retries the "of 0 not visible" assertion with page reloads (45s total, 3s intervals). Each reload triggers a fresh Elasticsearch query, eventually catching the indexed state.

```typescript
async expectRelatedEntitiesExist(): Promise<void> {
    await expect(async () => {
        await this.page.reload();
        await this.page.waitForLoadState('networkidle');
        await expect(this.page.getByText('of 0')).not.toBeVisible({ timeout: 5000 });
    }).toPass({ timeout: 45000, intervals: [3000] });
}
```

The existing `expectNoRelatedEntities()` (no reload) is preserved for test 101 which uses it after a search query (where reloading would lose the query state).

### 4. Updated test 88 in `tests/business-attributes/business-attribute.spec.ts`

Changed to use `expectRelatedEntitiesExist()` and increased timeout from 60s to 120s to accommodate the ES indexing wait plus the 45s retry budget.

```typescript
// BEFORE
test.setTimeout(60000);
await businessAttributePage.expectNoRelatedEntities();

// AFTER
test.setTimeout(120000);
await businessAttributePage.expectRelatedEntitiesExist();
```

## Files Modified

- `e2e-test/ui/playwright/pages/welcome-modal.page.ts`
- `e2e-test/ui/playwright/pages/business-attribute.page.ts`
- `e2e-test/ui/playwright/tests/business-attributes/business-attribute.spec.ts`

## Expected Results After Fix

| Test                                | Expected Status                                        |
| ----------------------------------- | ------------------------------------------------------ |
| login-with-welcome-modal.spec.ts:38 | PASS — button reliably found after afterChange         |
| business-attribute.spec.ts:88       | PASS — ES indexing wait via reload retry               |
| business-attribute.spec.ts:101      | PASS — unblocked; ES indexed by time test 88 completes |
| business-attribute.spec.ts:119      | PASS — unblocked                                       |
| business-attribute.spec.ts:134      | PASS — unblocked                                       |
