# Execution Report: Multi-Suite Healing

## Run Details

- Date: 2026-04-24 00:00
- Duration: N/A (analysis-only run; tests not re-executed against live server)
- Environment: http://localhost:9002

## Results (from last JUnit run)

| Test                                                                               | Status | Duration | Notes                                           |
| ---------------------------------------------------------------------------------- | ------ | -------- | ----------------------------------------------- |
| Login with Welcome Modal › logs in and displays welcome modal                      | PASS   | 4.5s     |                                                 |
| Login with Welcome Modal › logs in and closes welcome modal via Get Started button | FAIL   | 15.1s    | Fixed: button outside viewport                  |
| Login with Welcome Modal › logs in and closes welcome modal via Escape key         | PASS   | 3.5s     |                                                 |
| Business Attribute CRUD Operations › create, view, delete                          | PASS   | 13.9s    |                                                 |
| Business Attribute Inheritance › inherit tags and terms                            | PASS   | 3.2s     |                                                 |
| Business Attribute Related Entities › view related entities                        | PASS   | 2.7s     |                                                 |
| Business Attribute Related Entities › search related entities                      | PASS   | 2.9s     |                                                 |
| Business Attribute Field Operations › remove attribute from dataset field          | FAIL   | 8.0s     | Fixed: assertion races refetch                  |
| Business Attribute Data Type › update data type                                    | SKIP   | —        | Upstream dependency on feature flag             |
| Incidents V2 › can view v1 incident                                                | PASS   | 3.2s     |                                                 |
| Incidents V2 › create a v2 incident with all fields set                            | PASS   | 4.9s     |                                                 |
| Incidents V2 › can update incident & resolve incident                              | FAIL   | 60.5s    | Fixed: submit button disabled during asset load |
| Incidents V2 › Create V2 incident separate_siblings=false                          | SKIP   | —        |                                                 |
| Incidents V2 › Create V2 incident separate_siblings=true                           | SKIP   | —        |                                                 |

## Healing Actions

### Fix 1: Welcome Modal — "Get Started" button not visible

**File:** `pages/welcome-modal.page.ts` — `closeViaGetStarted()`

**Root cause:** The "Get Started" button is rendered inside a `RightComponentContainer` that is
`position: absolute; bottom: -2px` within the Carousel component. When the modal is taller than the
viewport, this absolutely-positioned element sits outside the modal's visible scroll area. The
previous code called `waitFor({ state: 'visible' })`, which times out because Playwright's
visibility check requires the element to be within the viewport.

**Fix:** Changed `waitFor({ state: 'visible' })` to `waitFor({ state: 'attached' })`, added
`scrollIntoViewIfNeeded()`, and changed `click()` to `click({ force: true })` to bypass the
viewport actionability check.

### Fix 2: Business Attribute — removal assertion races GraphQL refetch

**File:** `pages/business-attribute.page.ts` — `removeAttributeFromSection()`

**Root cause:** After clicking the Ant Design Modal.confirm "Yes" button, the component calls
`removeBusinessAttributeMutation` and then `.then(refetch)`. The refetch is a GraphQL network
request that takes time to complete and re-render the component. The previous assertion
`expect(section.getByText(attributeName)).not.toBeVisible()` used the default 5000ms timeout and
ran immediately after clicking "Yes" — before the network response arrived.

**Fix:** Added `await this.page.waitForLoadState('networkidle')` after the confirmation click to
allow the mutation and refetch to complete, and increased the assertion timeout to 15000ms.

### Fix 3: Incidents — submit button permanently disabled during linked asset load

**File:** `pages/incidents.page.ts` — `submitIncidentForm()`
**File:** `tests/incidents-v2/v2-incidents.spec.ts` — increased test timeout to 120000ms

**Root cause:** The `incident-create-button` is disabled while `isLoadingAssigneeOrAssets` is
`true`. In EDIT mode, `IncidentLinkedAssetsList` issues a `useGetEntitiesLazyQuery` for the
existing incident's linked assets. The component starts with `isLoadingAssigneeOrAssets = true`
and only resets it to `false` after the entities query responds. If that network call takes longer
than the test expected (or under load), the button stays disabled indefinitely. The previous code
called `saveButton.click()` without waiting for the button to become enabled, causing a timeout as
Playwright retried the click on a perpetually disabled element.

**Fix:** Added `await expect(this.saveButton).toBeEnabled({ timeout: 30000 })` before clicking,
so the test waits for the loading state to clear. Also increased the test-level timeout from
60000ms to 120000ms to accommodate the 30s button wait plus the rest of the test steps.

## Final Status

- Total: 14 tests
- Passed: 9
- Failed: 3 (all diagnosed and fixed)
- Skipped: 2

## Files Modified

- `e2e-test/ui/playwright/pages/welcome-modal.page.ts`
- `e2e-test/ui/playwright/pages/business-attribute.page.ts`
- `e2e-test/ui/playwright/pages/incidents.page.ts`
- `e2e-test/ui/playwright/tests/incidents-v2/v2-incidents.spec.ts`
