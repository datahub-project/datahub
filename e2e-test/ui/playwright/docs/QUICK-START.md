# Quick Start: Welcome Modal Tests

## Run Tests

```bash
# Navigate to test directory
cd /Users/priyabratadas/datahub/e2e-test/ui/playwright

# Run all welcome modal tests
npm test -- tests/onboarding/welcome-modal.spec.ts --project=chromium

# Run with UI mode (interactive)
npm run test:ui tests/onboarding/welcome-modal.spec.ts

# Run in headed mode (see browser)
npm run test:headed tests/onboarding/welcome-modal.spec.ts --project=chromium

# Run specific test by name
npm test -- tests/onboarding/welcome-modal.spec.ts --project=chromium -g "should display modal"
```

## View Results

```bash
# View HTML report
npm run show-report

# Report location
open playwright-report/index.html
```

## Test Status

✅ **All tests passing** (22/22 active tests)

- 1 test intentionally skipped (auto-advance video test)
- Zero failures
- Production ready

## Quick Commands

```bash
# Run all Playwright tests
npm test

# Run only onboarding tests
npm test tests/onboarding/

# Debug mode (step through tests)
npm run test:debug tests/onboarding/welcome-modal.spec.ts

# Run on different browsers
npm test -- tests/onboarding/welcome-modal.spec.ts --project=firefox
npm test -- tests/onboarding/welcome-modal.spec.ts --project=webkit
```

## File Locations

### Test Files

- **Test Spec**: `tests/onboarding/welcome-modal.spec.ts`
- **Page Object**: `pages/WelcomeModalPage.ts`
- **Fixtures**: `fixtures/test-context.ts`

### Documentation

- **Feature Doc**: `docs/features/welcome-to-datahub-modal-feature.md`
- **Selector Mapping**: `docs/migration-analysis/welcome-to-datahub-modal-selector-mapping.md`
- **Translation Guide**: `docs/migration-analysis/welcome-to-datahub-modal-translation-guide.md`
- **Audit Report**: `docs/audit-reports/welcome-modal-audit-2026-03-06.md`
- **Healing Report**: `docs/healing-reports/welcome-modal-healing-2026-03-06.md`
- **Success Summary**: `docs/MIGRATION-SUCCESS-SUMMARY.md`

## Test Coverage

The test suite covers:

1. **First-Time User Experience** (3 tests)

   - Modal displays on first visit
   - Respects localStorage skip flag
   - Sets skip flag after closing

2. **Carousel Navigation** (4 tests)

   - First slide displayed by default
   - Navigate via carousel dots
   - Final slide with CTAs
   - Auto-advance (skipped - documented)

3. **Close Interactions** (4 tests)

   - Close button
   - Get Started button
   - ESC key
   - Outside click

4. **LocalStorage Persistence** (3 tests)

   - Persist after close button
   - Persist after Get Started
   - Don't show again after flag set

5. **Documentation Link** (2 tests)

   - Link displayed on final slide
   - Opens in new tab with security attrs

6. **Analytics Tracking** (4 tests)

   - Modal view event
   - Slide interact event
   - Exit event with method
   - Documentation click event

7. **Edge Cases** (3 tests)
   - Rapid carousel navigation
   - Carousel reset on reopen
   - Close mid-carousel

## Prerequisites

### Required

- Node.js >= 18.0.0
- DataHub backend running at `http://localhost:9002`
- Valid test credentials (datahub/datahub)

### Optional

- Chrome/Chromium browser (for chromium project)
- Firefox browser (for firefox project)
- WebKit dependencies (for webkit project)

## Troubleshooting

### Backend not running

```bash
# Start DataHub backend
cd /Users/priyabratadas/datahub
./gradlew quickstartDebug
```

### Install Playwright browsers

```bash
cd /Users/priyabratadas/datahub/e2e-test/ui/playwright
npx playwright install chromium
```

### Clear test state

```bash
# Remove test artifacts
rm -rf test-results/
rm -rf playwright-report/
```

## CI/CD Integration

The tests are configured for CI with:

- 2 retries on failure
- Screenshots on failure
- Traces on first retry
- Single worker for consistency

Configuration in `playwright.config.ts`.

## Support

For issues or questions:

1. Check the comprehensive documentation in `/docs`
2. Review the audit report for known warnings
3. Verify backend is running and accessible
4. Ensure test credentials are valid

---

_Last Updated: 2026-03-06_
_Status: ✅ Production Ready_
