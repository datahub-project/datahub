import { test, expect } from '../../fixtures/base-test';
import { HomePage } from '../../pages/home.page';

test.describe('Homepage Basic Visibility', () => {
  let homePage: HomePage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    homePage = new HomePage(page, logger, logDir);
    await homePage.navigateToHome();
    await homePage.waitForPageLoad();
  });

  // Verify core homepage elements are rendered after navigation
  test('page title is visible on homepage', async () => {
    const isVisible = await homePage.isPageTitleVisible();
    expect(isVisible).toBe(true);
  });

  test('search bar is visible on homepage', async () => {
    const isVisible = await homePage.isSearchBarVisible();
    expect(isVisible).toBe(true);
  });

  // TEMP: fails on attempt 1, passes on attempt 2+ to verify PostHog flake detection
  test('temp-posthog-verification: fails on first attempt only', async () => {
    const attempt = parseInt(process.env.GITHUB_RUN_ATTEMPT ?? '1', 10);
    expect(attempt).toBeGreaterThan(1);
  });
});
