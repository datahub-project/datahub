import { test, expect } from '../../fixtures/base-test';
import { HomePage } from '../../pages/home.page';
import { GLOBAL_FEATURE_FLAGS } from '../../utils/test-feature-flags';

test.describe('Homepage Basic Visibility', () => {
  let homePage: HomePage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags(GLOBAL_FEATURE_FLAGS);
    homePage = new HomePage(page, logger, logDir);
    await homePage.navigateToHome();
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
});
