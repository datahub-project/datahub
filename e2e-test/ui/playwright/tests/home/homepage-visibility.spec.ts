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
});
