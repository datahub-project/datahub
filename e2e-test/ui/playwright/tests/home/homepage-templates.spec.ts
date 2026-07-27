import { test, expect } from '../../fixtures/base-test';
import { HomePage } from '../../pages/home.page';

test.describe('Homepage Templates', () => {
  let homePage: HomePage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    homePage = new HomePage(page, logger, logDir);
    await homePage.navigateToHome();
    await homePage.waitForPageLoad();
  });

  test('template loads with proper structure', async () => {
    const isLoaded = await homePage.isTemplateLoadedProperly();
    expect(isLoaded).toBe(true);
  });

  test('template displays all three module types together', async () => {
    const yourAssetsVisible = await homePage.isYourAssetsModuleVisible();
    const domainsVisible = await homePage.isDomainsModuleVisible();
    const platformsVisible = await homePage.isPlatformsModuleVisible();

    expect(yourAssetsVisible && domainsVisible && platformsVisible).toBe(true);
  });
});
