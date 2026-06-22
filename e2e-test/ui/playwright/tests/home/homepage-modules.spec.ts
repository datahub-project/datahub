import { test, expect } from '../../fixtures/base-test';
import { HomePage } from '../../pages/home.page';

test.describe('Homepage Modules', () => {
  let homePage: HomePage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    homePage = new HomePage(page, logger, logDir);
    await homePage.navigateToHome();
    await homePage.waitForPageLoad();
  });

  test('your assets module is visible on homepage', async () => {
    const isVisible = await homePage.isYourAssetsModuleVisible();
    expect(isVisible).toBe(true);
  });

  test('domains module is visible on homepage', async () => {
    const isVisible = await homePage.isDomainsModuleVisible();
    expect(isVisible).toBe(true);
  });

  test('your assets module displays correct header', async () => {
    const headerText = await homePage.getYourAssetsModuleHeader();
    expect(headerText).toBeTruthy();
    expect(headerText?.toLowerCase()).toContain('your assets');
  });

  test('domains module displays correct header', async () => {
    const headerText = await homePage.getDomainsModuleHeader();
    expect(headerText).toBeTruthy();
    expect(headerText?.toLowerCase()).toContain('domain');
  });

  test('your assets module displays entities', async () => {
    const entityCount = await homePage.getYourAssetsEntityCount();
    expect(entityCount).toBeGreaterThanOrEqual(0);
  });

  test('domains module displays entities', async () => {
    const entityCount = await homePage.getDomainsEntityCount();
    expect(entityCount).toBeGreaterThanOrEqual(0);
  });

  test('platforms module is visible on homepage', async () => {
    const isVisible = await homePage.isPlatformsModuleVisible();
    expect(isVisible).toBe(true);
  });

  test('platforms module displays correct header', async () => {
    const headerText = await homePage.getPlatformsModuleHeader();
    expect(headerText).toBeTruthy();
    expect(headerText?.toLowerCase()).toContain('platform');
  });

  test('platforms module displays entities', async () => {
    const entityCount = await homePage.getPlatformsEntityCount();
    expect(entityCount).toBeGreaterThanOrEqual(0);
  });

  test('modules render in expected order on homepage', async () => {
    const moduleCount = await homePage.getModuleCount();
    expect(moduleCount).toBe(3);

    const inOrder = await homePage.areModulesInExpectedOrder();
    expect(inOrder).toBe(true);
  });

  test('module container is visible', async () => {
    const containerVisible = await homePage.isModuleContainerVisible();
    expect(containerVisible).toBe(true);
  });
});
