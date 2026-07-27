/**
 * Home Page V2 tests — Migrated from Cypress homeV2/v2_home.js
 *
 * Tests the V2 home page design with showHomePageRedesign = false.
 *
 * Coverage:
 * 1. Home page content container is present
 * 2. Navbar is visible
 *
 * Test Data:
 * - Uses default demo data seeded in DataHub
 *
 * Page Objects:
 * - HomeV2Page: Home page V2 specific assertions
 * - NavbarPage: Navbar redesign assertions
 */

import { test } from '../../fixtures/base-test';
import { HomeV2Page } from '../../pages/home-v2.page';
import { NavbarPage } from '../../pages/navbar.page';

test.describe('home page v2', () => {
  let homePage: HomeV2Page;
  let navbarPage: NavbarPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags({ showHomePageRedesign: false });
    homePage = new HomeV2Page(page, logger, logDir);
    navbarPage = new NavbarPage(page, logger, logDir);
    await homePage.skipIntroducePage();
  });

  test('home page shows content container and navbar', async () => {
    await homePage.navigateToHome();

    await homePage.verifyHomePageContentContainerExists();
    await navbarPage.verifyNavbarSidebarVisible();
    await navbarPage.verifyNavbarLogoVisible();
  });
});
