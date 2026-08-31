/**
 * Navbar Redesign tests
 *
 * Tests the new navbar redesign:
 * - Navbar sidebar container
 * - Logo and branding
 * - Navigation menus (header, main, footer)
 * - Toggle/collapse functionality
 *
 * Page Objects:
 * - NavbarPage: Handles navigation and assertions for navbar redesign
 */

import { test, expect } from '../../fixtures/base-test';
import { NavbarPage } from '../../pages/navbar.page';

test.describe('navbar redesign', () => {
  let navbarPage: NavbarPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    navbarPage = new NavbarPage(page, logger, logDir);
  });

  test('navbar sidebar is visible', async () => {
    await navbarPage.navigateToHome();
    await navbarPage.verifyNavbarSidebarVisible();
  });

  test('navbar logo is visible', async () => {
    await navbarPage.navigateToHome();
    await navbarPage.verifyNavbarLogoVisible();
  });

  test('navbar toggler is visible', async () => {
    await navbarPage.navigateToHome();
    await navbarPage.verifyNavbarTogglerVisible();
  });

  test('navbar menus are present', async () => {
    await navbarPage.navigateToHome();
    await navbarPage.verifyNavbarMenusExist();
  });

  test('navbar toggle collapses and expands', async () => {
    await navbarPage.navigateToHome();
    await navbarPage.verifyNavbarSidebarVisible();

    // Check initial state (expanded)
    let isCollapsed = await navbarPage.isNavbarCollapsed();
    expect(isCollapsed).toEqual(false);

    // Toggle to collapse
    await navbarPage.toggleNavbar();
    isCollapsed = await navbarPage.isNavbarCollapsed();
    expect(isCollapsed).toEqual(true);

    // Toggle to expand
    await navbarPage.toggleNavbar();
    isCollapsed = await navbarPage.isNavbarCollapsed();
    expect(isCollapsed).toEqual(false);
  });
});
